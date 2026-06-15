/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.UUID
import java.util.concurrent.CountDownLatch

import org.apache.spark.connect.proto

// =============================================================================
// INTEGRATION NOTES (what still needs to be wired up by the integrator)
//
// This file is self-contained and compiles on its own, but an Observation is
// only useful once two other pieces of the client call into it. NONE of the
// following changes live in this file -- they must be made in Dataset.scala and
// in the result-collection code (SparkResult). They are described here so the
// wiring is unambiguous.
//
// (1) Dataset.observe -- build the CollectMetrics relation.
//
//     Add to Dataset (package org.apache.spark.sql):
//
//       def observe(
//           observation: Observation,
//           expr: Column,
//           exprs: Column*): DataFrame = {
//         val df = sparkSession.newDataFrame(
//           Relation.RelType.CollectMetrics(
//             observation.markObserved(this, expr +: exprs)))
//         df
//       }
//
//     `observation.markObserved(...)` (defined below) returns the fully built
//     `proto.CollectMetrics` message with `name = observation.name`,
//     `input = this.relation`, and `metrics = (expr +: exprs).map(_.expr)`.
//     It also records this Observation so the result code (step 2) can find it
//     by name. `RelType.CollectMetrics(...)` is the
//     `org.apache.spark.connect.proto.Relation.RelType.CollectMetrics` case
//     class; `sparkSession.newDataFrame(relType)` is the existing helper.
//
//     An Observation must only be attached to a single Dataset. `markObserved`
//     throws IllegalArgumentException if reused, matching Apache Spark.
//
// (2) SparkResult -- capture ExecutePlanResponse.observedMetrics.
//
//     While iterating the response stream, each `ExecutePlanResponse` may carry
//     `observedMetrics: Seq[ExecutePlanResponse.ObservedMetrics]`, where each
//     `ObservedMetrics` has:
//        - name:   String
//        - values: Seq[proto.Expression.Literal]
//        - keys:   Seq[String]
//
//     For every observed-metrics entry seen on the stream, look up the
//     registered Observation by name and hand it the decoded values, e.g.:
//
//       response.observedMetrics.foreach { om =>
//         Observation.lookup(om.name).foreach { obs =>
//           obs.setMetricsFromLiterals(
//             om.values.map(Observation.decodeLiteral), om.keys)
//         }
//       }
//
//     `Observation.decodeLiteral` (below) turns a `proto.Expression.Literal`
//     into a Scala `Any`. If the integrator already has a literal decoder
//     (e.g. in SparkResult / LiteralValueProtoConverter), pass the decoded
//     `Seq[Any]` straight to `setMetricsFromLiterals` and skip
//     `decodeLiteral`.
//
//     `Observation.lookup` returns the Observation registered under that name
//     by `markObserved`. Registration is by name; reusing a name across
//     concurrent observations is the caller's responsibility (same as Spark).
// =============================================================================

/**
 * Holder for named aggregate metrics computed while a [[Dataset]] is being materialised, without an
 * extra pass over the data. Pair with `Dataset.observe`.
 *
 * {{{
 *   val obs = new Observation("metrics")
 *   df.observe(obs, count(lit(1)).as("rows"), max("id").as("max_id")).collect()
 *   obs.get  // Map("rows" -> 100, "max_id" -> 99)
 * }}}
 *
 * The result of `get` is a map from metric column name to its value. `get` blocks until the metrics
 * have been observed (i.e. until an action on the observed Dataset has run and the result code has
 * called `setMetricsFromLiterals`).
 *
 * @param name
 *   a unique name for this observation.
 */
class Observation(val name: String) {

  /**
   * Creates an Observation with a random UUID name, mirroring the default in the Ruby reference
   * client.
   */
  def this() = this(UUID.randomUUID().toString)

  private val latch = new CountDownLatch(1)

  @volatile private var metrics: Option[Map[String, Any]] = None

  // Guards single-attachment of this Observation to a Dataset.
  private val lock = new Object

  // True once this Observation instance has been attached to a Dataset. The single-use check is
  // per-instance (not a global name lookup) so a fresh Observation may reuse a name already seen
  // elsewhere in the JVM, e.g. when the same query is run again.
  @volatile private var attached: Boolean = false

  /**
   * The observed metric values. Blocks until the metrics are available, i.e. until an action has
   * been executed on the Dataset this Observation was attached to and the response stream has been
   * consumed.
   *
   * @return
   *   a map from metric name to its observed value.
   */
  def get: Map[String, Any] = {
    latch.await()
    metrics.getOrElse(Map.empty)
  }

  /**
   * Builds the CollectMetrics message for the given Dataset and metric columns, and registers this
   * Observation so the result code can route observed metric values back to it by name. Intended to
   * be called from `Dataset.observe`.
   *
   * An Observation can be attached to at most one Dataset; calling this twice throws.
   *
   * @param ds
   *   the Dataset being observed.
   * @param exprs
   *   the metric columns (at least one).
   * @return
   *   the fully built `proto.CollectMetrics` relation message.
   */
  private[sql] def markObserved(ds: Dataset[?], exprs: Seq[Column]): proto.CollectMetrics =
    lock.synchronized {
      if (attached) {
        throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
      }
      attached = true
      Observation.register(name, this)
      proto.CollectMetrics(input = Some(ds.relation), name = name, metrics = exprs.map(_.expr))
    }

  /**
   * Sets the observed metric values from the response stream and unblocks any threads waiting in
   * `get`. Intended to be called from the result-collection code (SparkResult) once
   * `ExecutePlanResponse.observedMetrics` has been decoded.
   *
   * Only the first set of metrics is recorded; subsequent calls are ignored, matching Apache
   * Spark's behaviour for a single action.
   *
   * @param values
   *   the decoded metric values, in column order.
   * @param keys
   *   the metric column names, parallel to `values`.
   */
  private[sql] def setMetricsFromLiterals(values: Seq[Any], keys: Seq[String]): Unit =
    lock.synchronized {
      if (metrics.isEmpty) {
        metrics = Some(keys.zip(values).toMap)
        latch.countDown()
      }
    }
}

/**
 * Companion for [[Observation]]. Maintains a registry mapping observation names to instances so the
 * result-collection code can route observed metric values back to the originating Observation, and
 * provides a helper for decoding `proto.Expression.Literal` values.
 */
object Observation {

  private val registry =
    new java.util.concurrent.ConcurrentHashMap[String, Observation]()

  /** Registers an Observation under its name. */
  private[sql] def register(name: String, observation: Observation): Unit =
    registry.put(name, observation)

  /** Looks up a registered Observation by name. */
  private[sql] def lookup(name: String): Option[Observation] =
    Option(registry.get(name))

  /**
   * Decodes a `proto.Expression.Literal` into a Scala value. Covers the literal kinds that
   * aggregate metrics commonly produce. Unknown or null literals decode to `null`.
   *
   * If the integrator already has a richer literal decoder, that should be used instead and the
   * decoded `Seq[Any]` passed directly to `setMetricsFromLiterals`.
   */
  private[sql] def decodeLiteral(literal: proto.Expression.Literal): Any = {
    import proto.Expression.Literal.LiteralType
    literal.literalType match {
      case LiteralType.Null(_) => null
      case LiteralType.Boolean(v) => v
      case LiteralType.Byte(v) => v.toByte
      case LiteralType.Short(v) => v.toShort
      case LiteralType.Integer(v) => v
      case LiteralType.Long(v) => v
      case LiteralType.Float(v) => v
      case LiteralType.Double(v) => v
      case LiteralType.String(v) => v
      case LiteralType.Decimal(v) => BigDecimal(v.value)
      case LiteralType.Date(v) => v
      case LiteralType.Timestamp(v) => v
      case LiteralType.TimestampNtz(v) => v
      case LiteralType.Empty => null
      case other => other.value
    }
  }
}
