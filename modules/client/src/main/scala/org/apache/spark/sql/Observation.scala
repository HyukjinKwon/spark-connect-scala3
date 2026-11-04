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
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.GrpcExceptionConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Helper class to simplify usage of `Dataset.observe(String, Column, Column*)`:
 *
 * {{{
 *   // Observe row count (rows) and highest id (maxid) in the Dataset while writing it
 *   val observation = Observation("my metrics")
 *   val observed_ds = ds.observe(observation, count(lit(1)).as("rows"), max($"id").as("maxid"))
 *   observed_ds.write.parquet("ds.parquet")
 *   val metrics = observation.get
 * }}}
 *
 * This collects the metrics while the first action is executed on the observed dataset. Subsequent
 * actions do not modify the metrics returned by [[get]]. Retrieval of the metric via [[get]] blocks
 * until the first action has finished and metrics become available. If collecting the metrics
 * failed on the server (Spark 4.2+ reports such failures), [[get]] throws that error.
 *
 * An Observation can be attached to only one Dataset. Mirrors `org.apache.spark.sql.Observation`.
 *
 * @param name
 *   name of the metric
 */
class Observation(val name: String) {
  require(name.nonEmpty, "Name must not be empty")

  /** Create an Observation instance without providing a name. This generates a random name. */
  def this() = this(UUID.randomUUID().toString)

  private val isRegistered = new AtomicBoolean()

  private val promise = Promise[Row]()

  /** Future holding the (yet to be completed) observation. */
  val future: Future[Row] = promise.future

  /**
   * (Scala-specific) Get the observed metrics. This waits for the observed dataset to finish its
   * first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   *
   * @return
   *   the observed metrics as a `Map[String, Any]`
   */
  @throws[InterruptedException]
  def get: Map[String, Any] = {
    val row = getRow
    if (row == null || row.schema == null) Map.empty
    else row.getValuesMap[Any](row.schema.fieldNames.toSeq)
  }

  /**
   * (Java-specific) Get the observed metrics. This waits for the observed dataset to finish its
   * first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   */
  @throws[InterruptedException]
  def getAsJava: java.util.Map[String, Any] = get.asJava

  private[sql] def markRegistered(): Unit =
    if (!isRegistered.compareAndSet(false, true)) {
      throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
    }

  private[sql] def setMetricsAndNotify(metrics: Try[Row]): Boolean = promise.tryComplete(metrics)

  private[sql] def getRow: Row = Await.result(future, Duration.Inf)
}

object Observation {

  /** Observation constructor for creating an anonymous observation. */
  def apply(): Observation = new Observation()

  /** Observation constructor for creating a named observation. */
  def apply(name: String): Observation = new Observation(name)

  /**
   * Turns the observed metrics reported by the server into a [[Row]] with a schema, or into the
   * error the server reported while collecting them.
   */
  private[sql] def transformObservedMetrics(
      metric: proto.ExecutePlanResponse.ObservedMetrics
  ): Try[Row] =
    if (metric.rootErrorIdx.isDefined) {
      Failure(GrpcExceptionConverter.errorsToThrowable(metric.getRootErrorIdx, metric.errors))
    } else {
      val values = metric.values.map(LiteralValueProtoConverter.toScalaValue)
      val fields = metric.keys.zip(metric.values).map { case (key, literal) =>
        StructField(key, LiteralValueProtoConverter.dataTypeOf(literal))
      }
      Success(new GenericRowWithSchema(values.toArray, StructType(fields.toArray)))
    }
}
