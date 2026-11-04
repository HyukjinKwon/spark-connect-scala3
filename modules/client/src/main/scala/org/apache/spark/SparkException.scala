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

package org.apache.spark

import scala.jdk.CollectionConverters._

/**
 * The base type for errors raised by the Spark Connect server and surfaced to the client.
 *
 * Server-side failures arrive over gRPC; the client translates them into this hierarchy so callers
 * catch familiar Spark exceptions (`SparkException`, `AnalysisException`, `ParseException`)
 * carrying the server's original message, error class, message parameters, and SQLSTATE, rather
 * than a raw `io.grpc.StatusRuntimeException`.
 */
class SparkException(
    message: String,
    cause: Throwable,
    val errorClass: Option[String],
    val messageParameters: Map[String, String] = Map.empty,
    val sqlState: Option[String] = None
) extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null, None)
  def this(message: String, cause: Throwable) = this(message, cause, None)

  /** The Spark error class (e.g. `TABLE_OR_VIEW_NOT_FOUND`), if the server provided one. */
  def getErrorClass: String = errorClass.orNull

  /** The Spark error condition; an alias of [[getErrorClass]] matching `SparkThrowable`. */
  def getCondition: String = getErrorClass

  /** The parameters substituted into the error class's message template. */
  def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  /** The five-character SQLSTATE of this error, or `null` if unknown. */
  def getSqlState: String = sqlState.orNull
}

object SparkException {

  /**
   * Builds a client-side [[SparkException]] for one of the error classes the client raises itself,
   * rendering the message like the server would: `[ERROR_CLASS] text SQLSTATE: xxxxx`.
   */
  private[spark] def withErrorClass(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Throwable = null
  ): SparkException =
    new SparkException(
      ClientErrors.message(errorClass, messageParameters),
      cause,
      Some(errorClass),
      messageParameters,
      ClientErrors.sqlState(errorClass)
    )
}

/**
 * Message templates for the error classes the client raises without a server round trip. The
 * templates are copied from Apache Spark's `error-conditions.json` so client- and server-side
 * failures read the same.
 */
private[spark] object ClientErrors {

  private val templates: Map[String, (String, Option[String])] = Map(
    "UNSUPPORTED_TIME_PRECISION" -> (
      "The seconds precision <precision> of the TIME data type is out of the supported " +
        "range [0, 9].",
      Some("0A001")
    ),
    "INVALID_TIMESTAMP_PRECISION" -> (
      "The seconds precision <precision> of <type> is invalid. Expected an integer in [7, 9] " +
        "for nanosecond precision; use precision 6 or parameterless <type> for the standard " +
        "microsecond type.",
      Some("22023")
    ),
    "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE" -> (
      "Invalid nearest-by join. The number of results <numResults> must be between <min> and " +
        "<max>. Update the literal in `APPROX NEAREST <numResults> BY ...` (or " +
        "`EXACT NEAREST <numResults> BY ...`) to fall within that range.",
      Some("42604")
    ),
    "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE" -> (
      "Invalid nearest-by join. Unsupported nearest-by join type <joinType>. Supported types: " +
        "<supported>.",
      Some("42604")
    ),
    "NEAREST_BY_JOIN.UNSUPPORTED_MODE" -> (
      "Invalid nearest-by join. Unsupported nearest-by join mode '<mode>'. Supported modes " +
        "include: <supported>.",
      Some("42604")
    ),
    "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION" -> (
      "Invalid nearest-by join. Unsupported nearest-by join direction '<direction>'. Supported " +
        "nearest-by join directions include: <supported>.",
      Some("42604")
    ),
    "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME" -> (
      "Streaming query evolution error: Invalid streaming source name '<sourceName>'. Source " +
        "names must only contain ASCII letters (a-z, A-Z), digits (0-9), and underscores (_).",
      Some("42KDE")
    ),
    "_LEGACY_ERROR_TEMP_1189" -> ("User specified schema not supported with `<operation>`.", None),
    "_LEGACY_ERROR_TEMP_1197" -> ("'<operation>' does not support partitioning.", None),
    "_LEGACY_ERROR_TEMP_1312" -> ("'<operation>' does not support bucketBy right now.", None),
    "_LEGACY_ERROR_TEMP_1313" -> (
      "'<operation>' does not support bucketBy and sortBy right now.",
      None
    ),
    "SORT_BY_WITHOUT_BUCKETING" -> ("sortBy must be used together with bucketBy.", Some("42601")),
    "CLUSTERING_NOT_SUPPORTED" -> ("'<operation>' does not support clustering.", Some("42000")),
    "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED" -> (
      "Cannot specify both CLUSTER BY and PARTITIONED BY.",
      Some("42908")
    ),
    "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED" -> (
      "Cannot specify both CLUSTER BY and CLUSTERED BY INTO BUCKETS.",
      Some("42908")
    )
  )

  def sqlState(errorClass: String): Option[String] = templates.get(errorClass).flatMap(_._2)

  def message(errorClass: String, messageParameters: Map[String, String]): String = {
    val text = templates.get(errorClass).map(_._1) match {
      case Some(template) =>
        messageParameters.foldLeft(template) { case (acc, (k, v)) => acc.replace(s"<$k>", v) }
      case None =>
        messageParameters.map { case (k, v) => s"$k=$v" }.mkString(", ")
    }
    val state = sqlState(errorClass).map(s => s" SQLSTATE: $s").getOrElse("")
    s"[$errorClass] $text$state"
  }
}
