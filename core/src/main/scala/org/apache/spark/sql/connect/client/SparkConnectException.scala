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
package org.apache.spark.sql.connect.client

import io.grpc.StatusRuntimeException

/**
 * Base class for errors surfaced by the Spark Connect client. Server-side Spark errors are
 * translated into the most specific subclass that can be inferred from the message, preserving the
 * Spark error class (e.g. `[TABLE_OR_VIEW_NOT_FOUND]`) and the underlying gRPC status code.
 */
class SparkConnectException(
    message: String,
    val errorClass: Option[String] = None,
    val grpcCode: Option[String] = None,
    cause: Throwable = null
) extends RuntimeException(message, cause)

/** A SQL parse error reported by the server (`PARSE_*` / `ParseException`). */
class ParseException(
    message: String,
    errorClass: Option[String],
    grpcCode: Option[String],
    cause: Throwable
) extends SparkConnectException(message, errorClass, grpcCode, cause)

/** A query analysis error reported by the server (`AnalysisException`, `UNRESOLVED_*`, ...). */
class AnalysisException(
    message: String,
    errorClass: Option[String],
    grpcCode: Option[String],
    cause: Throwable
) extends SparkConnectException(message, errorClass, grpcCode, cause)

object SparkConnectException {

  private val errorClassPattern = """\[([A-Z0-9_.]+)\]""".r

  /** Translate a gRPC failure into the most specific [[SparkConnectException]] subtype. */
  def from(e: StatusRuntimeException): SparkConnectException = {
    val message = Option(e.getStatus.getDescription).getOrElse(e.getMessage)
    val grpcCode = Some(e.getStatus.getCode.name)
    val errorClass = errorClassPattern.findFirstMatchIn(message).map(_.group(1))
    val msg = message
    if (msg.contains("PARSE_") || msg.contains("ParseException")) {
      new ParseException(msg, errorClass, grpcCode, e)
    } else if (
      msg.contains("AnalysisException") || msg.contains("UNRESOLVED_") ||
      msg.contains("TABLE_OR_VIEW_NOT_FOUND")
    ) {
      new AnalysisException(msg, errorClass, grpcCode, e)
    } else {
      new SparkConnectException(msg, errorClass, grpcCode, e)
    }
  }
}
