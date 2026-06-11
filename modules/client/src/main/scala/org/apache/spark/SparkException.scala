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

/**
 * The base type for errors raised by the Spark Connect server and surfaced to the client.
 *
 * Server-side failures arrive over gRPC; the client translates them into this hierarchy so callers
 * catch familiar Spark exceptions (`SparkException`, `AnalysisException`, `ParseException`)
 * carrying the server's original message and error class, rather than a raw
 * `io.grpc.StatusRuntimeException`.
 */
class SparkException(message: String, cause: Throwable, val errorClass: Option[String])
    extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null, None)
  def this(message: String, cause: Throwable) = this(message, cause, None)

  /** The Spark error class (e.g. `TABLE_OR_VIEW_NOT_FOUND`), if the server provided one. */
  def getErrorClass: String = errorClass.orNull
}
