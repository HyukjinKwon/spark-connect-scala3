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

import org.apache.spark.SparkException

/**
 * Thrown when a query fails to analyze on the server: an unknown table or column, a type mismatch,
 * an ambiguous reference, and similar. Mirrors `org.apache.spark.sql.AnalysisException`.
 */
class AnalysisException(
    message: String,
    cause: Throwable,
    errorClass: Option[String],
    messageParameters: Map[String, String] = Map.empty,
    sqlState: Option[String] = None
) extends SparkException(message, cause, errorClass, messageParameters, sqlState) {
  def this(message: String) = this(message, null, None)
  def this(message: String, cause: Throwable) = this(message, cause, None)
}

object AnalysisException {

  /**
   * Builds a client-side [[AnalysisException]] for an error class the client validates itself (for
   * example invalid `nearestByJoin` arguments), with the same message the server renders.
   */
  private[spark] def withErrorClass(
      errorClass: String,
      messageParameters: Map[String, String]
  ): AnalysisException =
    new AnalysisException(
      org.apache.spark.ClientErrors.message(errorClass, messageParameters),
      null,
      Some(errorClass),
      messageParameters,
      org.apache.spark.ClientErrors.sqlState(errorClass)
    )
}
