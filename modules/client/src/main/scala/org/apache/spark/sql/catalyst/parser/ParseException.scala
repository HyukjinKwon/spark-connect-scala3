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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.AnalysisException

/**
 * Thrown when SQL text fails to parse on the server. Mirrors
 * `org.apache.spark.sql.catalyst.parser.ParseException`; extends [[AnalysisException]] so existing
 * `catch { case e: AnalysisException => ... }` handlers keep working.
 */
class ParseException(message: String, cause: Throwable, errorClass: Option[String])
    extends AnalysisException(message, cause, errorClass) {
  def this(message: String) = this(message, null, None)
  def this(message: String, cause: Throwable) = this(message, cause, None)
}
