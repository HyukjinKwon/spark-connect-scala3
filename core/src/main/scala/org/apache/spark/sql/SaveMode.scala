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

/**
 * `SaveMode` is used to specify the expected behavior of saving a DataFrame to a data source.
 * Mirrors `org.apache.spark.sql.SaveMode`.
 */
enum SaveMode {

  /** Append contents of the DataFrame to existing data. */
  case Append

  /** Overwrite existing data. */
  case Overwrite

  /** Throw an exception if data already exists (the default). */
  case ErrorIfExists

  /** Ignore the operation (i.e. no-op) if data already exists. */
  case Ignore
}

object SaveMode {

  /** Parse a save mode from one of Spark's accepted string spellings. */
  def fromString(mode: String): SaveMode = mode.toLowerCase(java.util.Locale.ROOT) match {
    case "append" => Append
    case "overwrite" => Overwrite
    case "error" | "errorifexists" | "default" => ErrorIfExists
    case "ignore" => Ignore
    case other =>
      throw new IllegalArgumentException(
        s"Unknown save mode: $other. Accepted save modes are " +
          "'overwrite', 'append', 'ignore', 'error', 'errorifexists'."
      )
  }
}
