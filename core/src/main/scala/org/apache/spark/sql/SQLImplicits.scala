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

import scala.language.implicitConversions

/**
 * A collection of implicit methods for converting common Scala objects into [[DataFrame]]s, and for
 * the `$"columnName"` column syntax. Accessed via `import spark.implicits.*`.
 *
 * {{{
 *   import spark.implicits.*
 *   val df = Seq((1, "a"), (2, "b")).toDF("id", "name")
 *   df.select($"id", $"name")
 * }}}
 */
class SQLImplicits private[sql] (session: SparkSession) {

  /** Converts `$"col name"` into a [[Column]]. */
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): Column = functions.col(sc.s(args: _*))
  }

  /** Adds a `.toDF` method to any sequence of values, tuples, or case classes. */
  implicit def localSeqToDataFrameHolder[A](seq: Seq[A]): DataFrameHolder =
    new DataFrameHolder(session, seq)
}

/**
 * Holder around a local sequence that knows how to turn it into a [[DataFrame]]. Tuples and case
 * classes (anything `Product`) become multi-column rows; any other value becomes a single column.
 * The schema is inferred from the runtime types of the first row.
 */
final class DataFrameHolder private[sql] (session: SparkSession, data: Seq[?]) {

  /**
   * Convert to a DataFrame. With no column names the columns are named `_1`, `_2`, ...; otherwise
   * the given names are used.
   */
  def toDF(colNames: String*): DataFrame =
    session.createDataFrameFromLocalSeq(data, colNames.toSeq)
}
