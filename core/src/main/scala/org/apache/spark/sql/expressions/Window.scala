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

package org.apache.spark.sql.expressions

import org.apache.spark.sql.Column

/**
 * Entry point for defining [[WindowSpec]] window specifications.
 *
 * {{{
 *   import org.apache.spark.sql.expressions.Window
 *   import org.apache.spark.sql.functions._
 *
 *   val byDeptOrderBySalaryDesc =
 *     Window.partitionBy("dept").orderBy(col("salary").desc)
 *   df.select(col("*"), rank().over(byDeptOrderBySalaryDesc).as("rank"))
 * }}}
 */
object Window {

  /** Value representing the lower bound of an unbounded frame ("-Infinity"). */
  val unboundedPreceding: Long = Long.MinValue

  /** Value representing the upper bound of an unbounded frame ("+Infinity"). */
  val unboundedFollowing: Long = Long.MaxValue

  /** Value representing the current row. */
  val currentRow: Long = 0L

  private def empty: WindowSpec = new WindowSpec(Nil, Nil, None)

  /** Create a [[WindowSpec]] with the given partitioning columns. */
  def partitionBy(colName: String, colNames: String*): WindowSpec =
    empty.partitionBy(colName, colNames: _*)

  /** Create a [[WindowSpec]] with the given partitioning columns. */
  def partitionBy(cols: Column*): WindowSpec = empty.partitionBy(cols: _*)

  /** Create a [[WindowSpec]] with the given ordering columns. */
  def orderBy(colName: String, colNames: String*): WindowSpec =
    empty.orderBy(colName, colNames: _*)

  /** Create a [[WindowSpec]] with the given ordering columns. */
  def orderBy(cols: Column*): WindowSpec = empty.orderBy(cols: _*)

  /** Create a [[WindowSpec]] with the given row-based frame boundaries. */
  def rowsBetween(start: Long, end: Long): WindowSpec = empty.rowsBetween(start, end)

  /** Create a [[WindowSpec]] with the given range-based frame boundaries. */
  def rangeBetween(start: Long, end: Long): WindowSpec = empty.rangeBetween(start, end)
}
