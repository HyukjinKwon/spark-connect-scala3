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

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.Window.WindowFrame
import org.apache.spark.sql.{functions, Column}

/**
 * A window specification that defines the partitioning, ordering and frame boundaries for window
 * functions. Construct one via [[Window]].
 *
 * {{{
 *   val w = Window.partitionBy("dept").orderBy(col("salary").desc)
 *   df.select(col("name"), rank().over(w))
 * }}}
 */
class WindowSpec private[sql] (
    private[sql] val partitionSpec: Seq[proto.Expression],
    private[sql] val orderSpec: Seq[proto.Expression.SortOrder],
    private[sql] val frame: Option[WindowFrame]
) {

  /** Define the partitioning columns. */
  def partitionBy(colName: String, colNames: String*): WindowSpec =
    partitionBy((colName +: colNames).map(Column.apply): _*)

  /** Define the partitioning columns. */
  def partitionBy(cols: Column*): WindowSpec =
    new WindowSpec(cols.map(_.expr), orderSpec, frame)

  /** Define the ordering columns. */
  def orderBy(colName: String, colNames: String*): WindowSpec =
    orderBy((colName +: colNames).map(Column.apply): _*)

  /** Define the ordering columns. */
  def orderBy(cols: Column*): WindowSpec =
    new WindowSpec(partitionSpec, cols.map(_.sortOrder), frame)

  /**
   * Define the frame boundaries by row offsets from the current row. Use
   * [[Window.unboundedPreceding]], [[Window.unboundedFollowing]] and [[Window.currentRow]] for the
   * special boundaries.
   */
  def rowsBetween(start: Long, end: Long): WindowSpec =
    withFrame(WindowFrame.FrameType.FRAME_TYPE_ROW, start, end)

  /**
   * Define the frame boundaries by value offsets from the current row's value (relative to the
   * ordering expression).
   */
  def rangeBetween(start: Long, end: Long): WindowSpec =
    withFrame(WindowFrame.FrameType.FRAME_TYPE_RANGE, start, end)

  private def withFrame(frameType: WindowFrame.FrameType, start: Long, end: Long): WindowSpec = {
    val f = WindowFrame()
      .withFrameType(frameType)
      .withLower(boundary(start))
      .withUpper(boundary(end))
    new WindowSpec(partitionSpec, orderSpec, Some(f))
  }

  private def boundary(offset: Long): WindowFrame.FrameBoundary = {
    val b = WindowFrame.FrameBoundary()
    offset match {
      case Window.unboundedPreceding | Window.unboundedFollowing => b.withUnbounded(true)
      case Window.currentRow => b.withCurrentRow(true)
      case other => b.withValue(functions.lit(other).expr)
    }
  }

  private[sql] def newColumn(windowFunction: Column): Column = {
    val window = proto.Expression
      .Window()
      .withWindowFunction(windowFunction.expr)
      .withPartitionSpec(partitionSpec)
      .withOrderSpec(orderSpec)
    val withFrameSpec = frame.map(window.withFrameSpec).getOrElse(window)
    Column(proto.Expression().withWindow(withFrameSpec))
  }
}
