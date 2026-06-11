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
import org.apache.spark.connect.proto.Expression.Window.{WindowFrame => ProtoFrame}
import org.apache.spark.sql.Column

/**
 * A window specification that defines the partitioning, ordering, and frame boundaries of an
 * analytic / windowed computation. Build with [[Window]].
 *
 * {{{
 *   import org.apache.spark.sql.expressions.Window
 *   import org.apache.spark.sql.functions._
 *   val w = Window.partitionBy("dept").orderBy(col("salary").desc)
 *   df.select(col("*"), rank().over(w).as("r"))
 * }}}
 */
class WindowSpec private[sql] (
    private[sql] val partitionSpec: Seq[proto.Expression],
    private[sql] val orderSpec: Seq[proto.Expression.SortOrder],
    private[sql] val frame: Option[ProtoFrame]) {

  def partitionBy(colName: String, colNames: String*): WindowSpec =
    partitionBy((colName +: colNames).map(Column.fromName)*)

  def partitionBy(cols: Column*): WindowSpec =
    new WindowSpec(cols.map(_.expr), orderSpec, frame)

  def orderBy(colName: String, colNames: String*): WindowSpec =
    orderBy((colName +: colNames).map(Column.fromName)*)

  def orderBy(cols: Column*): WindowSpec =
    new WindowSpec(partitionSpec, cols.map(WindowSpec.toSortOrder), frame)

  def rowsBetween(start: Long, end: Long): WindowSpec =
    withFrame(ProtoFrame.FrameType.FRAME_TYPE_ROW, start, end)

  def rangeBetween(start: Long, end: Long): WindowSpec =
    withFrame(ProtoFrame.FrameType.FRAME_TYPE_RANGE, start, end)

  private def withFrame(frameType: ProtoFrame.FrameType, start: Long, end: Long): WindowSpec =
    new WindowSpec(
      partitionSpec,
      orderSpec,
      Some(
        ProtoFrame(
          frameType = frameType,
          lower = Some(WindowSpec.boundary(start)),
          upper = Some(WindowSpec.boundary(end)))))

  /** Wraps `column` as a window function over this specification. */
  private[sql] def withAggregate(column: Column): Column = {
    val window = proto.Expression.Window(
      windowFunction = Some(column.expr),
      partitionSpec = partitionSpec,
      orderSpec = orderSpec,
      frameSpec = frame)
    new Column(proto.Expression(exprType = proto.Expression.ExprType.Window(window)))
  }
}

object WindowSpec {
  private[sql] def empty: WindowSpec = new WindowSpec(Seq.empty, Seq.empty, None)

  private def toSortOrder(col: Column): proto.Expression.SortOrder =
    col.expr.exprType match {
      case proto.Expression.ExprType.SortOrder(order) => order
      case _ =>
        proto.Expression.SortOrder(
          child = Some(col.expr),
          direction = proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
    }

  private def boundary(value: Long): ProtoFrame.FrameBoundary = {
    val b =
      if (value == Window.unboundedPreceding || value == Long.MinValue)
        ProtoFrame.FrameBoundary.Boundary.Unbounded(true)
      else if (value == Window.unboundedFollowing || value == Long.MaxValue)
        ProtoFrame.FrameBoundary.Boundary.Unbounded(true)
      else if (value == Window.currentRow)
        ProtoFrame.FrameBoundary.Boundary.CurrentRow(true)
      else
        ProtoFrame.FrameBoundary.Boundary.Value(Column.lit(value).expr)
    ProtoFrame.FrameBoundary(boundary = b)
  }
}

/** Entry point for building [[WindowSpec]]s. Mirrors `org.apache.spark.sql.expressions.Window`. */
object Window {
  val unboundedPreceding: Long = Long.MinValue
  val unboundedFollowing: Long = Long.MaxValue
  val currentRow: Long = 0L

  private[sql] def spec: WindowSpec = WindowSpec.empty

  def partitionBy(colName: String, colNames: String*): WindowSpec =
    spec.partitionBy(colName, colNames*)
  def partitionBy(cols: Column*): WindowSpec = spec.partitionBy(cols*)
  def orderBy(colName: String, colNames: String*): WindowSpec = spec.orderBy(colName, colNames*)
  def orderBy(cols: Column*): WindowSpec = spec.orderBy(cols*)
  def rowsBetween(start: Long, end: Long): WindowSpec = spec.rowsBetween(start, end)
  def rangeBetween(start: Long, end: Long): WindowSpec = spec.rangeBetween(start, end)
}
