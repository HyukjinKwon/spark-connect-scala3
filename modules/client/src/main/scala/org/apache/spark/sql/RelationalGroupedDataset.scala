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

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Relation.RelType

/**
 * A set of methods for aggregations on a [[Dataset]], created by [[Dataset.groupBy]],
 * [[Dataset.rollup]], [[Dataset.cube]], or [[Dataset.pivot]].
 */
class RelationalGroupedDataset private[sql] (
    df: Dataset[?],
    groupingExprs: Seq[Column],
    groupType: proto.Aggregate.GroupType,
    pivotCol: Option[Column] = None,
    pivotValues: Option[Seq[Any]] = None
) {

  private def build(aggExprs: Seq[Column]): DataFrame = {
    val pivot =
      if (groupType == proto.Aggregate.GroupType.GROUP_TYPE_PIVOT) {
        Some(
          proto.Aggregate.Pivot(
            col = pivotCol.map(_.expr),
            values = pivotValues.getOrElse(Nil).map(v => Column.lit(v).expr.getLiteral)
          )
        )
      } else None
    val agg = proto.Aggregate(
      input = Some(df.relation),
      groupType = groupType,
      groupingExpressions = groupingExprs.map(_.expr),
      aggregateExpressions = aggExprs.map(_.expr),
      pivot = pivot
    )
    df.sparkSession.newDataFrame(RelType.Aggregate(agg))
  }

  /** Compute aggregates by specifying a series of aggregate columns. */
  def agg(expr: Column, exprs: Column*): DataFrame = build(expr +: exprs)

  /** Compute aggregates by specifying a map from column name to aggregate function name. */
  def agg(exprs: Map[String, String]): DataFrame =
    build(exprs.toSeq.map { case (col, fn) => Column.fn(fn, Column.fromName(col)) })

  /** Count the number of rows for each group. */
  def count(): DataFrame = build(Seq(Column.fn("count", Column.lit(1)).as("count")))

  def sum(colNames: String*): DataFrame = numericAgg("sum", colNames)
  def avg(colNames: String*): DataFrame = numericAgg("avg", colNames)
  def mean(colNames: String*): DataFrame = numericAgg("avg", colNames)
  def max(colNames: String*): DataFrame = numericAgg("max", colNames)
  def min(colNames: String*): DataFrame = numericAgg("min", colNames)

  /** Pivots a column of the current Dataset and performs the specified aggregation. */
  def pivot(pivotColumn: String): RelationalGroupedDataset =
    pivot(Column.fromName(pivotColumn))
  def pivot(pivotColumn: Column): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      proto.Aggregate.GroupType.GROUP_TYPE_PIVOT,
      Some(pivotColumn),
      None
    )
  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      proto.Aggregate.GroupType.GROUP_TYPE_PIVOT,
      Some(Column.fromName(pivotColumn)),
      Some(values)
    )

  private def numericAgg(fn: String, colNames: Seq[String]): DataFrame =
    build(colNames.map(c => Column.fn(fn, Column.fromName(c))))
}
