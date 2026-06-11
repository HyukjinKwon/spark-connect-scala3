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

/**
 * A set of methods for aggregations on a [[DataFrame]], created by [[Dataset.groupBy groupBy]],
 * [[Dataset.cube cube]], [[Dataset.rollup rollup]], or [[pivot]].
 *
 * @since 0.1.0
 */
class RelationalGroupedDataset private[sql] (
    private[sql] val df: DataFrame,
    private[sql] val groupingExprs: Seq[Column],
    private[sql] val groupType: proto.Aggregate.GroupType,
    private[sql] val pivot: Option[proto.Aggregate.Pivot] = None
) {

  private def aggregate(aggExprs: Seq[Column]): DataFrame = {
    val rel = proto
      .Relation(common = Some(proto.RelationCommon(planId = Some(df.sparkSession.newPlanId()))))
      .withAggregate(
        proto.Aggregate(
          input = Some(df.relation),
          groupType =
            if (pivot.isDefined) proto.Aggregate.GroupType.GROUP_TYPE_PIVOT else groupType,
          groupingExpressions = groupingExprs.map(_.expr),
          aggregateExpressions = aggExprs.map(_.expr),
          pivot = pivot
        )
      )
    Dataset.ofRows(df.sparkSession, rel)
  }

  /** Compute aggregates by specifying a series of aggregate columns. */
  def agg(expr: Column, exprs: Column*): DataFrame = aggregate(expr +: exprs)

  /** Compute aggregates from a map of column name to aggregate function name. */
  def agg(exprs: Map[String, String]): DataFrame =
    aggregate(exprs.toSeq.map { case (colName, fnName) =>
      Column.fn(fnName, functions.col(colName))
    })

  /** Compute aggregates from `(column, function)` pairs. */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    agg((aggExpr +: aggExprs).toMap)

  /** Count the number of rows for each group. */
  def count(): DataFrame =
    aggregate(Seq(Column.fn("count", functions.lit(1)).as("count")))

  private def aggOverCols(colNames: Seq[String], fnName: String): DataFrame = {
    val cols = if (colNames.isEmpty) numericOrAll else colNames
    aggregate(cols.map(c => Column.fn(fnName, functions.col(c))))
  }

  // When no columns are given, Spark aggregates over all (numeric) columns. We cannot know the
  // numeric columns without a schema round-trip, so we fall back to all named columns.
  private def numericOrAll: Seq[String] = df.columns.toSeq

  def mean(colNames: String*): DataFrame = aggOverCols(colNames, "avg")
  def avg(colNames: String*): DataFrame = aggOverCols(colNames, "avg")
  def max(colNames: String*): DataFrame = aggOverCols(colNames, "max")
  def min(colNames: String*): DataFrame = aggOverCols(colNames, "min")
  def sum(colNames: String*): DataFrame = aggOverCols(colNames, "sum")

  /** Pivots a column of the current DataFrame and performs the specified aggregation. */
  def pivot(pivotColumn: String): RelationalGroupedDataset = pivot(functions.col(pivotColumn))

  /** Pivots a column of the current DataFrame and performs the specified aggregation. */
  def pivot(pivotColumn: Column): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      groupType,
      Some(proto.Aggregate.Pivot(col = Some(pivotColumn.expr)))
    )

  /** Pivots with an explicit list of distinct values to produce columns for. */
  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset =
    pivot(functions.col(pivotColumn), values)

  /** Pivots with an explicit list of distinct values to produce columns for. */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      groupType,
      Some(
        proto.Aggregate.Pivot(
          col = Some(pivotColumn.expr),
          values = values.map(v => functions.lit(v).expr.getLiteral)
        )
      )
    )
}
