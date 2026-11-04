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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Relation.RelType

/**
 * A set of methods for aggregations on a [[Dataset]], created by [[Dataset.groupBy]],
 * [[Dataset.rollup]], [[Dataset.cube]], [[Dataset.groupingSets]], or [[pivot]].
 */
class RelationalGroupedDataset private[sql] (
    df: Dataset[?],
    groupingExprs: Seq[Column],
    groupType: proto.Aggregate.GroupType,
    pivotCol: Option[Column] = None,
    pivotValues: Option[Seq[proto.Expression.Literal]] = None,
    groupingSets: Seq[Seq[Column]] = Nil
) {

  private def build(aggExprs: Seq[Column]): DataFrame = {
    val pivot =
      if (groupType == proto.Aggregate.GroupType.GROUP_TYPE_PIVOT) {
        Some(proto.Aggregate.Pivot(col = pivotCol.map(_.expr), values = pivotValues.getOrElse(Nil)))
      } else None
    val agg = proto.Aggregate(
      input = Some(df.relation),
      groupType = groupType,
      groupingExpressions = groupingExprs.map(_.expr),
      aggregateExpressions = aggExprs.map(_.expr),
      pivot = pivot,
      groupingSets =
        groupingSets.map(set => proto.Aggregate.GroupingSets(groupingSet = set.map(_.expr)))
    )
    df.sparkSession.newDataFrame(
      RelType.Aggregate(agg),
      groupingExprs ++ aggExprs ++ pivotCol.toSeq ++ groupingSets.flatten
    )
  }

  /** Converts a (column name, aggregate method name) pair into an aggregate column. */
  private def toAggCol(colAndMethod: (String, String)): Column = {
    val col = df.col(colAndMethod._1)
    colAndMethod._2.toLowerCase(java.util.Locale.ROOT) match {
      case "avg" | "average" | "mean" => functions.avg(col)
      case "stddev" | "std" => functions.stddev(col)
      case "count" | "size" => functions.count(col)
      case name => Column.fn(name, col)
    }
  }

  /** Compute aggregates by specifying a series of aggregate columns. */
  def agg(expr: Column, exprs: Column*): DataFrame = build(expr +: exprs)

  /**
   * Compute aggregates by specifying (column name, aggregate method) pairs, e.g. `agg("age" ->
   * "max", "salary" -> "avg")`. The method may be `avg`/`mean`, `max`, `min`, `sum`, `count`, or
   * any other aggregate function name.
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    build((aggExpr +: aggExprs).map(toAggCol))

  /** Compute aggregates by specifying a map from column name to aggregate function name. */
  def agg(exprs: Map[String, String]): DataFrame = build(exprs.toSeq.map(toAggCol))

  /** (Java-specific) Compute aggregates from a map of column name to aggregate function name. */
  def agg(exprs: java.util.Map[String, String]): DataFrame = agg(exprs.asScala.toMap)

  /** Count the number of rows for each group. */
  def count(): DataFrame = build(Seq(functions.count(functions.lit(1)).as("count")))

  def sum(colNames: String*): DataFrame = numericAgg(functions.sum, colNames)
  def avg(colNames: String*): DataFrame = numericAgg(functions.avg, colNames)
  def mean(colNames: String*): DataFrame = numericAgg(functions.avg, colNames)
  def max(colNames: String*): DataFrame = numericAgg(functions.max, colNames)
  def min(colNames: String*): DataFrame = numericAgg(functions.min, colNames)

  /** Pivots a column of the current Dataset and performs the specified aggregation. */
  def pivot(pivotColumn: String): RelationalGroupedDataset = pivot(df.col(pivotColumn))

  /** Pivots a column, computing the distinct values on the server. */
  def pivot(pivotColumn: Column): RelationalGroupedDataset = pivot(pivotColumn, Nil)

  /** Pivots a column over the given values. */
  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset =
    pivot(df.col(pivotColumn), values)

  /** (Java-specific) Pivots a column over the given values. */
  def pivot(pivotColumn: String, values: java.util.List[Any]): RelationalGroupedDataset =
    pivot(df.col(pivotColumn), values)

  /** (Java-specific) Pivots a column over the given values. */
  def pivot(pivotColumn: Column, values: java.util.List[Any]): RelationalGroupedDataset =
    pivot(pivotColumn, values.asScala.toSeq)

  /** Pivots a column over the given values (literals or literal columns). */
  def pivot(pivotColumn: Column, values: Seq[Any]): RelationalGroupedDataset =
    groupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
        val literals = values.map {
          case c: Column =>
            if (!c.expr.exprType.isLiteral) {
              throw new IllegalArgumentException("values only accept literal Column")
            }
            c.expr.getLiteral
          case v => Column.lit(v).expr.getLiteral
        }
        new RelationalGroupedDataset(
          df,
          groupingExprs,
          proto.Aggregate.GroupType.GROUP_TYPE_PIVOT,
          Some(pivotColumn),
          Some(literals)
        )
      case other =>
        throw new UnsupportedOperationException(s"pivot is not supported after $other")
    }

  private def numericAgg(fn: Column => Column, colNames: Seq[String]): DataFrame =
    build(colNames.map(name => fn(df.col(name))))
}
