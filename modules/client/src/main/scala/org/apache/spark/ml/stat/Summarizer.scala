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
package org.apache.spark.ml.stat

import org.apache.spark.sql.{Column, MlFunctionBridge}
import org.apache.spark.sql.{functions => F}

/**
 * Builds the server-side aggregate that computes several vector summary statistics in one pass.
 * Mirrors `org.apache.spark.ml.stat.SummaryBuilder`.
 */
final class SummaryBuilder private[stat] (metrics: Seq[String]) {

  /** A struct column with one field per metric, weighting every row equally. */
  def summary(featuresCol: Column): Column = summary(featuresCol, F.lit(1.0))

  /** A struct column with one field per metric, weighted by `weightCol`. */
  def summary(featuresCol: Column, weightCol: Column): Column =
    MlFunctionBridge.internal(
      "aggregate_metrics",
      F.array(metrics.map(F.lit)*),
      featuresCol,
      weightCol
    )
}

/**
 * Column-wise statistics of vector columns (mean, variance, min, and so on). Mirrors
 * `org.apache.spark.ml.stat.Summarizer`; the aggregation runs on the server.
 */
object Summarizer {

  /**
   * A builder for the given metric names: `mean`, `sum`, `variance`, `std`, `count`, `numNonZeros`,
   * `max`, `min`, `normL1`, `normL2`.
   */
  def metrics(metrics: String*): SummaryBuilder = new SummaryBuilder(metrics)

  /** The per-element mean of a vector column, weighted by `weightCol`. */
  def mean(col: Column, weightCol: Column): Column = single("mean", col, weightCol)

  /** The per-element mean of a vector column. */
  def mean(col: Column): Column = mean(col, F.lit(1.0))

  /** The per-element sum of a vector column, weighted by `weightCol`. */
  def sum(col: Column, weightCol: Column): Column = single("sum", col, weightCol)

  /** The per-element sum of a vector column. */
  def sum(col: Column): Column = sum(col, F.lit(1.0))

  /** The per-element unbiased variance of a vector column, weighted by `weightCol`. */
  def variance(col: Column, weightCol: Column): Column = single("variance", col, weightCol)

  /** The per-element unbiased variance of a vector column. */
  def variance(col: Column): Column = variance(col, F.lit(1.0))

  /** The per-element standard deviation of a vector column, weighted by `weightCol`. */
  def std(col: Column, weightCol: Column): Column = single("std", col, weightCol)

  /** The per-element standard deviation of a vector column. */
  def std(col: Column): Column = std(col, F.lit(1.0))

  /** The number of weighted rows. */
  def count(col: Column, weightCol: Column): Column = single("count", col, weightCol)

  /** The number of rows. */
  def count(col: Column): Column = count(col, F.lit(1.0))

  /** The per-element count of non-zero values, weighted by `weightCol`. */
  def numNonZeros(col: Column, weightCol: Column): Column = single("numNonZeros", col, weightCol)

  /** The per-element count of non-zero values. */
  def numNonZeros(col: Column): Column = numNonZeros(col, F.lit(1.0))

  /** The per-element maximum, weighted by `weightCol`. */
  def max(col: Column, weightCol: Column): Column = single("max", col, weightCol)

  /** The per-element maximum. */
  def max(col: Column): Column = max(col, F.lit(1.0))

  /** The per-element minimum, weighted by `weightCol`. */
  def min(col: Column, weightCol: Column): Column = single("min", col, weightCol)

  /** The per-element minimum. */
  def min(col: Column): Column = min(col, F.lit(1.0))

  /** The per-element L1 norm, weighted by `weightCol`. */
  def normL1(col: Column, weightCol: Column): Column = single("normL1", col, weightCol)

  /** The per-element L1 norm. */
  def normL1(col: Column): Column = normL1(col, F.lit(1.0))

  /** The per-element L2 norm, weighted by `weightCol`. */
  def normL2(col: Column, weightCol: Column): Column = single("normL2", col, weightCol)

  /** The per-element L2 norm. */
  def normL2(col: Column): Column = normL2(col, F.lit(1.0))

  private def single(metric: String, col: Column, weightCol: Column): Column =
    metrics(metric).summary(col, weightCol).getField(metric).as(metric)
}
