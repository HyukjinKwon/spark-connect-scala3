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
 * Statistical functions for a [[Dataset]], reached via `df.stat`. Mirrors PySpark's
 * `DataFrame.stat` (`DataFrameStatFunctions`).
 *
 * {{{
 *   df.stat.corr("x", "y")
 *   df.stat.approxQuantile("x", Array(0.25, 0.5, 0.75), 0.01)
 *   df.stat.crosstab("a", "b").show()
 * }}}
 */
class DataFrameStatFunctions private[sql] (df: Dataset) {

  /**
   * Calculates the approximate quantiles of a numerical column.
   *
   * @param col
   *   the column to compute quantiles for.
   * @param probabilities
   *   quantile probabilities, each in `[0.0, 1.0]` (e.g. `0.5` is the median).
   * @param relativeError
   *   the relative target precision; `0.0` yields exact quantiles (at high cost).
   * @return
   *   the approximate quantiles, one per probability.
   */
  def approxQuantile(
      col: String,
      probabilities: Array[Double],
      relativeError: Double): Array[Double] =
    approxQuantile(Array(col), probabilities, relativeError).head

  /**
   * Calculates the approximate quantiles of numerical columns.
   *
   * @param cols
   *   the columns to compute quantiles for.
   * @param probabilities
   *   quantile probabilities, each in `[0.0, 1.0]`.
   * @param relativeError
   *   the relative target precision; `0.0` yields exact quantiles (at high cost).
   * @return
   *   an array of quantile arrays, one inner array per column.
   */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double): Array[Array[Double]] = {
    val df0 = df.sparkSession.newDataFrame(
      proto.Relation.RelType.ApproxQuantile(
        proto.StatApproxQuantile(
          input = Some(df.relation),
          cols = cols.toSeq,
          probabilities = probabilities.toSeq,
          relativeError = relativeError)))
    val row = df0.sparkSession.execute(df0.plan).toArray.head
    Array.tabulate(cols.length)(i => row.getSeq[Double](i).toArray)
  }

  /**
   * Calculates the sample covariance of two numerical columns.
   *
   * @return
   *   the sample covariance of `col1` and `col2`.
   */
  def cov(col1: String, col2: String): Double = {
    val df0 = df.sparkSession.newDataFrame(
      proto.Relation.RelType.Cov(
        proto.StatCov(input = Some(df.relation), col1 = col1, col2 = col2)))
    df0.sparkSession.execute(df0.plan).toArray.head.getDouble(0)
  }

  /**
   * Calculates the Pearson correlation coefficient of two columns.
   *
   * @return
   *   the correlation of `col1` and `col2`.
   */
  def corr(col1: String, col2: String): Double = corr(col1, col2, "pearson")

  /**
   * Calculates the correlation of two columns.
   *
   * @param method
   *   the correlation method; currently only `"pearson"` is supported.
   * @return
   *   the correlation of `col1` and `col2`.
   */
  def corr(col1: String, col2: String, method: String): Double = {
    val df0 = df.sparkSession.newDataFrame(
      proto.Relation.RelType.Corr(
        proto.StatCorr(
          input = Some(df.relation),
          col1 = col1,
          col2 = col2,
          method = Some(method))))
    df0.sparkSession.execute(df0.plan).toArray.head.getDouble(0)
  }

  /**
   * Computes a pair-wise frequency table (contingency table) of the given columns.
   *
   * @return
   *   a [[Dataset]] containing the contingency table.
   */
  def crosstab(col1: String, col2: String): DataFrame =
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.Crosstab(
        proto.StatCrosstab(input = Some(df.relation), col1 = col1, col2 = col2)))

  /**
   * Finds frequent items for the given columns, with the default support `0.01`.
   *
   * @return
   *   a [[Dataset]] of frequent items per column.
   */
  def freqItems(cols: Seq[String]): DataFrame = freqItems(cols, 0.01)

  /**
   * Finds frequent items for the given columns.
   *
   * @param support
   *   the minimum frequency for an item to be considered frequent, in `(0.0, 1.0]`.
   * @return
   *   a [[Dataset]] of frequent items per column.
   */
  def freqItems(cols: Seq[String], support: Double): DataFrame =
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.FreqItems(
        proto.StatFreqItems(input = Some(df.relation), cols = cols, support = Some(support))))

  /**
   * Returns a stratified sample without replacement, keyed by the values in `col`.
   *
   * @param col
   *   the column defining the strata.
   * @param fractions
   *   a `stratum -> sampling fraction` mapping; fractions are in `[0.0, 1.0]`.
   * @param seed
   *   the random seed.
   * @return
   *   a [[Dataset]] containing the stratified sample.
   */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame = {
    val protoFractions = fractions.toSeq.map { case (stratum, fraction) =>
      proto.StatSampleBy.Fraction(
        stratum = Some(Column.lit(stratum).expr.getLiteral),
        fraction = fraction)
    }
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.SampleBy(
        proto.StatSampleBy(
          input = Some(df.relation),
          col = Some(Column.fromName(col).expr),
          fractions = protoFractions,
          seed = Some(seed))))
  }
}
