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
 * Statistic functions for a [[DataFrame]] (the `df.stat` namespace).
 *
 * @since 0.1.0
 */
class DataFrameStatFunctions private[sql] (private[sql] val df: DataFrame) {

  private def newRel(f: proto.Relation => proto.Relation): DataFrame = {
    val base = proto.Relation(common =
      Some(proto.RelationCommon(planId = Some(df.sparkSession.newPlanId())))
    )
    Dataset.ofRows(df.sparkSession, f(base))
  }

  /** Calculates the approximate quantiles of a numerical column. */
  def approxQuantile(
      col: String,
      probabilities: Array[Double],
      relativeError: Double
  ): Array[Double] =
    approxQuantile(Array(col), probabilities, relativeError).head

  /** Calculates the approximate quantiles of numerical columns. */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double
  ): Array[Array[Double]] = {
    val row = newRel(
      _.withApproxQuantile(
        proto.StatApproxQuantile(
          input = Some(df.relation),
          cols = cols.toSeq,
          probabilities = probabilities.toSeq,
          relativeError = relativeError
        )
      )
    ).collect().head
    cols.indices.map(i => row.getSeq[Double](i).toArray).toArray
  }

  /** Calculates the sample covariance of two numerical columns. */
  def cov(col1: String, col2: String): Double =
    newRel(_.withCov(proto.StatCov(input = Some(df.relation), col1 = col1, col2 = col2)))
      .collect()
      .head
      .getDouble(0)

  /** Calculates the Pearson correlation coefficient of two columns. */
  def corr(col1: String, col2: String, method: String): Double =
    newRel(
      _.withCorr(
        proto.StatCorr(input = Some(df.relation), col1 = col1, col2 = col2, method = Some(method))
      )
    ).collect().head.getDouble(0)

  /** Calculates the Pearson correlation coefficient of two columns. */
  def corr(col1: String, col2: String): Double = corr(col1, col2, "pearson")

  /** Computes a pair-wise frequency table (contingency table) of the given columns. */
  def crosstab(col1: String, col2: String): DataFrame =
    newRel(_.withCrosstab(proto.StatCrosstab(input = Some(df.relation), col1 = col1, col2 = col2)))

  /** Finds frequent items for the given columns, with the given minimum support. */
  def freqItems(cols: Seq[String], support: Double): DataFrame =
    newRel(
      _.withFreqItems(
        proto.StatFreqItems(input = Some(df.relation), cols = cols, support = Some(support))
      )
    )

  /** Finds frequent items for the given columns (default support 1%). */
  def freqItems(cols: Seq[String]): DataFrame = freqItems(cols, 0.01)

  /** Returns a stratified sample without replacement based on the fraction per stratum. */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame =
    sampleBy(functions.col(col), fractions, seed)

  /** Returns a stratified sample without replacement based on the fraction per stratum. */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame = {
    val protoFractions = fractions.toSeq.map { case (stratum, fraction) =>
      proto.StatSampleBy.Fraction(
        stratum = Some(functions.lit(stratum).expr.getLiteral),
        fraction = fraction
      )
    }
    newRel(
      _.withSampleBy(
        proto.StatSampleBy(
          input = Some(df.relation),
          col = Some(col.expr),
          fractions = protoFractions,
          seed = Some(seed)
        )
      )
    )
  }
}
