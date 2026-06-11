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
 * Functionality for working with missing data in a [[DataFrame]] (the `df.na` namespace).
 *
 * @since 0.1.0
 */
class DataFrameNaFunctions private[sql] (private[sql] val df: DataFrame) {

  private def newRel(f: proto.Relation => proto.Relation): DataFrame = {
    val base = proto.Relation(common =
      Some(proto.RelationCommon(planId = Some(df.sparkSession.newPlanId())))
    )
    Dataset.ofRows(df.sparkSession, f(base))
  }

  private def lit(v: Any): proto.Expression.Literal = functions.lit(v).expr.getLiteral

  // ---- drop -----------------------------------------------------------------

  /** Drops rows containing any null or NaN values. */
  def drop(): DataFrame = drop("any", Seq.empty)

  /** Drops rows containing null or NaN values (`how` = "any" or "all"). */
  def drop(how: String): DataFrame = drop(how, Seq.empty)

  /** Drops rows in the given columns containing any null or NaN values. */
  def drop(cols: Seq[String]): DataFrame = drop("any", cols)

  /** Drops rows in the given columns according to `how`. */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    val minNonNulls = how.trim.toLowerCase match {
      case "any" => None
      case "all" => Some(1)
      case other => throw new IllegalArgumentException(s"how must be 'any' or 'all', got '$other'")
    }
    dropImpl(minNonNulls, cols)
  }

  /** Drops rows that contain fewer than `minNonNulls` non-null/non-NaN values. */
  def drop(minNonNulls: Int): DataFrame = dropImpl(Some(minNonNulls), Seq.empty)

  /** Drops rows in the given columns with fewer than `minNonNulls` non-null/non-NaN values. */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = dropImpl(Some(minNonNulls), cols)

  private def dropImpl(minNonNulls: Option[Int], cols: Seq[String]): DataFrame =
    newRel(
      _.withDropNa(proto.NADrop(input = Some(df.relation), cols = cols, minNonNulls = minNonNulls))
    )

  // ---- fill -----------------------------------------------------------------

  /** Fills null values in numeric columns with `value`. */
  def fill(value: Long): DataFrame = fillValue(value, Seq.empty)

  /** Fills null values in the given numeric columns with `value`. */
  def fill(value: Long, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /** Fills null values in numeric columns with `value`. */
  def fill(value: Double): DataFrame = fillValue(value, Seq.empty)

  /** Fills null values in the given numeric columns with `value`. */
  def fill(value: Double, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /** Fills null values in string columns with `value`. */
  def fill(value: String): DataFrame = fillValue(value, Seq.empty)

  /** Fills null values in the given string columns with `value`. */
  def fill(value: String, cols: Seq[String]): DataFrame = fillValue(value, cols)

  /** Fills null values in boolean columns with `value`. */
  def fill(value: Boolean): DataFrame = fillValue(value, Seq.empty)

  /** Fills null values in the given boolean columns with `value`. */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = fillValue(value, cols)

  private def fillValue(value: Any, cols: Seq[String]): DataFrame =
    newRel(
      _.withFillNa(proto.NAFill(input = Some(df.relation), cols = cols, values = Seq(lit(value))))
    )

  /** Fills null values per a `column name -> fill value` map. */
  def fill(valueMap: Map[String, Any]): DataFrame = {
    val (cols, values) = valueMap.toSeq.unzip
    newRel(
      _.withFillNa(proto.NAFill(input = Some(df.relation), cols = cols, values = values.map(lit)))
    )
  }

  // ---- replace --------------------------------------------------------------

  /** Replaces values matching keys in `replacement` in the specified column. */
  def replace[V](col: String, replacement: Map[V, V]): DataFrame =
    replace(if (col == "*") Seq.empty else Seq(col), replacement)

  /** Replaces values matching keys in `replacement` in the specified columns. */
  def replace[V](cols: Seq[String], replacement: Map[V, V]): DataFrame = {
    val replacements = replacement.toSeq.map { case (oldV, newV) =>
      proto.NAReplace.Replacement(oldValue = Some(lit(oldV)), newValue = Some(lit(newV)))
    }
    newRel(
      _.withReplace(
        proto.NAReplace(input = Some(df.relation), cols = cols, replacements = replacements)
      )
    )
  }
}
