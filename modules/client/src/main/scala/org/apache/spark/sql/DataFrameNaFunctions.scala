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
 * Functionality for working with missing data in a [[Dataset]], reached via `df.na`. Mirrors
 * PySpark's `DataFrame.na` (`DataFrameNaFunctions`).
 *
 * {{{
 *   df.na.drop()
 *   df.na.fill(0)
 *   df.na.fill(Map("name" -> "unknown", "age" -> 0))
 *   df.na.replace("name", Map("UNKNOWN" -> "unnamed"))
 * }}}
 */
class DataFrameNaFunctions private[sql] (df: Dataset[?]) {

  /** Returns a new [[Dataset]] that drops rows containing any null values. */
  def drop(): DataFrame = drop("any")

  /**
   * Returns a new [[Dataset]] that drops rows containing null values.
   *
   * @param how
   *   `"any"` drops a row if it contains any null, `"all"` drops a row only if every value is null.
   */
  def drop(how: String): DataFrame = drop(how, Seq.empty)

  /**
   * Returns a new [[Dataset]] that drops rows containing fewer than `minNonNulls` non-null values.
   */
  def drop(minNonNulls: Int): DataFrame = buildDrop(Seq.empty, Some(minNonNulls))

  /**
   * Returns a new [[Dataset]] that drops rows containing any null values in the given columns.
   */
  def drop(cols: Seq[String]): DataFrame = drop("any", cols)

  /**
   * Returns a new [[Dataset]] that drops rows containing null values in the given columns.
   *
   * @param how
   *   `"any"` drops a row if any of `cols` is null, `"all"` only if all of `cols` are null.
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    val minNonNulls = how.toLowerCase match {
      case "any" => if (cols.isEmpty) None else Some(cols.size)
      case "all" => Some(1)
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
    buildDrop(cols, minNonNulls)
  }

  private def buildDrop(cols: Seq[String], minNonNulls: Option[Int]): DataFrame =
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.DropNa(
        proto.NADrop(input = Some(df.relation), cols = cols, minNonNulls = minNonNulls)
      )
    )

  /** Returns a new [[Dataset]] that replaces null values in all columns with `value`. */
  def fill(value: Long): DataFrame = buildFill(Seq.empty, Seq(value))

  /** Returns a new [[Dataset]] that replaces null values in all columns with `value`. */
  def fill(value: Double): DataFrame = buildFill(Seq.empty, Seq(value))

  /** Returns a new [[Dataset]] that replaces null values in all columns with `value`. */
  def fill(value: String): DataFrame = buildFill(Seq.empty, Seq(value))

  /** Returns a new [[Dataset]] that replaces null values in all columns with `value`. */
  def fill(value: Boolean): DataFrame = buildFill(Seq.empty, Seq(value))

  /** Returns a new [[Dataset]] that replaces null values in `cols` with `value`. */
  def fill(value: Long, cols: Seq[String]): DataFrame = buildFill(cols, cols.map(_ => value))

  /** Returns a new [[Dataset]] that replaces null values in `cols` with `value`. */
  def fill(value: Double, cols: Seq[String]): DataFrame = buildFill(cols, cols.map(_ => value))

  /** Returns a new [[Dataset]] that replaces null values in `cols` with `value`. */
  def fill(value: String, cols: Seq[String]): DataFrame = buildFill(cols, cols.map(_ => value))

  /** Returns a new [[Dataset]] that replaces null values in `cols` with `value`. */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = buildFill(cols, cols.map(_ => value))

  /**
   * Returns a new [[Dataset]] that replaces null values per column, keyed by column name.
   *
   * @param valueMap
   *   a `column -> fill value` mapping; values must be `Long`, `Double`, `String` or `Boolean`.
   */
  def fill(valueMap: Map[String, Any]): DataFrame =
    buildFill(valueMap.keys.toSeq, valueMap.values.toSeq)

  private def buildFill(cols: Seq[String], values: Seq[Any]): DataFrame =
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.FillNa(
        proto.NAFill(input = Some(df.relation), cols = cols, values = values.map(naLiteral))
      )
    )

  /**
   * Returns a new [[Dataset]] that replaces values matching keys of `replacement` in `col`.
   *
   * @param col
   *   the column to apply the replacement to.
   * @param replacement
   *   an `old -> new` value mapping.
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    buildReplace(Seq(col), replacement)

  /**
   * Returns a new [[Dataset]] that replaces values matching keys of `replacement` in `cols`.
   *
   * @param cols
   *   the columns to apply the replacement to.
   * @param replacement
   *   an `old -> new` value mapping.
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame =
    buildReplace(cols, replacement)

  private def buildReplace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    val replacements = replacement.toSeq.map { case (oldValue, newValue) =>
      proto.NAReplace.Replacement(
        oldValue = Some(naLiteral(oldValue)),
        newValue = Some(naLiteral(newValue))
      )
    }
    df.sparkSession.newDataFrame(
      proto.Relation.RelType.Replace(
        proto.NAReplace(input = Some(df.relation), cols = cols, replacements = replacements)
      )
    )
  }

  /**
   * Builds a literal for fill/replace. Spark's handlers only accept `Long`, `Double`, `String` or
   * `Boolean` literals (not 32-bit `Int`), so `Int` is widened to `Long` and `Float` to `Double`.
   */
  private def naLiteral(value: Any): proto.Expression.Literal = value match {
    case i: Int => Column.lit(i.toLong).expr.getLiteral
    case f: Float => Column.lit(f.toDouble).expr.getLiteral
    case other => Column.lit(other).expr.getLiteral
  }
}
