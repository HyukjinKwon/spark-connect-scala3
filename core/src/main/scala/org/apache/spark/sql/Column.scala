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
import org.apache.spark.connect.proto.Expression.SortOrder.{NullOrdering, SortDirection}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.types.{DataType, Metadata}

/**
 * A column in a [[DataFrame]], or a named expression produced by composing other columns and
 * [[functions]].
 *
 * Columns are constructed with [[functions.col]], the `$"name"` interpolator (see
 * `SparkSession.implicits`), `df("name")`, or by combining existing columns with the operators
 * defined here.
 *
 * {{{
 *   df.select(col("colA"), col("colB") + 1)
 *   df.filter(col("age") > 21 && col("country") === "KR")
 * }}}
 *
 * Each `Column` is backed by a Spark Connect `proto.Expression`; operations build a new immutable
 * expression tree that is sent to the server for analysis and execution.
 */
class Column private[sql] (private[sql] val expr: proto.Expression) {

  override def toString: String = expr.toString

  override def hashCode: Int = expr.hashCode

  override def equals(that: Any): Boolean = that match {
    case c: Column => c.expr == expr
    case _ => false
  }

  private def fn(name: String): Column = Column.fn(name, this)
  private def fn(name: String, other: Any): Column = Column.fn(name, this, lit(other))

  private def lit(v: Any): Column = functions.lit(v)

  // ------------------------------------------------------------------------
  // Unary operators
  // ------------------------------------------------------------------------

  /** Unary minus, i.e. negate the expression. */
  def unary_- : Column = fn("negative")

  /** Boolean NOT. */
  def unary_! : Column = fn("!")

  // ------------------------------------------------------------------------
  // Comparison
  // ------------------------------------------------------------------------

  /** Equality test. */
  def ===(other: Any): Column = fn("=", other)

  /** Equality test (alias of `===`). */
  def equalTo(other: Any): Column = this === other

  /** Inequality test. */
  def =!=(other: Any): Column = !(this === other)

  /** Inequality test (alias of `=!=`). */
  def notEqual(other: Any): Column = this =!= other

  /** Greater than. */
  def >(other: Any): Column = fn(">", other)

  /** Greater than (alias of `>`). */
  def gt(other: Any): Column = this > other

  /** Less than. */
  def <(other: Any): Column = fn("<", other)

  /** Less than (alias of `<`). */
  def lt(other: Any): Column = this < other

  /** Less than or equal to. */
  def <=(other: Any): Column = fn("<=", other)

  /** Less than or equal to (alias of `<=`). */
  def leq(other: Any): Column = this <= other

  /** Greater than or equal to. */
  def >=(other: Any): Column = fn(">=", other)

  /** Greater than or equal to (alias of `>=`). */
  def geq(other: Any): Column = this >= other

  /** Null-safe equality. */
  def <=>(other: Any): Column = fn("<=>", other)

  /** Null-safe equality (alias of `<=>`). */
  def eqNullSafe(other: Any): Column = this <=> other

  /** True if the value is between `lowerBound` and `upperBound` (inclusive). */
  def between(lowerBound: Any, upperBound: Any): Column =
    (this >= lowerBound) && (this <= upperBound)

  /** True if the current expression is NaN. */
  def isNaN: Column = fn("isNaN")

  /** True if the current expression is null. */
  def isNull: Column = fn("isNull")

  /** True if the current expression is NOT null. */
  def isNotNull: Column = fn("isNotNull")

  // ------------------------------------------------------------------------
  // Boolean
  // ------------------------------------------------------------------------

  /** Boolean OR. */
  def ||(other: Any): Column = fn("or", other)

  /** Boolean OR (alias of `||`). */
  def or(other: Column): Column = this || other

  /** Boolean AND. */
  def &&(other: Any): Column = fn("and", other)

  /** Boolean AND (alias of `&&`). */
  def and(other: Column): Column = this && other

  // ------------------------------------------------------------------------
  // Arithmetic
  // ------------------------------------------------------------------------

  /** Sum. */
  def +(other: Any): Column = fn("+", other)

  /** Sum (alias of `+`). */
  def plus(other: Any): Column = this + other

  /** Subtraction. */
  def -(other: Any): Column = fn("-", other)

  /** Subtraction (alias of `-`). */
  def minus(other: Any): Column = this - other

  /** Multiplication. */
  def *(other: Any): Column = fn("*", other)

  /** Multiplication (alias of `*`). */
  def multiply(other: Any): Column = this * other

  /** Division. */
  def /(other: Any): Column = fn("/", other)

  /** Division (alias of `/`). */
  def divide(other: Any): Column = this / other

  /** Modulo (remainder). */
  def %(other: Any): Column = fn("%", other)

  /** Modulo (alias of `%`). */
  def mod(other: Any): Column = this % other

  // ------------------------------------------------------------------------
  // Membership / pattern matching
  // ------------------------------------------------------------------------

  /** True if the value is contained in the given arguments. */
  def isin(list: Any*): Column = Column.fn("in", (this +: list.map(lit)): _*)

  /** True if the value is contained in the given collection. */
  def isInCollection(values: scala.collection.Iterable[_]): Column = isin(values.toSeq: _*)

  /** SQL `LIKE` match. */
  def like(literal: String): Column = fn("like", literal)

  /** SQL `RLIKE` (regexp) match. */
  def rlike(literal: String): Column = fn("rlike", literal)

  /** Case-insensitive SQL `LIKE` match. */
  def ilike(literal: String): Column = fn("ilike", literal)

  /** True if the column contains `other`. */
  def contains(other: Any): Column = fn("contains", other)

  /** True if the column starts with `other`. */
  def startsWith(other: Column): Column = fn("startswith", other)

  /** True if the column starts with `literal`. */
  def startsWith(literal: String): Column = startsWith(lit(literal))

  /** True if the column ends with `other`. */
  def endsWith(other: Column): Column = fn("endswith", other)

  /** True if the column ends with `literal`. */
  def endsWith(literal: String): Column = endsWith(lit(literal))

  /** Substring starting at `startPos` of length `len`. */
  def substr(startPos: Column, len: Column): Column = Column.fn("substr", this, startPos, len)

  /** Substring starting at `startPos` of length `len`. */
  def substr(startPos: Int, len: Int): Column = substr(lit(startPos), lit(len))

  // ------------------------------------------------------------------------
  // Bitwise
  // ------------------------------------------------------------------------

  /** Bitwise OR. */
  def bitwiseOR(other: Any): Column = fn("|", other)

  /** Bitwise AND. */
  def bitwiseAND(other: Any): Column = fn("&", other)

  /** Bitwise XOR. */
  def bitwiseXOR(other: Any): Column = fn("^", other)

  // ------------------------------------------------------------------------
  // Complex type access
  // ------------------------------------------------------------------------

  /** Extract a value by key (map), index (array) or field name (struct). */
  def apply(extraction: Any): Column = Column(
    proto
      .Expression()
      .withUnresolvedExtractValue(
        proto.Expression
          .UnresolvedExtractValue()
          .withChild(expr)
          .withExtraction(lit(extraction).expr)
      )
  )

  /** Extract an item from an array or map (alias of `apply`). */
  def getItem(key: Any): Column = apply(key)

  /** Extract a field from a struct (alias of `apply`). */
  def getField(fieldName: String): Column = apply(fieldName)

  /** Return a new struct column with `fieldName` added or replaced by `col`. */
  def withField(fieldName: String, col: Column): Column = Column(
    proto
      .Expression()
      .withUpdateFields(
        proto.Expression
          .UpdateFields()
          .withStructExpression(expr)
          .withFieldName(fieldName)
          .withValueExpression(col.expr)
      )
  )

  /** Return a new struct column with the given fields dropped. */
  def dropFields(fieldNames: String*): Column = {
    val updated = fieldNames.foldLeft(expr) { (structExpr, name) =>
      proto
        .Expression()
        .withUpdateFields(
          proto.Expression
            .UpdateFields()
            .withStructExpression(structExpr)
            .withFieldName(name)
        )
    }
    Column(updated)
  }

  // ------------------------------------------------------------------------
  // Conditional
  // ------------------------------------------------------------------------

  /**
   * Evaluate a list of conditions and return one of multiple possible result expressions. Chains
   * after [[functions.when]].
   */
  def when(condition: Column, value: Any): Column = expr.exprType.unresolvedFunction match {
    case Some(uf) if uf.functionName == "when" && uf.arguments.size % 2 == 0 =>
      Column(
        expr.withUnresolvedFunction(uf.addArguments(condition.expr).addArguments(lit(value).expr))
      )
    case Some(uf) if uf.functionName == "when" =>
      throw new IllegalArgumentException("when() cannot be applied once otherwise() is applied")
    case _ =>
      throw new IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when()"
      )
  }

  /** Evaluate to `value` when no preceding [[when]] condition matched. */
  def otherwise(value: Any): Column = expr.exprType.unresolvedFunction match {
    case Some(uf) if uf.functionName == "when" && uf.arguments.size % 2 == 0 =>
      Column(expr.withUnresolvedFunction(uf.addArguments(lit(value).expr)))
    case Some(uf) if uf.functionName == "when" =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied once on a Column previously generated by when()"
      )
    case _ =>
      throw new IllegalArgumentException(
        "otherwise() can only be applied on a Column previously generated by when()"
      )
  }

  // ------------------------------------------------------------------------
  // Aliasing
  // ------------------------------------------------------------------------

  /** Give the column an alias. */
  def as(alias: String): Column = name(alias)

  /** Give the column an alias (Symbol form). */
  def as(alias: Symbol): Column = name(alias.name)

  /** Assign multiple aliases (for expressions that return several columns). */
  def as(aliases: Seq[String]): Column = Column(
    proto.Expression().withAlias(proto.Expression.Alias().withExpr(expr).withName(aliases))
  )

  /** Assign multiple aliases. */
  def as(aliases: Array[String]): Column = as(aliases.toSeq)

  /** Give the column an alias with associated metadata. */
  def as(alias: String, metadata: Metadata): Column = Column(
    proto
      .Expression()
      .withAlias(
        proto.Expression
          .Alias()
          .withExpr(expr)
          .withName(Seq(alias))
          .withMetadata(metadata.json)
      )
  )

  /** Give the column an alias (alias of `as`). */
  def alias(alias: String): Column = name(alias)

  /** Give the column an alias. */
  def name(alias: String): Column = Column(
    proto.Expression().withAlias(proto.Expression.Alias().withExpr(expr).withName(Seq(alias)))
  )

  // ------------------------------------------------------------------------
  // Casting
  // ------------------------------------------------------------------------

  /** Cast the column to a different data type. */
  def cast(to: DataType): Column = Column(
    proto
      .Expression()
      .withCast(
        proto.Expression
          .Cast()
          .withExpr(expr)
          .withType(DataTypeProtoConverter.toConnectProtoType(to))
      )
  )

  /** Cast the column to a different data type, expressed as a DDL string (e.g. `"int"`). */
  def cast(to: String): Column = Column(
    proto.Expression().withCast(proto.Expression.Cast().withExpr(expr).withTypeStr(to))
  )

  /** `TRY_CAST` the column, returning null instead of failing on bad input. */
  def try_cast(to: DataType): Column = Column(
    proto
      .Expression()
      .withCast(
        proto.Expression
          .Cast()
          .withExpr(expr)
          .withType(DataTypeProtoConverter.toConnectProtoType(to))
          .withEvalMode(proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
      )
  )

  /** `TRY_CAST` the column to the DDL type string. */
  def try_cast(to: String): Column = Column(
    proto
      .Expression()
      .withCast(
        proto.Expression
          .Cast()
          .withExpr(expr)
          .withTypeStr(to)
          .withEvalMode(proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
      )
  )

  // ------------------------------------------------------------------------
  // Sort ordering
  // ------------------------------------------------------------------------

  private def sortOrder(direction: SortDirection, nullOrdering: NullOrdering): Column =
    Column(
      proto
        .Expression()
        .withSortOrder(
          proto.Expression
            .SortOrder()
            .withChild(expr)
            .withDirection(direction)
            .withNullOrdering(nullOrdering)
        )
    )

  /** Sort ascending; nulls first (alias of `asc_nulls_first`). */
  def asc: Column = asc_nulls_first

  /** Sort ascending; nulls first. */
  def asc_nulls_first: Column =
    sortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_FIRST)

  /** Sort ascending; nulls last. */
  def asc_nulls_last: Column =
    sortOrder(SortDirection.SORT_DIRECTION_ASCENDING, NullOrdering.SORT_NULLS_LAST)

  /** Sort descending; nulls last (alias of `desc_nulls_last`). */
  def desc: Column = desc_nulls_last

  /** Sort descending; nulls first. */
  def desc_nulls_first: Column =
    sortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_FIRST)

  /** Sort descending; nulls last. */
  def desc_nulls_last: Column =
    sortOrder(SortDirection.SORT_DIRECTION_DESCENDING, NullOrdering.SORT_NULLS_LAST)

  /**
   * Return this column as a `SortOrder` proto. If the column is not already a sort expression it
   * defaults to ascending / nulls-first. Used by `DataFrame.sort` / `orderBy`.
   */
  private[sql] def sortOrder: proto.Expression.SortOrder = expr.exprType.sortOrder.getOrElse(
    proto.Expression
      .SortOrder()
      .withChild(expr)
      .withDirection(SortDirection.SORT_DIRECTION_ASCENDING)
      .withNullOrdering(NullOrdering.SORT_NULLS_FIRST)
  )

  // ------------------------------------------------------------------------
  // Windowing
  // ------------------------------------------------------------------------

  /** Define a windowed column based on a [[org.apache.spark.sql.expressions.WindowSpec]]. */
  def over(window: WindowSpec): Column = window.newColumn(this)

  /** Define a windowed column over an empty window frame. */
  def over(): Column = over(new WindowSpec(Nil, Nil, None))
}

/**
 * Factory and helpers for [[Column]].
 */
object Column {

  /** Wrap a raw Spark Connect expression as a `Column`. */
  private[sql] def apply(expr: proto.Expression): Column = new Column(expr)

  /** Reference a column by (possibly dotted/qualified) name. */
  def apply(colName: String): Column = colName match {
    case "*" =>
      Column(proto.Expression().withUnresolvedStar(proto.Expression.UnresolvedStar()))
    case name if name.endsWith(".*") =>
      Column(
        proto
          .Expression()
          .withUnresolvedStar(proto.Expression.UnresolvedStar().withUnparsedTarget(name))
      )
    case name =>
      Column(
        proto
          .Expression()
          .withUnresolvedAttribute(
            proto.Expression.UnresolvedAttribute().withUnparsedIdentifier(name)
          )
      )
  }

  /** Build an unresolved function call expression. */
  private[sql] def fn(name: String, inputs: Column*): Column =
    fn(name, isDistinct = false, isInternal = false, inputs)

  /** Build an unresolved function call expression, optionally on distinct inputs. */
  private[sql] def fn(name: String, isDistinct: Boolean, inputs: Column*): Column =
    fn(name, isDistinct = isDistinct, isInternal = false, inputs)

  /** Build a function call against the internal (non-user) function registry. */
  private[sql] def internalFn(name: String, inputs: Column*): Column =
    fn(name, isDistinct = false, isInternal = true, inputs)

  private def fn(
      name: String,
      isDistinct: Boolean,
      isInternal: Boolean,
      inputs: Seq[Column]
  ): Column =
    Column(
      proto
        .Expression()
        .withUnresolvedFunction(
          proto.Expression
            .UnresolvedFunction()
            .withFunctionName(name)
            .withArguments(inputs.map(_.expr))
            .withIsDistinct(isDistinct)
            .withIsUserDefinedFunction(false)
            .withIsInternal(isInternal)
        )
    )
}
