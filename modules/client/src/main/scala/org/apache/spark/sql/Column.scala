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

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.{SortOrder => ProtoSortOrder}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types._

/**
 * A column in a [[Dataset]]: a lazily-evaluated reference to a column or a computation over
 * columns. Columns are immutable; operators and methods return new [[Column]]s.
 *
 * Build columns with [[functions.col]], [[functions.lit]], by indexing a DataFrame (`df("id")`), or
 * by combining other columns with operators.
 *
 * {{{
 *   import org.apache.spark.sql.functions._
 *   (col("age") + 1).as("next_age")
 *   col("name").like("a%") && (col("age") >= 18)
 * }}}
 */
class Column(private[sql] val expr: proto.Expression) {

  private def fn(name: String, args: Column*): Column = Column.fn(name, args*)
  private def binOp(name: String, other: Any): Column =
    Column.fn(name, this, Column.lit(other))

  // -- Arithmetic ------------------------------------------------------------
  def +(other: Any): Column = binOp("+", other)
  def -(other: Any): Column = binOp("-", other)
  def *(other: Any): Column = binOp("*", other)
  def /(other: Any): Column = binOp("/", other)
  def %(other: Any): Column = binOp("%", other)
  def unary_- : Column = fn("negative", this)

  // -- Comparison ------------------------------------------------------------
  def ===(other: Any): Column = binOp("==", other)
  def =!=(other: Any): Column = binOp("!=", other)
  def notEqual(other: Any): Column = binOp("!=", other)
  def <(other: Any): Column = binOp("<", other)
  def <=(other: Any): Column = binOp("<=", other)
  def >(other: Any): Column = binOp(">", other)
  def >=(other: Any): Column = binOp(">=", other)
  def <=>(other: Any): Column = binOp("<=>", other)
  def eqNullSafe(other: Any): Column = binOp("<=>", other)

  // -- Boolean ---------------------------------------------------------------
  def &&(other: Column): Column = Column.fn("and", this, other)
  def ||(other: Column): Column = Column.fn("or", this, other)
  def and(other: Column): Column = Column.fn("and", this, other)
  def or(other: Column): Column = Column.fn("or", this, other)
  def unary_! : Column = fn("not", this)

  // -- Bitwise ---------------------------------------------------------------
  def bitwiseAND(other: Any): Column = binOp("&", other)
  def bitwiseOR(other: Any): Column = binOp("|", other)
  def bitwiseXOR(other: Any): Column = binOp("^", other)

  // -- Null / membership predicates ------------------------------------------
  def isNull: Column = fn("isNull", this)
  def isNotNull: Column = fn("isNotNull", this)
  def isNaN: Column = fn("isNaN", this)

  /** True if the column's value is in the given list. */
  def isin(values: Any*): Column = Column.fn("in", (this +: values.map(Column.lit))*)

  /** True if `lowerBound <= this <= upperBound`. */
  def between(lowerBound: Any, upperBound: Any): Column =
    (this >= lowerBound) && (this <= upperBound)

  // -- String predicates -----------------------------------------------------
  def like(literal: String): Column = binOp("like", literal)
  def rlike(literal: String): Column = binOp("rlike", literal)
  def ilike(literal: String): Column = binOp("ilike", literal)
  def contains(other: Any): Column = binOp("contains", other)
  def startsWith(other: Any): Column = binOp("startswith", other)
  def endsWith(other: Any): Column = binOp("endswith", other)
  def substr(startPos: Int, len: Int): Column =
    Column.fn("substr", this, Column.lit(startPos), Column.lit(len))

  // -- Complex-type access ---------------------------------------------------
  /** Extract an array element by index, a map value by key, or a struct field. */
  def apply(key: Any): Column = getItem(key)
  def getItem(key: Any): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedExtractValue(
          proto.Expression
            .UnresolvedExtractValue(child = Some(expr), extraction = Some(Column.lit(key).expr))
        )
      )
    )
  def getField(fieldName: String): Column = getItem(fieldName)

  // -- Aliasing / naming -----------------------------------------------------
  def as(alias: String): Column = named(alias)
  def as(aliases: Seq[String]): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.Alias(proto.Expression.Alias(expr = Some(expr), name = aliases))
      )
    )
  def alias(alias: String): Column = named(alias)
  def name(alias: String): Column = named(alias)
  private def named(alias: String): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.Alias(
          proto.Expression.Alias(expr = Some(expr), name = Seq(alias))
        )
      )
    )

  // -- Casting ---------------------------------------------------------------
  def cast(to: DataType): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.Cast(
          proto.Expression.Cast(
            expr = Some(expr),
            castToType =
              proto.Expression.Cast.CastToType.Type(DataTypeProtoConverter.toConnectProtoType(to))
          )
        )
      )
    )
  def cast(to: String): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.Cast(
          proto.Expression
            .Cast(expr = Some(expr), castToType = proto.Expression.Cast.CastToType.TypeStr(to))
        )
      )
    )

  // -- Sort ordering ---------------------------------------------------------
  def asc: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_ASCENDING, nullsFirst = true)
  def asc_nulls_first: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_ASCENDING, nullsFirst = true)
  def asc_nulls_last: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_ASCENDING, nullsFirst = false)
  def desc: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_DESCENDING, nullsFirst = false)
  def desc_nulls_first: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_DESCENDING, nullsFirst = true)
  def desc_nulls_last: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_DESCENDING, nullsFirst = false)

  private def sortOrder(direction: ProtoSortOrder.SortDirection, nullsFirst: Boolean): Column = {
    val nullOrdering =
      if (nullsFirst) ProtoSortOrder.NullOrdering.SORT_NULLS_FIRST
      else ProtoSortOrder.NullOrdering.SORT_NULLS_LAST
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.SortOrder(
          ProtoSortOrder(child = Some(expr), direction = direction, nullOrdering = nullOrdering)
        )
      )
    )
  }

  // -- CASE WHEN -------------------------------------------------------------
  /** Adds a branch to a CASE expression started by [[functions.when]]. */
  def when(condition: Column, value: Any): Column = {
    requireWhen("when")
    val args = expr.getUnresolvedFunction.arguments ++
      Seq(condition.expr, Column.lit(value).expr)
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedFunction(
          proto.Expression.UnresolvedFunction(functionName = "when", arguments = args)
        )
      )
    )
  }

  /** Provides the default (ELSE) value for a CASE expression. */
  def otherwise(value: Any): Column = {
    requireWhen("otherwise")
    val args = expr.getUnresolvedFunction.arguments ++ Seq(Column.lit(value).expr)
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedFunction(
          proto.Expression.UnresolvedFunction(functionName = "when", arguments = args)
        )
      )
    )
  }

  private def requireWhen(method: String): Unit = {
    val isWhen = expr.exprType.isUnresolvedFunction &&
      expr.getUnresolvedFunction.functionName == "when"
    require(isWhen, s"$method() can only be applied on a Column previously generated by when()")
  }

  // -- Windowing -------------------------------------------------------------
  def over(window: expressions.WindowSpec): Column = window.withAggregate(this)
  def over(): Column = over(expressions.Window.spec)

  override def toString: String = s"Column<${expr.exprType.getClass.getSimpleName}>"

  override def equals(obj: Any): Boolean = obj match {
    case other: Column => other.expr == this.expr
    case _ => false
  }

  override def hashCode(): Int = expr.hashCode()
}

object Column {

  /** Wraps an existing protobuf expression. */
  private[sql] def apply(expr: proto.Expression): Column = new Column(expr)

  /** An unresolved attribute reference by (possibly dotted) name; `"*"` expands to all columns. */
  private[sql] def fromName(name: String): Column =
    if (name == "*") {
      new Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedStar(proto.Expression.UnresolvedStar())
        )
      )
    } else {
      new Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedAttribute(
            proto.Expression.UnresolvedAttribute(unparsedIdentifier = name)
          )
        )
      )
    }

  /** Builds an unresolved function call column. */
  private[sql] def fn(name: String, args: Column*): Column =
    fnInternal(name, isDistinct = false, args*)

  private[sql] def fnInternal(name: String, isDistinct: Boolean, args: Column*): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedFunction(
          proto.Expression.UnresolvedFunction(
            functionName = name,
            arguments = args.map(_.expr),
            isDistinct = isDistinct
          )
        )
      )
    )

  /**
   * Coerces a value into a Column; existing Columns pass through, everything else becomes a
   * literal.
   */
  private[sql] def toCol(value: Any): Column = value match {
    case c: Column => c
    case other => lit(other)
  }

  /** Builds a literal column from a Scala value. */
  private[sql] def lit(value: Any): Column = value match {
    case c: Column => c
    case _ =>
      new Column(
        proto.Expression(exprType = proto.Expression.ExprType.Literal(toLiteralProto(value)))
      )
  }

  private val MICROS_PER_SECOND = 1000000L

  private[sql] def toLiteralProto(value: Any): proto.Expression.Literal = {
    import proto.Expression.Literal.LiteralType
    val lt = value match {
      case null => LiteralType.Null(DataTypeProtoConverter.toConnectProtoType(NullType))
      case b: Boolean => LiteralType.Boolean(b)
      case b: Byte => LiteralType.Byte(b.toInt)
      case s: Short => LiteralType.Short(s.toInt)
      case i: Int => LiteralType.Integer(i)
      case l: Long => LiteralType.Long(l)
      case f: Float => LiteralType.Float(f)
      case d: Double => LiteralType.Double(d)
      case s: String => LiteralType.String(s)
      case b: Array[Byte] => LiteralType.Binary(ByteString.copyFrom(b))
      case d: java.math.BigDecimal =>
        LiteralType.Decimal(
          proto.Expression.Literal.Decimal(
            value = d.toPlainString,
            precision = Some(d.precision()),
            scale = Some(d.scale())
          )
        )
      case d: BigDecimal => return toLiteralProto(d.bigDecimal)
      case d: java.time.LocalDate => LiteralType.Date(d.toEpochDay.toInt)
      case d: java.sql.Date => LiteralType.Date(d.toLocalDate.toEpochDay.toInt)
      case t: java.time.Instant => LiteralType.Timestamp(instantToMicros(t))
      case t: java.sql.Timestamp => LiteralType.Timestamp(instantToMicros(t.toInstant))
      case t: java.time.LocalDateTime =>
        LiteralType.TimestampNtz(instantToMicros(t.toInstant(java.time.ZoneOffset.UTC)))
      case s: Seq[?] => arrayLiteral(s)
      case a: Array[?] => arrayLiteral(a.toSeq)
      case m: Map[?, ?] => mapLiteral(m)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported literal value of type ${other.getClass.getName}: $other"
        )
    }
    proto.Expression.Literal(literalType = lt)
  }

  private def instantToMicros(t: java.time.Instant): Long =
    Math.multiplyExact(t.getEpochSecond, MICROS_PER_SECOND) + t.getNano / 1000

  private def arrayLiteral(values: Seq[Any]): proto.Expression.Literal.LiteralType = {
    val elementType = inferType(values.find(_ != null).orNull)
    proto.Expression.Literal.LiteralType.Array(
      proto.Expression.Literal.Array(
        elementType = Some(DataTypeProtoConverter.toConnectProtoType(elementType)),
        elements = values.map(toLiteralProto)
      )
    )
  }

  private def mapLiteral(m: Map[?, ?]): proto.Expression.Literal.LiteralType = {
    val keyType = m.keys.headOption.map(inferType).getOrElse(StringType)
    val valueType = m.values.headOption.map(inferType).getOrElse(StringType)
    proto.Expression.Literal.LiteralType.Map(
      proto.Expression.Literal.Map(
        keyType = Some(DataTypeProtoConverter.toConnectProtoType(keyType)),
        valueType = Some(DataTypeProtoConverter.toConnectProtoType(valueType)),
        keys = m.keys.toSeq.map(toLiteralProto),
        values = m.values.toSeq.map(toLiteralProto)
      )
    )
  }

  private[sql] def inferType(value: Any): DataType = value match {
    case null => NullType
    case _: Boolean => BooleanType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: String => StringType
    case _: Array[Byte] => BinaryType
    case _: java.math.BigDecimal | _: BigDecimal => DecimalType(38, 18)
    case _: java.time.LocalDate | _: java.sql.Date => DateType
    case _: java.time.Instant | _: java.sql.Timestamp => TimestampType
    case _: java.time.LocalDateTime => TimestampNTZType
    case s: Seq[?] => ArrayType(s.headOption.map(inferType).getOrElse(NullType))
    case a: Array[?] => ArrayType(a.headOption.map(inferType).getOrElse(NullType))
    case m: Map[?, ?] =>
      MapType(
        m.keys.headOption.map(inferType).getOrElse(StringType),
        m.values.headOption.map(inferType).getOrElse(StringType)
      )
    case other =>
      throw new IllegalArgumentException(s"Cannot infer Spark type for ${other.getClass.getName}")
  }
}

/** A type-class-free alias used in a few APIs; kept for source compatibility with Spark. */
private[sql] object ColumnName
