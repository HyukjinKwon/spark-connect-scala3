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
 *
 * The operators mirror `org.apache.spark.sql.Column` and build the same Spark Connect expressions
 * as the Apache Spark Scala client.
 */
class Column private[sql] (
    private[sql] val expr: proto.Expression,
    /**
     * Relations referenced by subquery expressions (`Dataset.scalar`, `Dataset.exists`,
     * `Column.isin(Dataset)`) anywhere inside this column. A [[Dataset]] built from such a column
     * ships them alongside its plan in a `WithRelations` relation, as Apache Spark does.
     */
    private[sql] val subqueryRelations: Seq[proto.Relation]
) {

  /** Wraps an existing protobuf expression. */
  def this(expr: proto.Expression) = this(expr, Nil)

  /** A column reference by (possibly dotted) name; `"*"` expands to all columns. */
  def this(name: String) = this(Column.fromName(name).expr, Nil)

  private def fn(name: String): Column = Column.fn(name, this)
  private def fn(name: String, other: Any): Column = Column.fn(name, this, Column.lit(other))

  /** A new column over `e` that keeps this column's (and `others`') subquery references. */
  private def derive(e: proto.Expression, others: Column*): Column =
    new Column(e, Column.mergeRelations(this +: others))

  // -- Arithmetic ------------------------------------------------------------

  def unary_- : Column = fn("negative")
  def +(other: Any): Column = fn("+", other)
  def plus(other: Any): Column = this + other
  def -(other: Any): Column = fn("-", other)
  def minus(other: Any): Column = this - other
  def *(other: Any): Column = fn("*", other)
  def multiply(other: Any): Column = this * other
  def /(other: Any): Column = fn("/", other)
  def divide(other: Any): Column = this / other
  def %(other: Any): Column = fn("%", other)
  def mod(other: Any): Column = this % other

  // -- Comparison ------------------------------------------------------------

  def ===(other: Any): Column = fn("=", other)
  def equalTo(other: Any): Column = this === other
  def =!=(other: Any): Column = !(this === other)
  @deprecated("!== does not have the same precedence as ===, use =!= instead", "2.0.0")
  def !==(other: Any): Column = this =!= other
  def notEqual(other: Any): Column = this =!= other
  def >(other: Any): Column = fn(">", other)
  def gt(other: Any): Column = this > other
  def <(other: Any): Column = fn("<", other)
  def lt(other: Any): Column = this < other
  def <=(other: Any): Column = fn("<=", other)
  def leq(other: Any): Column = this <= other
  def >=(other: Any): Column = fn(">=", other)
  def geq(other: Any): Column = this >= other
  def <=>(other: Any): Column = fn("<=>", other)
  def eqNullSafe(other: Any): Column = this <=> other

  /** True if `lowerBound <= this <= upperBound`. */
  def between(lowerBound: Any, upperBound: Any): Column =
    (this >= lowerBound) && (this <= upperBound)

  // -- Boolean ---------------------------------------------------------------

  def unary_! : Column = fn("!")
  def ||(other: Any): Column = fn("or", other)
  def or(other: Column): Column = this || other
  def &&(other: Any): Column = fn("and", other)
  def and(other: Column): Column = this && other

  // -- Bitwise ---------------------------------------------------------------

  def bitwiseOR(other: Any): Column = fn("|", other)
  def bitwiseAND(other: Any): Column = fn("&", other)
  def bitwiseXOR(other: Any): Column = fn("^", other)

  // -- Null / membership predicates ------------------------------------------

  def isNaN: Column = fn("isNaN")
  def isNull: Column = fn("isNull")
  def isNotNull: Column = fn("isNotNull")

  /** True if the column's value is in the given list. */
  def isin(list: Any*): Column = Column.fn("in", (this +: list.map(Column.lit))*)

  /** True if the column's value is in the given collection. */
  def isInCollection(values: scala.collection.Iterable[?]): Column = isin(values.toSeq*)

  /** (Java-specific) True if the column's value is in the given collection. */
  def isInCollection(values: java.lang.Iterable[?]): Column = isInCollection(values.asScala)

  /**
   * True if the column's value is in the result of the single-column (or, for a `struct(...)`
   * column, multi-column) subquery `ds`. Builds an `IN (subquery)` expression.
   */
  def isin(ds: Dataset[?]): Column =
    if (ds == null) {
      // A single null is handled as a value, as in Apache Spark.
      isin(Seq(ds)*)
    } else {
      val values = expr.exprType match {
        case proto.Expression.ExprType.UnresolvedFunction(f) if f.functionName == "struct" =>
          f.arguments
        case _ => Seq(expr)
      }
      val subquery = proto.SubqueryExpression(
        planId = Column.planIdOf(ds.relation),
        subqueryType = proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN,
        inSubqueryValues = values
      )
      new Column(
        proto.Expression(exprType = proto.Expression.ExprType.SubqueryExpression(subquery)),
        Column.mergeRelations(Seq(this)) :+ ds.relation
      )
    }

  // -- String predicates -----------------------------------------------------

  def like(literal: String): Column = fn("like", literal)
  def rlike(literal: String): Column = fn("rlike", literal)
  def ilike(literal: String): Column = fn("ilike", literal)
  def contains(other: Any): Column = fn("contains", other)
  def startsWith(other: Column): Column = fn("startswith", other)
  def startsWith(literal: String): Column = startsWith(Column.lit(literal))
  def endsWith(other: Column): Column = fn("endswith", other)
  def endsWith(literal: String): Column = endsWith(Column.lit(literal))
  def substr(startPos: Column, len: Column): Column = Column.fn("substr", this, startPos, len)
  def substr(startPos: Int, len: Int): Column = substr(Column.lit(startPos), Column.lit(len))

  // -- Complex-type access ---------------------------------------------------

  /** Extract an array element by index, a map value by key, or a struct field. */
  def apply(extraction: Any): Column = {
    val key = Column.lit(extraction)
    derive(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedExtractValue(
          proto.Expression.UnresolvedExtractValue(child = Some(expr), extraction = Some(key.expr))
        )
      ),
      key
    )
  }
  def getItem(key: Any): Column = apply(key)
  def getField(fieldName: String): Column = apply(fieldName)

  /** Adds or replaces a field in a struct column by name. */
  def withField(fieldName: String, col: Column): Column = {
    require(fieldName != null, "fieldName cannot be null")
    require(col != null, "col cannot be null")
    derive(Column.updateFields(expr, fieldName, Some(col.expr)), col)
  }

  /** Drops fields of a struct column by name; a no-op for fields that do not exist. */
  def dropFields(fieldNames: String*): Column =
    derive(fieldNames.foldLeft(expr)((acc, name) => Column.updateFields(acc, name, None)))

  // -- Aliasing / naming -----------------------------------------------------

  def as(alias: String): Column = name(alias)
  def as(aliases: Seq[String]): Column = Column.alias(this, aliases, None)
  def as(aliases: Array[String]): Column = as(aliases.toSeq)
  def as(alias: Symbol): Column = name(alias.name)
  def as(alias: String, metadata: Metadata): Column = Column.alias(this, Seq(alias), Some(metadata))
  def alias(alias: String): Column = name(alias)
  def name(alias: String): Column = Column.alias(this, Seq(alias), None)

  /**
   * Gives this column an [[Encoder]], producing a [[TypedColumn]] usable with the typed
   * `Dataset.select` overloads.
   */
  def as[U](using enc: Encoder[U]): TypedColumn[Any, U] =
    new TypedColumn[Any, U](expr, subqueryRelations, enc)

  // -- Casting ---------------------------------------------------------------

  def cast(to: DataType): Column = castInternal(Left(to), None)
  def cast(to: String): Column = castInternal(Right(to), None)

  /** Casts to `to`, returning `null` instead of failing when the cast is not possible. */
  def try_cast(to: DataType): Column =
    castInternal(Left(to), Some(proto.Expression.Cast.EvalMode.EVAL_MODE_TRY))

  /** Casts to the type given as a SQL string, returning `null` when not possible. */
  def try_cast(to: String): Column =
    castInternal(Right(to), Some(proto.Expression.Cast.EvalMode.EVAL_MODE_TRY))

  private def castInternal(
      to: Either[DataType, String],
      evalMode: Option[proto.Expression.Cast.EvalMode]
  ): Column = {
    val castTo = to match {
      case Left(dt) =>
        proto.Expression.Cast.CastToType.Type(DataTypeProtoConverter.toConnectProtoType(dt))
      case Right(str) => proto.Expression.Cast.CastToType.TypeStr(str)
    }
    derive(
      proto.Expression(exprType =
        proto.Expression.ExprType.Cast(
          proto.Expression.Cast(
            expr = Some(expr),
            castToType = castTo,
            evalMode = evalMode.getOrElse(proto.Expression.Cast.EvalMode.EVAL_MODE_UNSPECIFIED)
          )
        )
      )
    )
  }

  // -- Sort ordering ---------------------------------------------------------

  def asc: Column = asc_nulls_first
  def asc_nulls_first: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_ASCENDING, nullsFirst = true)
  def asc_nulls_last: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_ASCENDING, nullsFirst = false)
  def desc: Column = desc_nulls_last
  def desc_nulls_first: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_DESCENDING, nullsFirst = true)
  def desc_nulls_last: Column =
    sortOrder(ProtoSortOrder.SortDirection.SORT_DIRECTION_DESCENDING, nullsFirst = false)

  private def sortOrder(direction: ProtoSortOrder.SortDirection, nullsFirst: Boolean): Column = {
    val nullOrdering =
      if (nullsFirst) ProtoSortOrder.NullOrdering.SORT_NULLS_FIRST
      else ProtoSortOrder.NullOrdering.SORT_NULLS_LAST
    derive(
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
    val branches = caseArguments("when")
    if (branches.size % 2 == 1) {
      throw new IllegalArgumentException("when() cannot be applied once otherwise() is applied")
    }
    val v = Column.lit(value)
    derive(Column.caseWhen(branches ++ Seq(condition.expr, v.expr)), condition, v)
  }

  /** Provides the default (ELSE) value for a CASE expression. */
  def otherwise(value: Any): Column = {
    val branches = caseArguments("otherwise")
    if (branches.size % 2 == 1) {
      throw new IllegalArgumentException(
        "otherwise() can only be applied once on a Column previously generated by when()"
      )
    }
    val v = Column.lit(value)
    derive(Column.caseWhen(branches :+ v.expr), v)
  }

  private def caseArguments(method: String): Seq[proto.Expression] = {
    val isWhen = expr.exprType.isUnresolvedFunction &&
      expr.getUnresolvedFunction.functionName == "when"
    if (!isWhen) {
      val suffix = if (method == "when") " function" else ""
      throw new IllegalArgumentException(
        s"$method() can only be applied on a Column previously generated by when()$suffix"
      )
    }
    expr.getUnresolvedFunction.arguments
  }

  // -- Windowing / misc ------------------------------------------------------

  def over(window: expressions.WindowSpec): Column = window.withAggregate(this)
  def over(): Column = over(expressions.Window.spec)

  /**
   * Marks this column as an outer reference for a correlated subquery or a lateral join's
   * correlated right side. Spark Connect resolves outer references on the server, so (as in the
   * Apache Spark Connect client) this does not change the expression sent to the server.
   */
  def outer(): Column = this

  /** Applies `f` to this column, for chaining custom transformations. */
  def transform(f: Column => Column): Column = f(this)

  /** Prints the expression to the console: the SQL-like form, or the full protobuf if extended. */
  def explain(extended: Boolean): Unit =
    // scalastyle:off println
    if (extended) println(expr.toProtoString) else println(toString)
    // scalastyle:on println

  /** A SQL-like rendering of this column, e.g. `(a + 1) AS b`. */
  override def toString: String = Column.sql(expr)

  override def equals(obj: Any): Boolean = obj match {
    case other: Column => other.expr == this.expr
    case _ => false
  }

  override def hashCode(): Int = expr.hashCode()
}

object Column {

  /** Returns a [[Column]] for the given column name. */
  def apply(colName: String): Column = new Column(colName)

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
    } else if (name.endsWith(".*")) {
      new Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedStar(
            proto.Expression.UnresolvedStar(unparsedTarget = Some(name))
          )
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

  /** Builds an unresolved function call column, optionally over distinct values. */
  private[sql] def fn(name: String, isDistinct: Boolean, args: Column*): Column =
    fnInternal(name, isDistinct, args*)

  /** Builds a call to a function from the server's internal function registry. */
  private[sql] def internalFn(name: String, args: Column*): Column =
    function(name, args, isDistinct = false, isInternal = Some(true))

  private[sql] def fnInternal(name: String, isDistinct: Boolean, args: Column*): Column =
    function(name, args, isDistinct, isInternal = None)

  private[sql] def function(
      name: String,
      args: Seq[Column],
      isDistinct: Boolean = false,
      isInternal: Option[Boolean] = None,
      isUserDefinedFunction: Boolean = false
  ): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedFunction(
          proto.Expression.UnresolvedFunction(
            functionName = name,
            arguments = args.map(_.expr),
            isDistinct = isDistinct,
            isUserDefinedFunction = isUserDefinedFunction,
            isInternal = isInternal
          )
        )
      ),
      mergeRelations(args)
    )

  /**
   * A function call whose trailing argument is a `map(k1, v1, ...)` of options, when there are any
   * (mirrors `Column.fnWithOptions` in Apache Spark).
   */
  private[sql] def fnWithOptions(
      name: String,
      options: Iterator[(String, String)],
      args: Column*
  ): Column = {
    val opts = options.toSeq
    val all =
      if (opts.isEmpty) args
      else args :+ fn("map", opts.flatMap { case (k, v) => Seq(lit(k), lit(v)) }*)
    fn(name, all*)
  }

  private[sql] def mergeRelations(cols: Seq[Column]): Seq[proto.Relation] = {
    val all = cols.flatMap(c => if (c == null) Nil else c.subqueryRelations)
    if (all.isEmpty) Nil else all.distinctBy(r => planIdOf(r))
  }

  private[sql] def planIdOf(relation: proto.Relation): Long =
    relation.common
      .flatMap(_.planId)
      .getOrElse(
        throw new IllegalStateException("A subquery Dataset must have a plan id")
      )

  private[sql] def alias(c: Column, names: Seq[String], metadata: Option[Metadata]): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.Alias(
          proto.Expression.Alias(
            expr = Some(c.expr),
            name = names,
            metadata = metadata.filterNot(_.isEmpty).map(_.json)
          )
        )
      ),
      c.subqueryRelations
    )

  private def updateFields(
      struct: proto.Expression,
      fieldName: String,
      value: Option[proto.Expression]
  ): proto.Expression =
    proto.Expression(exprType =
      proto.Expression.ExprType.UpdateFields(
        proto.Expression.UpdateFields(
          structExpression = Some(struct),
          fieldName = fieldName,
          valueExpression = value
        )
      )
    )

  private def caseWhen(arguments: Seq[proto.Expression]): proto.Expression =
    proto.Expression(exprType =
      proto.Expression.ExprType.UnresolvedFunction(
        proto.Expression.UnresolvedFunction(
          functionName = "when",
          arguments = arguments,
          isInternal = Some(false)
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
    case s: Symbol => new ColumnName(s.name)
    case _ =>
      new Column(
        proto.Expression(exprType = proto.Expression.ExprType.Literal(toLiteralProto(value)))
      )
  }

  private val MICROS_PER_SECOND = 1000000L
  private val NANOS_PER_MICRO = 1000L

  private[sql] def toLiteralProto(value: Any): proto.Expression.Literal = {
    import proto.Expression.Literal.LiteralType
    val lt = value match {
      case null =>
        // Like Apache Spark, a null literal also carries its (NULL) type at the root.
        val nullType = DataTypeProtoConverter.toConnectProtoType(NullType)
        return proto.Expression.Literal(
          literalType = LiteralType.Null(nullType),
          dataType = Some(nullType)
        )
      case b: Boolean => LiteralType.Boolean(b)
      case b: Byte => LiteralType.Byte(b.toInt)
      case s: Short => LiteralType.Short(s.toInt)
      case i: Int => LiteralType.Integer(i)
      case l: Long => LiteralType.Long(l)
      case f: Float => LiteralType.Float(f)
      case d: Double => LiteralType.Double(d)
      case s: String => LiteralType.String(s)
      case c: Char => LiteralType.String(c.toString)
      case c: Array[Char] => LiteralType.String(String.valueOf(c))
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
      case t: java.sql.Timestamp =>
        LiteralType.Timestamp(instantToMicros(SqlTimestampConversion.toInstant(t)))
      case t: java.time.LocalDateTime =>
        LiteralType.TimestampNtz(instantToMicros(t.toInstant(java.time.ZoneOffset.UTC)))
      case t: java.time.LocalTime =>
        LiteralType.Time(
          proto.Expression.Literal.Time(
            nano = t.toNanoOfDay,
            precision = Some(TimeType.DEFAULT_PRECISION)
          )
        )
      case d: java.time.Duration =>
        LiteralType.DayTimeInterval(
          Math.addExact(
            Math.multiplyExact(d.getSeconds, MICROS_PER_SECOND),
            (d.getNano / NANOS_PER_MICRO).toLong
          )
        )
      case p: java.time.Period => LiteralType.YearMonthInterval(Math.toIntExact(p.toTotalMonths))
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
    Math.addExact(Math.multiplyExact(t.getEpochSecond, MICROS_PER_SECOND), t.getNano / 1000)

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

  /**
   * Builds a literal of an explicit Spark type, so that empty collections, `null`s, and structs
   * (case classes / tuples, as [[Row]]s) keep their full type. Used by `functions.typedLit`. The
   * type is sent both in the root `data_type` (Spark 4.1+) and in the legacy per-kind fields (older
   * servers).
   */
  private[sql] def typedLiteralProto(value: Any, dataType: DataType): proto.Expression.Literal = {
    import proto.Expression.Literal.LiteralType
    val protoType = DataTypeProtoConverter.toConnectProtoType(dataType)
    (value, dataType) match {
      case (null, _) =>
        proto.Expression.Literal(
          literalType = LiteralType.Null(protoType),
          dataType = Some(protoType)
        )
      case (_, ArrayType(elementType, _)) =>
        val elements = value match {
          case s: scala.collection.Seq[?] => s.toSeq
          case a: Array[?] => a.toSeq
          case i: scala.collection.Iterable[?] => i.toSeq
          case other =>
            throw new IllegalArgumentException(s"Cannot build an array literal from $other")
        }
        proto.Expression.Literal(
          literalType = LiteralType.Array(
            proto.Expression.Literal.Array(
              elementType = Some(DataTypeProtoConverter.toConnectProtoType(elementType)),
              elements = elements.map(typedLiteralProto(_, elementType))
            )
          ),
          dataType = Some(protoType)
        )
      case (m: scala.collection.Map[?, ?], MapType(keyType, valueType, _)) =>
        val entries = m.toSeq
        proto.Expression.Literal(
          literalType = LiteralType.Map(
            proto.Expression.Literal.Map(
              keyType = Some(DataTypeProtoConverter.toConnectProtoType(keyType)),
              valueType = Some(DataTypeProtoConverter.toConnectProtoType(valueType)),
              keys = entries.map(e => typedLiteralProto(e._1, keyType)),
              values = entries.map(e => typedLiteralProto(e._2, valueType))
            )
          ),
          dataType = Some(protoType)
        )
      case (_, s: StructType) =>
        val fieldValues: Seq[Any] = value match {
          case r: Row => r.toSeq
          case p: Product => p.productIterator.toSeq
          case other =>
            throw new IllegalArgumentException(s"Cannot build a struct literal from $other")
        }
        proto.Expression.Literal(
          literalType = LiteralType.Struct(
            proto.Expression.Literal.Struct(
              structType = Some(protoType),
              elements = fieldValues.zip(s.fields).map { case (v, f) =>
                typedLiteralProto(v, f.dataType)
              }
            )
          ),
          dataType = Some(protoType)
        )
      case (v, DecimalType(precision, scale)) =>
        val bd = v match {
          case b: java.math.BigDecimal => b
          case b: BigDecimal => b.bigDecimal
          case n: java.lang.Number => new java.math.BigDecimal(n.toString)
          case other => new java.math.BigDecimal(other.toString)
        }
        proto.Expression.Literal(literalType =
          LiteralType.Decimal(
            proto.Expression.Literal.Decimal(
              value = bd.toString,
              precision = Some(precision),
              scale = Some(scale)
            )
          )
        )
      case (t: java.time.LocalTime, TimeType(precision)) =>
        proto.Expression.Literal(literalType =
          LiteralType.Time(
            proto.Expression.Literal.Time(nano = t.toNanoOfDay, precision = Some(precision))
          )
        )
      case (t: java.time.LocalDateTime, TimestampNTZNanosType(precision)) =>
        val instant = t.toInstant(java.time.ZoneOffset.UTC)
        proto.Expression.Literal(literalType =
          LiteralType.TimestampNtzNanos(
            proto.Expression.Literal.TimestampNTZNanos(
              epochMicros = microsOf(instant),
              nanosWithinMicro = (instant.getNano % NANOS_PER_MICRO).toInt,
              precision = Some(precision)
            )
          )
        )
      case (t: java.time.Instant, TimestampLTZNanosType(precision)) =>
        proto.Expression.Literal(literalType =
          LiteralType.TimestampLtzNanos(
            proto.Expression.Literal.TimestampLTZNanos(
              epochMicros = microsOf(t),
              nanosWithinMicro = (t.getNano % NANOS_PER_MICRO).toInt,
              precision = Some(precision)
            )
          )
        )
      case (v, _) => toLiteralProto(v)
    }
  }

  // Floor-based epoch microseconds, so instants before 1970 keep a non-negative sub-micro part.
  private def microsOf(t: java.time.Instant): Long =
    Math.addExact(Math.multiplyExact(t.getEpochSecond, MICROS_PER_SECOND), t.getNano / 1000L)

  private[sql] def inferType(value: Any): DataType = value match {
    case null => NullType
    case _: Boolean => BooleanType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: String | _: Char | _: Array[Char] => StringType
    case _: Array[Byte] => BinaryType
    case _: java.math.BigDecimal | _: BigDecimal => DecimalType(38, 18)
    case _: java.time.LocalDate | _: java.sql.Date => DateType
    case _: java.time.Instant | _: java.sql.Timestamp => TimestampType
    case _: java.time.LocalDateTime => TimestampNTZType
    case _: java.time.LocalTime => TimeType()
    case _: java.time.Duration => DayTimeIntervalType()
    case _: java.time.Period => YearMonthIntervalType()
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

  // -- SQL-like rendering (used by toString / explain) ------------------------

  private val binaryOperators =
    Set("+", "-", "*", "/", "%", "=", "==", "!=", "<", "<=", ">", ">=", "<=>", "&", "|", "^")

  private[sql] def sql(e: proto.Expression): String = {
    import proto.Expression.ExprType
    e.exprType match {
      case ExprType.UnresolvedAttribute(a) => a.unparsedIdentifier
      case ExprType.UnresolvedStar(s) => s.unparsedTarget.getOrElse("*")
      case ExprType.UnresolvedRegex(r) => r.colName
      case ExprType.ExpressionString(s) => s.expression
      case ExprType.Literal(l) => literalSql(l)
      case ExprType.UnresolvedFunction(f)
          if binaryOperators(f.functionName) &&
            f.arguments.size == 2 =>
        s"(${sql(f.arguments.head)} ${f.functionName} ${sql(f.arguments(1))})"
      case ExprType.UnresolvedFunction(f) if f.functionName == "and" && f.arguments.size == 2 =>
        s"(${sql(f.arguments.head)} AND ${sql(f.arguments(1))})"
      case ExprType.UnresolvedFunction(f) if f.functionName == "or" && f.arguments.size == 2 =>
        s"(${sql(f.arguments.head)} OR ${sql(f.arguments(1))})"
      case ExprType.UnresolvedFunction(f) if f.functionName == "!" && f.arguments.size == 1 =>
        s"(NOT ${sql(f.arguments.head)})"
      case ExprType.UnresolvedFunction(f) =>
        val distinct = if (f.isDistinct) "DISTINCT " else ""
        f.arguments.map(sql).mkString(s"${f.functionName}($distinct", ", ", ")")
      case ExprType.Alias(a) =>
        val names = if (a.name.size == 1) a.name.head else a.name.mkString("(", ", ", ")")
        s"${sql(a.getExpr)} AS $names"
      case ExprType.Cast(c) =>
        val prefix = if (c.evalMode.isEvalModeTry) "TRY_" else ""
        val target = c.castToType match {
          case proto.Expression.Cast.CastToType.Type(t) =>
            DataTypeProtoConverter.toCatalystType(t).sql
          case proto.Expression.Cast.CastToType.TypeStr(s) => s
          case _ => "?"
        }
        s"${prefix}CAST(${sql(c.getExpr)} AS $target)"
      case ExprType.SortOrder(o) =>
        val dir =
          if (o.direction.isSortDirectionDescending) "DESC"
          else "ASC"
        val nulls = if (o.nullOrdering.isSortNullsLast) "NULLS LAST" else "NULLS FIRST"
        s"${sql(o.getChild)} $dir $nulls"
      case ExprType.UnresolvedExtractValue(x) => s"${sql(x.getChild)}[${sql(x.getExtraction)}]"
      case ExprType.UpdateFields(u) =>
        u.valueExpression match {
          case Some(v) => s"update_field(${sql(u.getStructExpression)}, ${u.fieldName}, ${sql(v)})"
          case None => s"drop_field(${sql(u.getStructExpression)}, ${u.fieldName})"
        }
      case ExprType.Window(w) => s"${sql(w.getWindowFunction)} OVER (...)"
      case ExprType.LambdaFunction(l) =>
        val vars = l.arguments.map(_.nameParts.mkString(".")).mkString(", ")
        s"lambdafunction(${sql(l.getFunction)}, $vars)"
      case ExprType.UnresolvedNamedLambdaVariable(v) => v.nameParts.mkString(".")
      case ExprType.SubqueryExpression(s) => s"subquery(${s.subqueryType.name}, ${s.planId})"
      case other => other.getClass.getSimpleName
    }
  }

  private def literalSql(l: proto.Expression.Literal): String = {
    import proto.Expression.Literal.LiteralType
    l.literalType match {
      case LiteralType.Null(_) => "NULL"
      case LiteralType.String(s) => "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
      case LiteralType.Long(v) => s"${v}L"
      case LiteralType.Short(v) => s"${v}S"
      case LiteralType.Byte(v) => s"${v}Y"
      case LiteralType.Decimal(d) => s"${d.value}BD"
      case other => String.valueOf(other.value)
    }
  }
}

/**
 * A [[Column]] created from a column name with `$"name"`, which can also build a
 * [[org.apache.spark.sql.types.StructField]] of that name (e.g. `$"age".int`). Mirrors
 * `org.apache.spark.sql.ColumnName`.
 */
class ColumnName(colName: String) extends Column(Column.fromName(colName).expr, Nil) {

  def boolean: StructField = StructField(colName, BooleanType)
  def byte: StructField = StructField(colName, ByteType)
  def short: StructField = StructField(colName, ShortType)
  def int: StructField = StructField(colName, IntegerType)
  def long: StructField = StructField(colName, LongType)
  def float: StructField = StructField(colName, FloatType)
  def double: StructField = StructField(colName, DoubleType)
  def string: StructField = StructField(colName, StringType)
  def date: StructField = StructField(colName, DateType)
  def decimal: StructField = StructField(colName, DecimalType.USER_DEFAULT)
  def decimal(precision: Int, scale: Int): StructField =
    StructField(colName, DecimalType(precision, scale))
  def timestamp: StructField = StructField(colName, TimestampType)
  def binary: StructField = StructField(colName, BinaryType)
  def array(dataType: DataType): StructField = StructField(colName, ArrayType(dataType))
  def map(keyType: DataType, valueType: DataType): StructField =
    map(MapType(keyType, valueType))
  def map(mapType: MapType): StructField = StructField(colName, mapType)
  def struct(fields: StructField*): StructField = struct(StructType(fields))
  def struct(structType: StructType): StructField = StructField(colName, structType)
}

/**
 * A [[Column]] with an [[Encoder]] for its values, produced by `Column.as[U]`. Passing typed
 * columns to `Dataset.select` returns a typed `Dataset[U]` (or a tuple Dataset). Mirrors
 * `org.apache.spark.sql.TypedColumn`.
 */
class TypedColumn[-T, U] private[sql] (
    expr: proto.Expression,
    subqueryRelations: Seq[proto.Relation],
    private[sql] val encoder: Encoder[U]
) extends Column(expr, subqueryRelations) {

  /** Gives the typed column a name (alias), keeping its encoder. */
  override def name(alias: String): TypedColumn[T, U] = {
    val aliased = Column.alias(this, Seq(alias), None)
    new TypedColumn[T, U](aliased.expr, aliased.subqueryRelations, encoder)
  }
}
