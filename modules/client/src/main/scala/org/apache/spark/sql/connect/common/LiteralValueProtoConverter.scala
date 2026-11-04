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

package org.apache.spark.sql.connect.common

import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, Period, ZoneOffset}

import scala.annotation.nowarn

import org.apache.spark.connect.{proto => p}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Converts `spark.connect.Expression.Literal` messages (e.g. observed metrics reported by the
 * server) into client-side values and types. Values use the same JVM representation as rows decoded
 * from Arrow results: `java.time` types for dates and timestamps, `java.math.BigDecimal` for
 * decimals, `Seq` / `Map` / [[Row]] for arrays, maps, and structs.
 */
object LiteralValueProtoConverter {

  private val MICROS_PER_SECOND = 1000000L

  /** The client-side value of `literal` (`null` for a NULL literal). */
  def toScalaValue(literal: p.Expression.Literal): Any = {
    import p.Expression.Literal.LiteralType
    literal.literalType match {
      case LiteralType.Null(_) | LiteralType.Empty => null
      case LiteralType.Boolean(v) => v
      case LiteralType.Byte(v) => v.toByte
      case LiteralType.Short(v) => v.toShort
      case LiteralType.Integer(v) => v
      case LiteralType.Long(v) => v
      case LiteralType.Float(v) => v
      case LiteralType.Double(v) => v
      case LiteralType.String(v) => v
      case LiteralType.Binary(v) => v.toByteArray
      case LiteralType.Decimal(v) => new java.math.BigDecimal(v.value)
      case LiteralType.Date(v) => LocalDate.ofEpochDay(v.toLong)
      case LiteralType.Timestamp(v) => microsToInstant(v)
      case LiteralType.TimestampNtz(v) =>
        LocalDateTime.ofInstant(microsToInstant(v), ZoneOffset.UTC)
      case LiteralType.TimestampNtzNanos(v) =>
        LocalDateTime.ofInstant(nanosToInstant(v.epochMicros, v.nanosWithinMicro), ZoneOffset.UTC)
      case LiteralType.TimestampLtzNanos(v) => nanosToInstant(v.epochMicros, v.nanosWithinMicro)
      case LiteralType.Time(v) => LocalTime.ofNanoOfDay(v.nano)
      case LiteralType.DayTimeInterval(v) => Duration.ofNanos(Math.multiplyExact(v, 1000L))
      case LiteralType.YearMonthInterval(v) => Period.ofMonths(v).normalized()
      case LiteralType.CalendarInterval(v) =>
        new org.apache.arrow.vector.PeriodDuration(
          Period.of(0, v.months, v.days),
          Duration.ofNanos(Math.multiplyExact(v.microseconds, 1000L))
        )
      case LiteralType.Array(v) => v.elements.map(toScalaValue)
      case LiteralType.Map(v) =>
        v.keys.map(toScalaValue).zip(v.values.map(toScalaValue)).toMap
      case LiteralType.Struct(v) => Row.fromSeq(v.elements.map(toScalaValue))
      case LiteralType.SpecializedArray(v) => specializedArray(v)
    }
  }

  /** The Spark type of `literal`, from its explicit data type when it has one. */
  @nowarn("cat=deprecation")
  def dataTypeOf(literal: p.Expression.Literal): DataType = {
    import p.Expression.Literal.LiteralType
    literal.dataType.map(DataTypeProtoConverter.toCatalystType).getOrElse {
      literal.literalType match {
        case LiteralType.Null(t) => DataTypeProtoConverter.toCatalystType(t)
        case LiteralType.Boolean(_) => BooleanType
        case LiteralType.Byte(_) => ByteType
        case LiteralType.Short(_) => ShortType
        case LiteralType.Integer(_) => IntegerType
        case LiteralType.Long(_) => LongType
        case LiteralType.Float(_) => FloatType
        case LiteralType.Double(_) => DoubleType
        case LiteralType.String(_) => StringType
        case LiteralType.Binary(_) => BinaryType
        case LiteralType.Decimal(d) =>
          DecimalType(d.precision.getOrElse(DecimalType.MAX_PRECISION), d.scale.getOrElse(0))
        case LiteralType.Date(_) => DateType
        case LiteralType.Timestamp(_) => TimestampType
        case LiteralType.TimestampNtz(_) => TimestampNTZType
        case LiteralType.TimestampNtzNanos(v) =>
          v.precision.map(TimestampNTZNanosType(_)).getOrElse(TimestampNTZNanosType())
        case LiteralType.TimestampLtzNanos(v) =>
          v.precision.map(TimestampLTZNanosType(_)).getOrElse(TimestampLTZNanosType())
        case LiteralType.Time(v) => v.precision.map(TimeType(_)).getOrElse(TimeType())
        case LiteralType.DayTimeInterval(_) => DayTimeIntervalType()
        case LiteralType.YearMonthInterval(_) => YearMonthIntervalType()
        case LiteralType.CalendarInterval(_) => CalendarIntervalType
        case LiteralType.Array(a) =>
          ArrayType(
            a.elementType
              .map(DataTypeProtoConverter.toCatalystType)
              .getOrElse(a.elements.headOption.map(dataTypeOf).getOrElse(NullType))
          )
        case LiteralType.Map(m) =>
          MapType(
            m.keyType
              .map(DataTypeProtoConverter.toCatalystType)
              .getOrElse(m.keys.headOption.map(dataTypeOf).getOrElse(NullType)),
            m.valueType
              .map(DataTypeProtoConverter.toCatalystType)
              .getOrElse(m.values.headOption.map(dataTypeOf).getOrElse(NullType))
          )
        case LiteralType.Struct(s) =>
          s.structType
            .map(DataTypeProtoConverter.toCatalystType)
            .getOrElse(
              StructType(s.elements.zipWithIndex.map { case (e, i) =>
                StructField(s"col${i + 1}", dataTypeOf(e))
              }.toArray)
            )
        case LiteralType.SpecializedArray(v) => ArrayType(specializedElementType(v))
        case _ => NullType
      }
    }
  }

  private def microsToInstant(micros: Long): Instant =
    Instant.ofEpochSecond(
      Math.floorDiv(micros, MICROS_PER_SECOND),
      Math.floorMod(micros, MICROS_PER_SECOND) * 1000L
    )

  private def nanosToInstant(epochMicros: Long, nanosWithinMicro: Int): Instant =
    microsToInstant(epochMicros).plusNanos(nanosWithinMicro.toLong)

  private def specializedArray(v: p.Expression.Literal.SpecializedArray): Seq[Any] = {
    import p.Expression.Literal.SpecializedArray.ValueTypeOneof as ValueType
    v.valueType match {
      case ValueType.Bools(b) => b.values
      case ValueType.Ints(i) => i.values
      case ValueType.Longs(l) => l.values
      case ValueType.Floats(f) => f.values
      case ValueType.Doubles(d) => d.values
      case ValueType.Strings(s) => s.values
      case ValueType.Empty => Seq.empty
    }
  }

  private def specializedElementType(v: p.Expression.Literal.SpecializedArray): DataType = {
    import p.Expression.Literal.SpecializedArray.ValueTypeOneof as ValueType
    v.valueType match {
      case ValueType.Bools(_) => BooleanType
      case ValueType.Ints(_) => IntegerType
      case ValueType.Longs(_) => LongType
      case ValueType.Floats(_) => FloatType
      case ValueType.Doubles(_) => DoubleType
      case ValueType.Strings(_) => StringType
      case ValueType.Empty => NullType
    }
  }
}
