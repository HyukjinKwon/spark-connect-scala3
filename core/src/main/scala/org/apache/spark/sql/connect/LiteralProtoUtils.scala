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

package org.apache.spark.sql.connect

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period, ZoneOffset}

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types._

/**
 * Builds Spark Connect `proto.Expression.Literal` values from JVM values.
 *
 * This is the Scala 3 / ScalaPB counterpart to Spark's `LiteralValueProtoConverter`. It is
 * intentionally self-contained for scalar values (no dependency on Spark's internal date/interval
 * utilities) so that `functions.lit` works without a server round-trip.
 */
private[sql] object LiteralProtoUtils {

  private val MICROS_PER_SECOND = 1000000L
  private val MICROS_PER_MILLIS = 1000L
  private val NANOS_PER_MICROS = 1000L

  /** Build a literal expression from an arbitrary supported JVM value. */
  def toLiteralExpression(value: Any): proto.Expression =
    proto.Expression().withLiteral(toLiteralProto(value))

  def toLiteralProto(value: Any): proto.Expression.Literal = {
    val l = proto.Expression.Literal()
    value match {
      case null => l.withNull(DataTypeProtoConverter.toConnectProtoType(NullType))
      case v: Boolean => l.withBoolean(v)
      case v: Byte => l.withByte(v.toInt)
      case v: Short => l.withShort(v.toInt)
      case v: Int => l.withInteger(v)
      case v: Long => l.withLong(v)
      case v: Float => l.withFloat(v)
      case v: Double => l.withDouble(v)
      case v: String => l.withString(v)
      case v: Char => l.withString(v.toString)
      case v: Array[Char] => l.withString(String.valueOf(v))
      case v: Array[Byte] => l.withBinary(ByteString.copyFrom(v))
      case v: BigDecimal => l.withDecimal(decimal(v.bigDecimal))
      case v: JBigDecimal => l.withDecimal(decimal(v))
      case v: BigInt => l.withDecimal(decimal(new JBigDecimal(v.bigInteger)))
      case v: BigInteger => l.withDecimal(decimal(new JBigDecimal(v)))
      case v: LocalDate => l.withDate(v.toEpochDay.toInt)
      case v: Date => l.withDate(v.toLocalDate.toEpochDay.toInt)
      case v: Instant => l.withTimestamp(instantToMicros(v))
      case v: Timestamp => l.withTimestamp(instantToMicros(v.toInstant))
      case v: LocalDateTime =>
        l.withTimestampNtz(
          Math.addExact(
            Math.multiplyExact(v.toEpochSecond(ZoneOffset.UTC), MICROS_PER_SECOND),
            v.getNano / NANOS_PER_MICROS
          )
        )
      case v: Duration => l.withDayTimeInterval(durationToMicros(v))
      case v: Period => l.withYearMonthInterval(v.toTotalMonths.toInt)
      case v: Array[_] => l.withArray(array(v))
      case v: scala.collection.Seq[_] => l.withArray(array(v.toArray[Any]))
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported literal type ${other.getClass.getName} ($other)"
        )
    }
  }

  private def decimal(v: JBigDecimal): proto.Expression.Literal.Decimal =
    proto.Expression.Literal
      .Decimal()
      .withValue(v.toPlainString)
      .withPrecision(Math.max(v.precision(), v.scale()))
      .withScale(v.scale())

  private def array(values: Array[_]): proto.Expression.Literal.Array = {
    val elementType =
      if (values.isEmpty) StringType
      else componentType(values.head.getClass)
    proto.Expression.Literal
      .Array()
      .withElementType(DataTypeProtoConverter.toConnectProtoType(elementType))
      .withElements(values.toSeq.map(toLiteralProto))
  }

  private def instantToMicros(v: Instant): Long = {
    val us = Math.multiplyExact(v.getEpochSecond, MICROS_PER_SECOND)
    Math.addExact(us, v.getNano / NANOS_PER_MICROS)
  }

  private def durationToMicros(v: Duration): Long = {
    val us = Math.multiplyExact(v.getSeconds, MICROS_PER_SECOND)
    Math.addExact(us, v.getNano / NANOS_PER_MICROS)
  }

  private def componentType(clz: Class[_]): DataType = clz match {
    case c if c == classOf[java.lang.Boolean] || c == java.lang.Boolean.TYPE => BooleanType
    case c if c == classOf[java.lang.Byte] || c == java.lang.Byte.TYPE => ByteType
    case c if c == classOf[java.lang.Short] || c == java.lang.Short.TYPE => ShortType
    case c if c == classOf[java.lang.Integer] || c == java.lang.Integer.TYPE => IntegerType
    case c if c == classOf[java.lang.Long] || c == java.lang.Long.TYPE => LongType
    case c if c == classOf[java.lang.Float] || c == java.lang.Float.TYPE => FloatType
    case c if c == classOf[java.lang.Double] || c == java.lang.Double.TYPE => DoubleType
    case c if c == classOf[String] => StringType
    case c if c == classOf[JBigDecimal] || c == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case c if c == classOf[Array[Byte]] => BinaryType
    case c if c == classOf[LocalDate] || c == classOf[Date] => DateType
    case c if c == classOf[Instant] || c == classOf[Timestamp] => TimestampType
    case c if c == classOf[LocalDateTime] => TimestampNTZType
    case c if c.isArray => ArrayType(componentType(c.getComponentType))
    case other =>
      throw new IllegalArgumentException(s"Unsupported array element type $other")
  }
}
