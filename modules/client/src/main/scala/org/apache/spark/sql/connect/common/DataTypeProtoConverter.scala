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

import org.apache.spark.connect.{proto => p}
import org.apache.spark.sql.types._

/**
 * Converts between the client-side [[org.apache.spark.sql.types.DataType]] hierarchy and the
 * `spark.connect.DataType` protobuf message.
 */
object DataTypeProtoConverter {

  /** Converts a protobuf `DataType` into the client-side [[DataType]]. */
  def toCatalystType(t: p.DataType): DataType = t.kind match {
    case _: p.DataType.Kind.Null => NullType
    case _: p.DataType.Kind.Boolean => BooleanType
    case _: p.DataType.Kind.Byte => ByteType
    case _: p.DataType.Kind.Short => ShortType
    case _: p.DataType.Kind.Integer => IntegerType
    case _: p.DataType.Kind.Long => LongType
    case _: p.DataType.Kind.Float => FloatType
    case _: p.DataType.Kind.Double => DoubleType
    case _: p.DataType.Kind.String => StringType
    case _: p.DataType.Kind.Binary => BinaryType
    case _: p.DataType.Kind.Date => DateType
    case _: p.DataType.Kind.Timestamp => TimestampType
    case _: p.DataType.Kind.TimestampNtz => TimestampNTZType
    case _: p.DataType.Kind.CalendarInterval => CalendarIntervalType
    case _: p.DataType.Kind.Variant => VariantType
    case k: p.DataType.Kind.Char => CharType(k.value.length)
    case k: p.DataType.Kind.VarChar => VarcharType(k.value.length)
    case k: p.DataType.Kind.Decimal =>
      DecimalType(k.value.precision.getOrElse(10), k.value.scale.getOrElse(0))
    case k: p.DataType.Kind.YearMonthInterval =>
      YearMonthIntervalType(
        k.value.startField.getOrElse(0).toByte,
        k.value.endField.getOrElse(1).toByte)
    case k: p.DataType.Kind.DayTimeInterval =>
      DayTimeIntervalType(
        k.value.startField.getOrElse(0).toByte,
        k.value.endField.getOrElse(3).toByte)
    case k: p.DataType.Kind.Array =>
      ArrayType(
        toCatalystType(k.value.getElementType),
        k.value.containsNull)
    case k: p.DataType.Kind.Map =>
      MapType(
        toCatalystType(k.value.getKeyType),
        toCatalystType(k.value.getValueType),
        k.value.valueContainsNull)
    case k: p.DataType.Kind.Struct =>
      StructType(k.value.fields.map(toStructField).toArray)
    case k: p.DataType.Kind.Unparsed =>
      throw new IllegalArgumentException(
        s"Cannot convert unparsed data type: ${k.value.dataTypeString}")
    case other =>
      throw new IllegalArgumentException(s"Unsupported data type: $other")
  }

  private def toStructField(f: p.DataType.StructField): StructField =
    StructField(
      name = f.name,
      dataType = toCatalystType(f.getDataType),
      nullable = f.nullable,
      metadata = Metadata.empty)

  /** Converts a client-side [[DataType]] into the protobuf representation. */
  def toConnectProtoType(t: DataType): p.DataType = {
    val kind = t match {
      case NullType => p.DataType.Kind.Null(p.DataType.NULL())
      case BooleanType => p.DataType.Kind.Boolean(p.DataType.Boolean())
      case ByteType => p.DataType.Kind.Byte(p.DataType.Byte())
      case ShortType => p.DataType.Kind.Short(p.DataType.Short())
      case IntegerType => p.DataType.Kind.Integer(p.DataType.Integer())
      case LongType => p.DataType.Kind.Long(p.DataType.Long())
      case FloatType => p.DataType.Kind.Float(p.DataType.Float())
      case DoubleType => p.DataType.Kind.Double(p.DataType.Double())
      case StringType => p.DataType.Kind.String(p.DataType.String())
      case BinaryType => p.DataType.Kind.Binary(p.DataType.Binary())
      case DateType => p.DataType.Kind.Date(p.DataType.Date())
      case TimestampType => p.DataType.Kind.Timestamp(p.DataType.Timestamp())
      case TimestampNTZType => p.DataType.Kind.TimestampNtz(p.DataType.TimestampNTZ())
      case CalendarIntervalType =>
        p.DataType.Kind.CalendarInterval(p.DataType.CalendarInterval())
      case VariantType => p.DataType.Kind.Variant(p.DataType.Variant())
      case CharType(length) => p.DataType.Kind.Char(p.DataType.Char(length = length))
      case VarcharType(length) => p.DataType.Kind.VarChar(p.DataType.VarChar(length = length))
      case DecimalType(precision, scale) =>
        p.DataType.Kind.Decimal(
          p.DataType.Decimal(precision = Some(precision), scale = Some(scale)))
      case YearMonthIntervalType(start, end) =>
        p.DataType.Kind.YearMonthInterval(
          p.DataType.YearMonthInterval(
            startField = Some(start.toInt),
            endField = Some(end.toInt)))
      case DayTimeIntervalType(start, end) =>
        p.DataType.Kind.DayTimeInterval(
          p.DataType.DayTimeInterval(
            startField = Some(start.toInt),
            endField = Some(end.toInt)))
      case ArrayType(elementType, containsNull) =>
        p.DataType.Kind.Array(
          p.DataType.Array(
            elementType = Some(toConnectProtoType(elementType)),
            containsNull = containsNull))
      case MapType(keyType, valueType, valueContainsNull) =>
        p.DataType.Kind.Map(
          p.DataType.Map(
            keyType = Some(toConnectProtoType(keyType)),
            valueType = Some(toConnectProtoType(valueType)),
            valueContainsNull = valueContainsNull))
      case s: StructType =>
        p.DataType.Kind.Struct(
          p.DataType.Struct(fields = s.fields.toSeq.map(toProtoStructField)))
      case other =>
        throw new IllegalArgumentException(s"Unsupported data type: ${other.simpleString}")
    }
    p.DataType(kind = kind)
  }

  private def toProtoStructField(f: StructField): p.DataType.StructField =
    p.DataType.StructField(
      name = f.name,
      dataType = Some(toConnectProtoType(f.dataType)),
      nullable = f.nullable,
      metadata = if (f.metadata.isEmpty) None else Some(f.metadata.json))
}
