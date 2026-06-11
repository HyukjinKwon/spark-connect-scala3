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

package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.*

/**
 * Converts between the Spark Connect protobuf [[proto.DataType]] and the client-side
 * [[org.apache.spark.sql.types.DataType]] hierarchy, in both directions.
 */
object DataTypeProtoConverter:

  // -------------------------------------------------------------------------
  // proto -> catalyst
  // -------------------------------------------------------------------------

  def toCatalystType(t: proto.DataType): DataType =
    import proto.DataType.Kind
    t.kind match
      case Kind.Null(_) => NullType
      case Kind.Boolean(_) => BooleanType
      case Kind.Byte(_) => ByteType
      case Kind.Short(_) => ShortType
      case Kind.Integer(_) => IntegerType
      case Kind.Long(_) => LongType
      case Kind.Float(_) => FloatType
      case Kind.Double(_) => DoubleType
      case Kind.String(_) => StringType
      case Kind.Binary(_) => BinaryType
      case Kind.Date(_) => DateType
      case Kind.Timestamp(_) => TimestampType
      case Kind.TimestampNtz(_) => TimestampNTZType
      case Kind.CalendarInterval(_) => CalendarIntervalType
      case Kind.Variant(_) => VariantType
      case Kind.Char(c) => CharType(c.length)
      case Kind.VarChar(c) => VarcharType(c.length)
      case Kind.Decimal(d) =>
        DecimalType(d.precision.getOrElse(10), d.scale.getOrElse(0))
      case Kind.YearMonthInterval(i) =>
        YearMonthIntervalType(i.startField.getOrElse(0).toByte, i.endField.getOrElse(1).toByte)
      case Kind.DayTimeInterval(i) =>
        DayTimeIntervalType(i.startField.getOrElse(0).toByte, i.endField.getOrElse(3).toByte)
      case Kind.Array(a) =>
        ArrayType(toCatalystType(a.getElementType), a.containsNull)
      case Kind.Map(m) =>
        MapType(toCatalystType(m.getKeyType), toCatalystType(m.getValueType), m.valueContainsNull)
      case Kind.Struct(s) =>
        StructType(s.fields.map(structFieldFromProto).toArray)
      case Kind.Udt(u) =>
        u.sqlType.map(toCatalystType).getOrElse(NullType)
      case Kind.Unparsed(_) =>
        // A type the server has not resolved yet; treat as unknown.
        NullType
      case Kind.Empty =>
        NullType

  private def structFieldFromProto(f: proto.DataType.StructField): StructField =
    StructField(
      name = f.name,
      dataType = toCatalystType(f.getDataType),
      nullable = f.nullable,
      metadata = f.metadata.map(Metadata.fromJson).getOrElse(Metadata.empty)
    )

  def toStructType(t: proto.DataType): StructType = toCatalystType(t) match
    case s: StructType => s
    case other => StructType(Array(StructField("value", other)))

  // -------------------------------------------------------------------------
  // catalyst -> proto
  // -------------------------------------------------------------------------

  def toConnectProtoType(dt: DataType): proto.DataType =
    import proto.DataType.Kind
    val kind: Kind = dt match
      case NullType => Kind.Null(proto.DataType.NULL())
      case BooleanType => Kind.Boolean(proto.DataType.Boolean())
      case ByteType => Kind.Byte(proto.DataType.Byte())
      case ShortType => Kind.Short(proto.DataType.Short())
      case IntegerType => Kind.Integer(proto.DataType.Integer())
      case LongType => Kind.Long(proto.DataType.Long())
      case FloatType => Kind.Float(proto.DataType.Float())
      case DoubleType => Kind.Double(proto.DataType.Double())
      case StringType => Kind.String(proto.DataType.String())
      case BinaryType => Kind.Binary(proto.DataType.Binary())
      case DateType => Kind.Date(proto.DataType.Date())
      case TimestampType => Kind.Timestamp(proto.DataType.Timestamp())
      case TimestampNTZType => Kind.TimestampNtz(proto.DataType.TimestampNTZ())
      case CalendarIntervalType =>
        Kind.CalendarInterval(proto.DataType.CalendarInterval())
      case VariantType => Kind.Variant(proto.DataType.Variant())
      case CharType(n) => Kind.Char(proto.DataType.Char(length = n))
      case VarcharType(n) => Kind.VarChar(proto.DataType.VarChar(length = n))
      case DecimalType(p, s) =>
        Kind.Decimal(proto.DataType.Decimal(precision = Some(p), scale = Some(s)))
      case YearMonthIntervalType(start, end) =>
        Kind.YearMonthInterval(
          proto.DataType
            .YearMonthInterval(startField = Some(start.toInt), endField = Some(end.toInt))
        )
      case DayTimeIntervalType(start, end) =>
        Kind.DayTimeInterval(
          proto.DataType.DayTimeInterval(startField = Some(start.toInt), endField = Some(end.toInt))
        )
      case ArrayType(elem, containsNull) =>
        Kind.Array(
          proto.DataType
            .Array(elementType = Some(toConnectProtoType(elem)), containsNull = containsNull)
        )
      case MapType(k, v, valueContainsNull) =>
        Kind.Map(
          proto.DataType.Map(
            keyType = Some(toConnectProtoType(k)),
            valueType = Some(toConnectProtoType(v)),
            valueContainsNull = valueContainsNull
          )
        )
      case s: StructType =>
        Kind.Struct(proto.DataType.Struct(fields = s.fields.toSeq.map(structFieldToProto)))
      case other =>
        // Fallback: send the SQL string for the server to parse.
        Kind.Unparsed(proto.DataType.Unparsed(dataTypeString = other.sql))
    proto.DataType(kind = kind)

  private def structFieldToProto(f: StructField): proto.DataType.StructField =
    proto.DataType.StructField(
      name = f.name,
      dataType = Some(toConnectProtoType(f.dataType)),
      nullable = f.nullable,
      metadata = if f.metadata.isEmpty then None else Some(f.metadata.json)
    )
