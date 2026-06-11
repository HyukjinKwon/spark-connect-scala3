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

package org.apache.spark.sql.connect.client.arrow

import scala.jdk.CollectionConverters.*

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.types.*

/**
 * Conversions between Apache Arrow schema metadata and the client-side
 * [[org.apache.spark.sql.types.DataType]] hierarchy. Used as a fallback when the server does not
 * attach an explicit Spark schema to the response (the Arrow IPC stream always carries one).
 */
private[sql] object ArrowUtils:

  /** Convert an Arrow [[Schema]] to a Spark [[StructType]]. */
  def fromArrowSchema(schema: Schema): StructType =
    StructType(schema.getFields.asScala.map(fromArrowField).toArray)

  /** Convert a single Arrow [[Field]] to a Spark [[StructField]]. */
  def fromArrowField(field: Field): StructField =
    StructField(field.getName, fromArrowFieldType(field), field.isNullable)

  private def fromArrowFieldType(field: Field): DataType =
    field.getType match
      case _: ArrowType.Null => NullType
      case _: ArrowType.Bool => BooleanType
      case int: ArrowType.Int =>
        int.getBitWidth match
          case 8 => ByteType
          case 16 => ShortType
          case 32 => IntegerType
          case 64 => LongType
          case w => throw new UnsupportedOperationException(s"Unsupported int width: $w")
      case fp: ArrowType.FloatingPoint =>
        fp.getPrecision match
          case FloatingPointPrecision.SINGLE => FloatType
          case FloatingPointPrecision.DOUBLE => DoubleType
          case FloatingPointPrecision.HALF => FloatType
      case _: ArrowType.Utf8 | _: ArrowType.LargeUtf8 => StringType
      case _: ArrowType.Binary | _: ArrowType.LargeBinary | _: ArrowType.FixedSizeBinary =>
        BinaryType
      case dec: ArrowType.Decimal => DecimalType(dec.getPrecision, dec.getScale)
      case _: ArrowType.Date => DateType
      case ts: ArrowType.Timestamp =>
        if ts.getTimezone == null || ts.getTimezone.isEmpty then TimestampNTZType
        else TimestampType
      case _: ArrowType.Interval | _: ArrowType.Duration => CalendarIntervalType
      case _: ArrowType.List | _: ArrowType.LargeList | _: ArrowType.FixedSizeList =>
        val child = field.getChildren.get(0)
        ArrayType(fromArrowFieldType(child), child.isNullable)
      case _: ArrowType.Map =>
        // Map field has a single child: a non-nullable Struct{key, value}.
        val entries = field.getChildren.get(0)
        val keyField = entries.getChildren.get(0)
        val valField = entries.getChildren.get(1)
        MapType(fromArrowFieldType(keyField), fromArrowFieldType(valField), valField.isNullable)
      case _: ArrowType.Struct =>
        StructType(field.getChildren.asScala.map(fromArrowField).toArray)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported Arrow type: $other")

  /** Returns true if the unit is microseconds (the unit Spark Connect uses for timestamps). */
  private[arrow] def isMicros(unit: TimeUnit): Boolean = unit == TimeUnit.MICROSECOND
  private[arrow] def isDay(unit: DateUnit): Boolean = unit == DateUnit.DAY

  // --- catalyst -> arrow (for building LocalRelation payloads) ---------------

  /** The default session-local timezone used when materialising timestamp vectors. */
  val defaultTimeZoneId: String = "UTC"

  /** Convert a Spark [[StructType]] to an Arrow [[Schema]]. */
  def toArrowSchema(schema: StructType, timeZoneId: String = defaultTimeZoneId): Schema =
    new Schema(
      schema.fields.toSeq.map(f => toArrowField(f.name, f.dataType, f.nullable, timeZoneId)).asJava
    )

  /** Convert a Spark field to an Arrow [[Field]] (recursively for nested types). */
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field =
    dt match
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(
          name,
          fieldType,
          java.util.Collections.singletonList(
            toArrowField("element", elementType, containsNull, timeZoneId)
          )
        )
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(
          name,
          fieldType,
          fields.toSeq.map(f => toArrowField(f.name, f.dataType, f.nullable, timeZoneId)).asJava
        )
      case MapType(keyType, valueType, valueContainsNull) =>
        val fieldType = new FieldType(nullable, new ArrowType.Map(false), null)
        val entries = new Field(
          MapVectorName,
          new FieldType(false, ArrowType.Struct.INSTANCE, null),
          java.util.Arrays.asList(
            toArrowField("key", keyType, nullable = false, timeZoneId),
            toArrowField("value", valueType, valueContainsNull, timeZoneId)
          )
        )
        new Field(name, fieldType, java.util.Collections.singletonList(entries))
      case _ =>
        new Field(name, new FieldType(nullable, toArrowType(dt, timeZoneId), null), null)

  private final val MapVectorName = "entries"

  private def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match
    case NullType => ArrowType.Null.INSTANCE
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(16, true)
    case IntegerType => new ArrowType.Int(32, true)
    case LongType => new ArrowType.Int(64, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType | _: CharType | _: VarcharType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TimestampNTZType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case d: DecimalType => new ArrowType.Decimal(d.precision, d.scale, 128)
    case other =>
      throw new UnsupportedOperationException(
        s"Type $other is not supported for local Arrow serialization."
      )
