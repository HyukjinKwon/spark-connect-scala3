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

import java.io.ByteArrayOutputStream
import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.types._

/**
 * Encodes local rows into an Apache Arrow IPC stream so that the client can ship local data to the
 * server as a `LocalRelation` (used by `SparkSession.createDataFrame`).
 *
 * This is the symmetric ENCODE counterpart to [[SparkResult]] (which DECODES the Arrow batches
 * returned by the server). The Spark-type-to-Arrow-type mapping and the per-cell value conversions
 * here mirror the decode logic in [[SparkResult.getValue]] so that a value round-trips faithfully.
 *
 * Modelled on the same-author Ruby reference (`spark_connect/arrow.rb`, `from_rows` /
 * `build_arrow_schema` / `arrow_field_type`).
 */
object ArrowSerializer {

  private val MICROS_PER_SECOND = 1000000L
  private val UTC = "UTC"

  /**
   * Serializes `rows` into a single, self-contained Arrow IPC stream (schema + one record batch).
   *
   * @param rows
   *   the local data, one inner `Seq` per row, ordered to match `schema.fields`.
   * @param schema
   *   the Spark schema describing the columns.
   * @param allocator
   *   the Arrow [[BufferAllocator]] used to back the vectors (caller owns its lifecycle; the
   *   [[VectorSchemaRoot]] created here is closed before returning).
   * @return
   *   the Arrow IPC stream bytes.
   */
  def serialize(
      rows: Seq[Seq[Any]],
      schema: StructType,
      allocator: BufferAllocator
  ): Array[Byte] = {
    val arrowSchema = toArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    try {
      val writer = new ArrowStreamWriter(root, /* provider = */ null, out)
      try {
        writer.start()
        val rowCount = rows.size
        root.setRowCount(rowCount)
        val vectors = root.getFieldVectors
        // Populate column-by-column so each vector is filled densely.
        var c = 0
        while (c < schema.fields.length) {
          val vector = vectors.get(c)
          val dataType = schema.fields(c).dataType
          var r = 0
          while (r < rowCount) {
            val row = rows(r)
            val value = if (c < row.length) row(c) else null
            setValue(vector, r, value, dataType)
            r += 1
          }
          vector.setValueCount(rowCount)
          c += 1
        }
        writer.writeBatch()
        writer.end()
      } finally writer.close()
    } finally root.close()
    out.toByteArray
  }

  // ---------------------------------------------------------------------------
  // Schema construction
  // ---------------------------------------------------------------------------

  /** Builds an Arrow [[Schema]] from a Spark [[StructType]]. */
  def toArrowSchema(schema: StructType): Schema =
    new Schema(schema.fields.map(f => toArrowField(f.name, f.dataType, f.nullable)).toList.asJava)

  /** Builds an Arrow [[Field]] (with nested children) from a Spark [[DataType]]. */
  private def toArrowField(name: String, dataType: DataType, nullable: Boolean): Field =
    dataType match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        // Arrow lists use a single child field conventionally named "element".
        val element = toArrowField("element", elementType, containsNull)
        new Field(name, fieldType, List(element).asJava)

      case StructType(structFields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        val children = structFields.map(f => toArrowField(f.name, f.dataType, f.nullable)).toList
        new Field(name, fieldType, children.asJava)

      case MapType(keyType, valueType, valueContainsNull) =>
        // Arrow maps are a List<Struct<key, value>>; keysSorted = false.
        val fieldType = new FieldType(nullable, new ArrowType.Map( /* keysSorted = */ false), null)
        val entries = new Field(
          MapVector.DATA_VECTOR_NAME,
          // Map entries (the struct) are non-nullable per the Arrow spec.
          new FieldType(false, ArrowType.Struct.INSTANCE, null),
          List(
            toArrowField(MapVector.KEY_NAME, keyType, nullable = false),
            toArrowField(MapVector.VALUE_NAME, valueType, valueContainsNull)
          ).asJava
        )
        new Field(name, fieldType, List(entries).asJava)

      case other =>
        // Leaf/atomic types: no children.
        new Field(name, new FieldType(nullable, toArrowType(other), null), null)
    }

  /**
   * Maps a Spark primitive/atomic [[DataType]] to the corresponding Arrow [[ArrowType]]. Nested
   * types (Array/Struct/Map) are handled in [[toArrowField]]. Unknown types default to Utf8,
   * matching the Ruby reference's `:string` default.
   */
  private def toArrowType(dataType: DataType): ArrowType = dataType match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, /* isSigned = */ true)
    case ShortType => new ArrowType.Int(16, true)
    case IntegerType => new ArrowType.Int(32, true)
    case LongType => new ArrowType.Int(64, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType | _: CharType | _: VarcharType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, UTC)
    case TimestampNTZType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case d: DecimalType => new ArrowType.Decimal(d.precision, d.scale, /* bitWidth = */ 128)
    case _ => ArrowType.Utf8.INSTANCE
  }

  // ---------------------------------------------------------------------------
  // Value population
  // ---------------------------------------------------------------------------

  /** Sets the value at row `i` of `vector`, handling nulls per cell. */
  private def setValue(vector: ValueVector, i: Int, value: Any, dataType: DataType): Unit = {
    if (value == null) {
      setNull(vector, i)
      return
    }
    vector match {
      case v: BitVector =>
        v.setSafe(i, if (toBoolean(value)) 1 else 0)
      case v: TinyIntVector =>
        v.setSafe(i, toByte(value))
      case v: SmallIntVector =>
        v.setSafe(i, toShort(value))
      case v: IntVector =>
        v.setSafe(i, toInt(value))
      case v: BigIntVector =>
        v.setSafe(i, toLong(value))
      case v: Float4Vector =>
        v.setSafe(i, toFloat(value))
      case v: Float8Vector =>
        v.setSafe(i, toDouble(value))
      case v: VarCharVector =>
        v.setSafe(i, value.toString.getBytes(StandardCharsets.UTF_8))
      case v: VarBinaryVector =>
        v.setSafe(i, toBytes(value))
      case v: DateDayVector =>
        v.setSafe(i, toEpochDay(value))
      case v: TimeStampMicroTZVector =>
        v.setSafe(i, toEpochMicros(value))
      case v: TimeStampMicroVector =>
        v.setSafe(i, toEpochMicros(value))
      case v: DecimalVector =>
        // DecimalVector.setSafe(int, BigDecimal) rescales to the vector's declared scale.
        v.setSafe(i, toBigDecimal(value))
      // MapVector extends ListVector, so it must be matched first.
      case v: MapVector =>
        setMap(v, i, value, dataType.asInstanceOf[MapType])
      case v: ListVector =>
        setList(v, i, value, dataType.asInstanceOf[ArrayType])
      case v: StructVector =>
        setStruct(v, i, value, dataType.asInstanceOf[StructType])
      case other =>
        throw new UnsupportedOperationException(
          s"Cannot set value into Arrow vector of type ${other.getClass.getName}"
        )
    }
  }

  /** Marks the cell at row `i` of `vector` as null using the vector-specific API. */
  private def setNull(vector: ValueVector, i: Int): Unit = vector match {
    case v: BitVector => v.setNull(i)
    case v: TinyIntVector => v.setNull(i)
    case v: SmallIntVector => v.setNull(i)
    case v: IntVector => v.setNull(i)
    case v: BigIntVector => v.setNull(i)
    case v: Float4Vector => v.setNull(i)
    case v: Float8Vector => v.setNull(i)
    case v: VarCharVector => v.setNull(i)
    case v: VarBinaryVector => v.setNull(i)
    case v: DateDayVector => v.setNull(i)
    case v: TimeStampMicroTZVector => v.setNull(i)
    case v: TimeStampMicroVector => v.setNull(i)
    case v: DecimalVector => v.setNull(i)
    // MapVector extends ListVector, so it must be matched first.
    case v: MapVector => v.setNull(i)
    case v: ListVector => v.setNull(i)
    case v: StructVector => v.setNull(i)
    case v: BaseFixedWidthVector => v.setNull(i)
    case v: BaseVariableWidthVector => v.setNull(i)
    case _ =>
      // Last resort: zero out validity via the FieldVector contract.
      vector.asInstanceOf[FieldVector].setNull(i)
  }

  // ---------------------------------------------------------------------------
  // Nested-type population (best-effort)
  // ---------------------------------------------------------------------------

  private def setList(vector: ListVector, index: Int, value: Any, arrayType: ArrayType): Unit = {
    val elements = toSeq(value)
    // startNewValue records this list's start offset and returns the position in the data vector
    // at which to begin writing children; endValue finalizes the offset for this row.
    val start = vector.startNewValue(index)
    val dataVector = vector.getDataVector
    var pos = start
    elements.foreach { element =>
      setValue(dataVector, pos, element, arrayType.elementType)
      pos += 1
    }
    vector.endValue(index, elements.size)
    dataVector.setValueCount(pos)
  }

  private def setStruct(
      vector: StructVector,
      index: Int,
      value: Any,
      structType: StructType
  ): Unit = {
    vector.setIndexDefined(index)
    val values = toSeq(value)
    val children = vector.getChildrenFromFields
    var c = 0
    while (c < structType.fields.length && c < children.size()) {
      val child = children.get(c)
      val fieldValue = if (c < values.length) values(c) else null
      setValue(child, index, fieldValue, structType.fields(c).dataType)
      c += 1
    }
  }

  private def setMap(vector: MapVector, index: Int, value: Any, mapType: MapType): Unit = {
    val entries = value match {
      case m: scala.collection.Map[_, _] => m.toSeq
      case m: java.util.Map[_, _] => m.asScala.toSeq
      case other =>
        throw new IllegalArgumentException(
          s"Cannot encode value of type ${other.getClass.getName} as a MapType"
        )
    }
    // A MapVector is a ListVector whose data vector is a StructVector with `key` and `value`
    // children. Populate the children directly (mirroring setList/setStruct) rather than via
    // UnionMapWriter, which leaves the vector value counts in a state that does not round-trip
    // through the Arrow IPC stream.
    val start = vector.startNewValue(index)
    val struct = vector.getDataVector.asInstanceOf[StructVector]
    val keyVector = struct.getChild(MapVector.KEY_NAME)
    val valueVector = struct.getChild(MapVector.VALUE_NAME)
    var pos = start
    entries.foreach { case (k, v) =>
      struct.setIndexDefined(pos)
      setValue(keyVector, pos, k, mapType.keyType)
      setValue(valueVector, pos, v, mapType.valueType)
      pos += 1
    }
    vector.endValue(index, entries.size)
    struct.setValueCount(pos)
    keyVector.setValueCount(pos)
    valueVector.setValueCount(pos)
  }

  // ---------------------------------------------------------------------------
  // Scalar value coercion helpers (mirror SparkResult decode semantics)
  // ---------------------------------------------------------------------------

  private def toBoolean(value: Any): Boolean = value match {
    case b: Boolean => b
    case b: java.lang.Boolean => b.booleanValue()
    case n: Number => n.intValue() != 0
    case s: String => s.toBoolean
    case other => throw coerceError(other, "Boolean")
  }

  private def toByte(value: Any): Byte = value match {
    case n: Number => n.byteValue()
    case b: Boolean => if (b) 1 else 0
    case s: String => s.toByte
    case other => throw coerceError(other, "Byte")
  }

  private def toShort(value: Any): Short = value match {
    case n: Number => n.shortValue()
    case s: String => s.toShort
    case other => throw coerceError(other, "Short")
  }

  private def toInt(value: Any): Int = value match {
    case n: Number => n.intValue()
    case s: String => s.toInt
    case other => throw coerceError(other, "Int")
  }

  private def toLong(value: Any): Long = value match {
    case n: Number => n.longValue()
    case s: String => s.toLong
    case other => throw coerceError(other, "Long")
  }

  private def toFloat(value: Any): Float = value match {
    case n: Number => n.floatValue()
    case s: String => s.toFloat
    case other => throw coerceError(other, "Float")
  }

  private def toDouble(value: Any): Double = value match {
    case n: Number => n.doubleValue()
    case s: String => s.toDouble
    case other => throw coerceError(other, "Double")
  }

  private def toBytes(value: Any): Array[Byte] = value match {
    case b: Array[Byte] => b
    case s: String => s.getBytes(StandardCharsets.UTF_8)
    case other => throw coerceError(other, "binary")
  }

  private def toBigDecimal(value: Any): JBigDecimal = value match {
    case d: JBigDecimal => d
    case d: scala.math.BigDecimal => d.bigDecimal
    case d: BigInt => new JBigDecimal(d.bigInteger)
    case n: Number => JBigDecimal.valueOf(n.doubleValue())
    case s: String => new JBigDecimal(s)
    case other => throw coerceError(other, "Decimal")
  }

  /** Converts a date-ish value to an epoch-day `Int` (mirrors [[SparkResult]] decode). */
  private def toEpochDay(value: Any): Int = value match {
    case d: LocalDate => d.toEpochDay.toInt
    case d: java.sql.Date => d.toLocalDate.toEpochDay.toInt
    case n: Number => n.intValue()
    case other => throw coerceError(other, "Date")
  }

  /** Converts a timestamp-ish value to epoch microseconds (mirrors [[SparkResult]] decode). */
  private def toEpochMicros(value: Any): Long = value match {
    case i: Instant => instantToMicros(i)
    case t: java.sql.Timestamp => instantToMicros(t.toInstant)
    case ldt: LocalDateTime => instantToMicros(ldt.toInstant(ZoneOffset.UTC))
    case n: Number => n.longValue()
    case other => throw coerceError(other, "Timestamp")
  }

  private def instantToMicros(instant: Instant): Long = {
    val seconds = instant.getEpochSecond
    val micros = instant.getNano / 1000L
    Math.addExact(Math.multiplyExact(seconds, MICROS_PER_SECOND), micros)
  }

  private def toSeq(value: Any): Seq[Any] = value match {
    case s: Seq[_] => s
    case a: Array[_] => a.toSeq
    case it: Iterable[_] => it.toSeq
    case l: java.util.List[_] => l.asScala.toSeq
    case other => throw coerceError(other, "Array")
  }

  private def coerceError(value: Any, target: String): IllegalArgumentException =
    new IllegalArgumentException(
      s"Cannot coerce value of type ${value.getClass.getName} to $target"
    )
}
