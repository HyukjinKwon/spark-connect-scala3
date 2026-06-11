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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.jdk.CollectionConverters.*

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

/**
 * Serializes local [[Row]]s into a single Arrow IPC stream, used by `SparkSession.createDataFrame`
 * to ship local data to the server as a `LocalRelation`. This is the inverse of
 * [[org.apache.spark.sql.connect.client.SparkResult]] and mirrors the `from_rows` helper in the
 * reference Ruby client.
 *
 * Scalar (non-nested) column types are supported, which covers the overwhelming majority of local
 * data sets; nested array/struct/map local data raises a clear error.
 */
private[sql] object ArrowSerializer:

  /** Serialize `rows` (matching `schema`) into Arrow IPC stream bytes. */
  def serialize(
      rows: Seq[Row],
      schema: StructType,
      allocator: BufferAllocator,
      timeZoneId: String = ArrowUtils.defaultTimeZoneId
  ): Array[Byte] =
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    try
      root.allocateNew()
      val vectors = root.getFieldVectors.asScala.toArray
      var r = 0
      while r < rows.length do
        val row = rows(r)
        var c = 0
        while c < vectors.length do
          setValue(vectors(c), r, if c < row.length then row.get(c) else null)
          c += 1
        r += 1
      root.setRowCount(rows.length)
      val writer = new ArrowStreamWriter(root, null, out)
      try
        writer.start()
        writer.writeBatch()
        writer.end()
      finally writer.close()
      out.toByteArray
    finally root.close()

  private def setValue(vector: FieldVector, i: Int, value: Any): Unit =
    if value == null then vector.setNull(i)
    else
      vector match
        case v: BitVector => v.setSafe(i, if asBoolean(value) then 1 else 0)
        case v: TinyIntVector => v.setSafe(i, asNumber(value).byteValue)
        case v: SmallIntVector => v.setSafe(i, asNumber(value).shortValue)
        case v: IntVector => v.setSafe(i, asNumber(value).intValue)
        case v: BigIntVector => v.setSafe(i, asNumber(value).longValue)
        case v: Float4Vector => v.setSafe(i, asNumber(value).floatValue)
        case v: Float8Vector => v.setSafe(i, asNumber(value).doubleValue)
        case v: DecimalVector =>
          v.setSafe(i, asBigDecimal(value).setScale(v.getScale, java.math.RoundingMode.HALF_UP))
        case v: VarCharVector =>
          v.setSafe(i, value.toString.getBytes(UTF_8))
        case v: VarBinaryVector =>
          v.setSafe(i, value.asInstanceOf[Array[Byte]])
        case v: DateDayVector =>
          v.setSafe(i, asEpochDay(value))
        case v: TimeStampMicroTZVector =>
          v.setSafe(i, asEpochMicros(value))
        case v: TimeStampMicroVector =>
          v.setSafe(i, asEpochMicros(value))
        case other =>
          throw new UnsupportedOperationException(
            s"Local serialization of vector ${other.getClass.getSimpleName} is not supported."
          )

  private def asBoolean(v: Any): Boolean = v match
    case b: Boolean => b
    case b: java.lang.Boolean => b.booleanValue
    case n: Number => n.intValue != 0
    case s: String => s.toBoolean
    case _ => throw coercion(v, "boolean")

  private def asNumber(v: Any): Number = v match
    case n: Number => n
    case b: Boolean => if b then 1 else 0
    case s: String => BigDecimal(s).bigDecimal
    case _ => throw coercion(v, "number")

  private def asBigDecimal(v: Any): java.math.BigDecimal = v match
    case d: java.math.BigDecimal => d
    case d: BigDecimal => d.bigDecimal
    case n: Number => new java.math.BigDecimal(n.toString)
    case _ => throw coercion(v, "decimal")

  private def asEpochDay(v: Any): Int = v match
    case d: Date => d.toLocalDate.toEpochDay.toInt
    case d: LocalDate => d.toEpochDay.toInt
    case n: Number => n.intValue
    case _ => throw coercion(v, "date")

  private def asEpochMicros(v: Any): Long = v match
    case t: Timestamp =>
      Math.multiplyExact(t.getTime / 1000L, 1000000L) + (t.getNanos / 1000L)
    case i: Instant =>
      Math.addExact(Math.multiplyExact(i.getEpochSecond, 1000000L), i.getNano / 1000L)
    case d: LocalDateTime =>
      val instant = d.toInstant(ZoneOffset.UTC)
      Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
    case n: Number => n.longValue
    case _ => throw coercion(v, "timestamp")

  private def coercion(v: Any, target: String): Throwable =
    new IllegalArgumentException(
      s"Cannot coerce value '$v' of type ${v.getClass.getName} to $target."
    )
