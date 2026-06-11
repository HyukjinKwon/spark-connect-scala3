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

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.jdk.CollectionConverters.*

import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.{LargeListVector, ListVector, MapVector, StructVector}

import org.apache.spark.sql.{GenericRowWithSchema, Row}
import org.apache.spark.sql.types.StructType

/**
 * Reads a single value out of an Apache Arrow [[ValueVector]] at a given row index and converts it
 * to the JVM type the rest of the client expects (the same mapping Apache Spark uses for `Row`).
 *
 * Values are fully materialised into plain JVM objects, so the caller is free to release the
 * underlying Arrow buffers immediately after reading a batch.
 */
private[sql] object ArrowVectorReader:

  /** The offset buffer of a (small) list/map vector stores 32-bit offsets. */
  private final val OFFSET_WIDTH = 4

  def read(vector: ValueVector, index: Int): Any =
    if vector.isNull(index) then null
    else
      vector match
        case v: BitVector => v.get(index) != 0
        case v: TinyIntVector => v.get(index)
        case v: SmallIntVector => v.get(index)
        case v: IntVector => v.get(index)
        case v: BigIntVector => v.get(index)
        case v: UInt1Vector => v.get(index)
        case v: UInt2Vector => v.get(index).toInt
        case v: UInt4Vector => v.get(index)
        case v: UInt8Vector => v.get(index)
        case v: Float4Vector => v.get(index)
        case v: Float8Vector => v.get(index)
        case v: DecimalVector => v.getObject(index)
        case v: Decimal256Vector => v.getObject(index)
        case v: VarCharVector => new String(v.get(index), UTF_8)
        case v: LargeVarCharVector => new String(v.get(index), UTF_8)
        case v: VarBinaryVector => v.get(index)
        case v: LargeVarBinaryVector => v.get(index)
        case v: FixedSizeBinaryVector => v.get(index)
        case v: DateDayVector => Date.valueOf(LocalDate.ofEpochDay(v.get(index).toLong))
        case v: DateMilliVector => new Date(v.get(index))
        // Timezone-aware timestamps -> java.sql.Timestamp (an instant).
        case v: TimeStampSecTZVector => secondsToTimestamp(v.get(index))
        case v: TimeStampMilliTZVector => millisToTimestamp(v.get(index))
        case v: TimeStampMicroTZVector => microsToTimestamp(v.get(index))
        case v: TimeStampNanoTZVector => nanosToTimestamp(v.get(index))
        // Timezone-naive timestamps -> java.time.LocalDateTime (wall clock).
        case v: TimeStampSecVector => secondsToLocalDateTime(v.get(index))
        case v: TimeStampMilliVector => millisToLocalDateTime(v.get(index))
        case v: TimeStampMicroVector => microsToLocalDateTime(v.get(index))
        case v: TimeStampNanoVector => nanosToLocalDateTime(v.get(index))
        case v: ListVector => readList(v.getDataVector, start(v, index), end(v, index))
        case v: LargeListVector => readLargeList(v, index)
        case v: MapVector => readMap(v, index)
        case v: StructVector => readStruct(v, index)
        case v: NullVector => null
        case other => other.getObject(index)

  // --- nested ---------------------------------------------------------------

  private def start(v: ListVector, i: Int): Int =
    v.getOffsetBuffer.getInt(i.toLong * OFFSET_WIDTH)
  private def end(v: ListVector, i: Int): Int =
    v.getOffsetBuffer.getInt((i + 1).toLong * OFFSET_WIDTH)

  private def readList(child: ValueVector, from: Int, until: Int): Seq[Any] =
    val b = Vector.newBuilder[Any]
    var j = from
    while j < until do
      b += read(child, j)
      j += 1
    b.result()

  private def readLargeList(v: LargeListVector, i: Int): Seq[Any] =
    val from = v.getOffsetBuffer.getLong(i.toLong * 8).toInt
    val until = v.getOffsetBuffer.getLong((i + 1).toLong * 8).toInt
    readList(v.getDataVector, from, until)

  private def readMap(v: MapVector, i: Int): Map[Any, Any] =
    val from = start(v, i)
    val until = end(v, i)
    val entries = v.getDataVector.asInstanceOf[StructVector]
    val keys = entries.getChildByOrdinal(0)
    val values = entries.getChildByOrdinal(1)
    val b = Map.newBuilder[Any, Any]
    var j = from
    while j < until do
      b += (read(keys, j) -> read(values, j))
      j += 1
    b.result()

  private def readStruct(v: StructVector, i: Int): Row =
    val children = v.getChildrenFromFields.asScala
    val values = new Array[Any](children.size)
    var c = 0
    while c < children.size do
      values(c) = read(children(c), i)
      c += 1
    val schema = ArrowUtils.fromArrowField(v.getField).dataType match
      case s: StructType => s
      case _ => StructType(Array.empty[org.apache.spark.sql.types.StructField])
    new GenericRowWithSchema(values, schema)

  // --- temporal helpers -----------------------------------------------------

  private def secondsToTimestamp(s: Long): Timestamp = new Timestamp(s * 1000L)
  private def millisToTimestamp(ms: Long): Timestamp = new Timestamp(ms)
  private def microsToTimestamp(micros: Long): Timestamp =
    val seconds = Math.floorDiv(micros, 1000000L)
    val microOf = Math.floorMod(micros, 1000000L)
    val ts = new Timestamp(seconds * 1000L)
    ts.setNanos((microOf * 1000L).toInt)
    ts
  private def nanosToTimestamp(nanos: Long): Timestamp =
    val seconds = Math.floorDiv(nanos, 1000000000L)
    val nanoOf = Math.floorMod(nanos, 1000000000L)
    val ts = new Timestamp(seconds * 1000L)
    ts.setNanos(nanoOf.toInt)
    ts

  private def secondsToLocalDateTime(s: Long): LocalDateTime =
    LocalDateTime.ofEpochSecond(s, 0, ZoneOffset.UTC)
  private def millisToLocalDateTime(ms: Long): LocalDateTime =
    LocalDateTime.ofEpochSecond(
      Math.floorDiv(ms, 1000L),
      Math.floorMod(ms, 1000L).toInt * 1000000,
      ZoneOffset.UTC
    )
  private def microsToLocalDateTime(micros: Long): LocalDateTime =
    LocalDateTime.ofEpochSecond(
      Math.floorDiv(micros, 1000000L),
      Math.floorMod(micros, 1000000L).toInt * 1000,
      ZoneOffset.UTC
    )
  private def nanosToLocalDateTime(nanos: Long): LocalDateTime =
    LocalDateTime.ofEpochSecond(
      Math.floorDiv(nanos, 1000000000L),
      Math.floorMod(nanos, 1000000000L).toInt,
      ZoneOffset.UTC
    )
