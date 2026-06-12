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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

import scala.collection.mutable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.util.Text

import org.apache.spark.connect.proto
import org.apache.spark.sql.{GenericRowWithSchema, Row}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Materializes the streaming `ExecutePlanResponse`s of a query into a sequence of [[Row]]s by
 * decoding the embedded Arrow IPC batches. The result is computed lazily on first access and
 * cached.
 */
private[sql] class SparkResult(
    responses: Iterator[proto.ExecutePlanResponse],
    allocator: BufferAllocator
) {

  private var _schema: StructType = null
  private val rows = mutable.ArrayBuffer.empty[Row]
  private var processed = false

  private def processResponses(): Unit = {
    if (processed) return
    while (responses.hasNext) {
      val response = responses.next()
      response.schema.foreach { protoSchema =>
        _schema = DataTypeProtoConverter.toCatalystType(protoSchema) match {
          case s: StructType => s
          case other => StructType(Array(StructField("value", other)))
        }
      }
      response.responseType.arrowBatch.foreach { batch =>
        readArrowBatch(batch.data.toByteArray)
      }
      response.observedMetrics.foreach { om =>
        org.apache.spark.sql.Observation.lookup(om.name).foreach { obs =>
          obs.setMetricsFromLiterals(
            om.values.map(org.apache.spark.sql.Observation.decodeLiteral),
            om.keys
          )
        }
      }
    }
    processed = true
  }

  private def readArrowBatch(bytes: Array[Byte]): Unit = {
    val reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    try {
      val root = reader.getVectorSchemaRoot
      while (reader.loadNextBatch()) {
        if (_schema == null) {
          _schema = SparkResult.schemaFromArrow(root)
        }
        val rowCount = root.getRowCount
        val vectors = root.getFieldVectors
        val numCols = vectors.size()
        var r = 0
        while (r < rowCount) {
          val values = new Array[Any](numCols)
          var c = 0
          while (c < numCols) {
            values(c) = SparkResult.getValue(vectors.get(c), r)
            c += 1
          }
          rows += new GenericRowWithSchema(values, _schema)
          r += 1
        }
      }
    } finally reader.close()
  }

  def schema: StructType = {
    processResponses()
    if (_schema == null) StructType(Array.empty[StructField]) else _schema
  }

  def toArray: Array[Row] = {
    processResponses()
    rows.toArray
  }

  def length: Int = {
    processResponses()
    rows.length
  }

  def iterator: Iterator[Row] = toArray.iterator
}

private[sql] object SparkResult {

  private val MICROS_PER_SECOND = 1000000L

  /** Best-effort schema derivation directly from an Arrow root (used as a fallback). */
  private def schemaFromArrow(root: VectorSchemaRoot): StructType = {
    import scala.jdk.CollectionConverters._
    val fields = root.getSchema.getFields.asScala.map { f =>
      StructField(f.getName, StringType, f.isNullable)
    }
    StructType(fields.toArray)
  }

  /** Extracts the value at row `i` from an Arrow vector and converts it to a Scala value. */
  def getValue(vector: ValueVector, i: Int): Any = {
    if (vector.isNull(i)) return null
    vector match {
      case v: BitVector => v.get(i) != 0
      case v: TinyIntVector => v.get(i)
      case v: SmallIntVector => v.get(i)
      case v: IntVector => v.get(i)
      case v: BigIntVector => v.get(i)
      case v: Float4Vector => v.get(i)
      case v: Float8Vector => v.get(i)
      case v: DecimalVector => v.getObject(i)
      case v: VarCharVector => new String(v.get(i), StandardCharsets.UTF_8)
      case v: LargeVarCharVector => new String(v.get(i), StandardCharsets.UTF_8)
      case v: VarBinaryVector => v.get(i)
      case v: LargeVarBinaryVector => v.get(i)
      case v: DateDayVector => LocalDate.ofEpochDay(v.get(i).toLong)
      case v: TimeStampMicroTZVector => microsToInstant(v.get(i))
      case v: TimeStampMicroVector => microsToLocalDateTime(v.get(i))
      case v: DurationVector => v.getObject(i)
      case v: NullVector => null
      // MapVector extends ListVector, so it must be matched first.
      case v: MapVector => mapValue(v, i)
      case v: ListVector => normalize(v.getObject(i))
      case v: StructVector => structValue(v, i)
      case other => normalize(other.getObject(i))
    }
  }

  private def microsToInstant(micros: Long): Instant = {
    val seconds = Math.floorDiv(micros, MICROS_PER_SECOND)
    val microsOfSecond = Math.floorMod(micros, MICROS_PER_SECOND)
    Instant.ofEpochSecond(seconds, microsOfSecond * 1000)
  }

  private def microsToLocalDateTime(micros: Long): LocalDateTime =
    LocalDateTime.ofInstant(microsToInstant(micros), ZoneOffset.UTC)

  private def structValue(vector: StructVector, i: Int): Row = {
    import scala.jdk.CollectionConverters._
    val children = vector.getChildrenFromFields.asScala
    val values = children.map(child => getValue(child, i)).toArray[Any]
    Row.fromSeq(values.toIndexedSeq)
  }

  /**
   * Decodes a map cell. A [[MapVector]] is a list of `key`/`value` entry structs; we walk the entry
   * range for row `i` and decode each key and value with [[getValue]] so that nested key/value
   * types are handled, building an insertion-ordered Scala [[Map]].
   */
  private def mapValue(vector: MapVector, i: Int): Any = {
    val struct = vector.getDataVector.asInstanceOf[StructVector]
    val keyVector = struct.getChild(MapVector.KEY_NAME)
    val valueVector = struct.getChild(MapVector.VALUE_NAME)
    val builder = scala.collection.mutable.LinkedHashMap.empty[Any, Any]
    val start = vector.getElementStartIndex(i)
    val end = vector.getElementEndIndex(i)
    var p = start
    while (p < end) {
      builder += (getValue(keyVector, p) -> getValue(valueVector, p))
      p += 1
    }
    builder.toMap
  }

  /** Recursively converts Arrow's Java result objects (Text, List, Map) into Scala values. */
  private def normalize(value: Any): Any = value match {
    case null => null
    case t: Text => t.toString
    case l: java.util.List[_] =>
      import scala.jdk.CollectionConverters._
      l.asScala.map(normalize).toSeq
    case m: java.util.Map[_, _] =>
      import scala.jdk.CollectionConverters._
      m.asScala.map { case (k, v) => normalize(k) -> normalize(v) }.toMap
    case other => other
  }
}
