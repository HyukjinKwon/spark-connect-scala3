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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.connect.proto
import org.apache.spark.sql.{GenericRowWithSchema, Row}
import org.apache.spark.sql.connect.client.arrow.{ArrowUtils, ArrowVectorReader}
import org.apache.spark.sql.types.StructType

/**
 * Materialises the streamed result of an `ExecutePlan` RPC into [[Row]]s.
 *
 * Each [[proto.ExecutePlanResponse]] may carry an `ArrowBatch` whose `data` is a self-contained
 * Arrow IPC stream (schema + record batches). We read every batch with an [[ArrowStreamReader]],
 * convert each row to a [[Row]] (copying values into plain JVM objects), and release the Arrow
 * buffers right away. The optional `schema` field on the response (a Spark `DataType`) is
 * authoritative; if absent we fall back to the schema embedded in the Arrow stream.
 *
 * Results are read lazily on first access and cached, so repeated calls to [[toArray]] / [[schema]]
 * / [[length]] do not re-read the stream.
 *
 * @param responses
 *   the response stream from `SparkConnectClient.execute`
 * @param allocator
 *   Arrow allocator (typically owned by the [[org.apache.spark.sql.SparkSession]])
 * @param timeZoneId
 *   session time zone, reserved for future timestamp localisation
 */
final class SparkResult(
    responses: Iterator[proto.ExecutePlanResponse],
    allocator: BufferAllocator,
    timeZoneId: String = "UTC"
) extends AutoCloseable:

  private var _schema: StructType = null
  private var _operationId: Option[String] = None
  private var _processedRows: Long = 0L
  private val _metrics = ArrayBuffer.empty[(String, Long)]
  private var _rows: Array[Row] = null

  private def materialize(): Unit =
    if _rows != null then return
    val buffer = ArrayBuffer.empty[Row]
    var arrowSchema: StructType = null
    while responses.hasNext do
      val response = responses.next()
      if _operationId.isEmpty && response.operationId.nonEmpty then
        _operationId = Some(response.operationId)
      if response.schema.isDefined then
        _schema = DataTypeProtoConverter.toStructType(response.schema.get)
      response.getMetrics.metrics.foreach { m =>
        m.executionMetrics.foreach { case (k, v) => _metrics += (s"${m.name}.$k" -> v.value) }
      }
      response.responseType.arrowBatch.foreach { batch =>
        _processedRows += batch.rowCount
        val in = new ByteArrayInputStream(batch.data.toByteArray)
        val reader = new ArrowStreamReader(in, allocator)
        try
          while reader.loadNextBatch() do
            val root = reader.getVectorSchemaRoot
            if arrowSchema == null then arrowSchema = ArrowUtils.fromArrowSchema(root.getSchema)
            val vectors = root.getFieldVectors.asScala.toArray
            val rowSchema = if _schema != null then _schema else arrowSchema
            val n = root.getRowCount
            var r = 0
            while r < n do
              val vals = new Array[Any](vectors.length)
              var c = 0
              while c < vectors.length do
                vals(c) = ArrowVectorReader.read(vectors(c), r)
                c += 1
              buffer += new GenericRowWithSchema(vals, rowSchema)
              r += 1
        finally reader.close()
      }
    if _schema == null then _schema = if arrowSchema != null then arrowSchema else StructType.empty
    _rows = buffer.toArray

  /** The schema of the result. Triggers reading of the stream if not yet known. */
  def schema: StructType =
    materialize()
    _schema

  /** Number of rows in the result. */
  def length: Int =
    materialize()
    _rows.length

  /** All rows of the result, in order. */
  def toArray: Array[Row] =
    materialize()
    _rows

  /** A (re-iterable) iterator over the rows. */
  def iterator: Iterator[Row] =
    materialize()
    _rows.iterator

  /** Total number of rows the server reported processing. */
  def processedRowCount: Long =
    materialize()
    _processedRows

  /** The server-assigned operation id, if any. */
  def operationId: Option[String] =
    materialize()
    _operationId

  /** Best-effort SQL metrics collected from the response stream. */
  def metrics: Seq[(String, Long)] =
    materialize()
    _metrics.toSeq

  override def close(): Unit =
    responses match
      case c: AutoCloseable => c.close()
      case _ => ()
