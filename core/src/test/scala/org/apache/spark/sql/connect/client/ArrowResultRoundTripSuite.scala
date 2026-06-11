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

import com.google.protobuf.ByteString
import org.apache.arrow.memory.RootAllocator

import org.apache.spark.connect.proto
import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer
import org.apache.spark.sql.types.*

/**
 * End-to-end test of the result pipeline without a server: build [[Row]]s, serialize them to an
 * Arrow IPC stream, wrap them in a synthetic [[proto.ExecutePlanResponse]], and decode them back
 * through [[SparkResult]]. This exercises [[ArrowSerializer]], the catalyst<->proto schema
 * converter, and the Arrow vector reader together.
 */
class ArrowResultRoundTripSuite extends munit.FunSuite:

  private val allocators = new java.util.concurrent.ConcurrentLinkedQueue[RootAllocator]()

  private def newAllocator(): RootAllocator =
    val a = new RootAllocator(Long.MaxValue)
    allocators.add(a)
    a

  override def afterAll(): Unit =
    allocators.forEach(_.close())

  private def roundTrip(schema: StructType, rows: Seq[Row]): Array[Row] =
    val allocator = newAllocator()
    val bytes = ArrowSerializer.serialize(rows, schema, allocator)
    val response = proto.ExecutePlanResponse(
      responseType = proto.ExecutePlanResponse.ResponseType.ArrowBatch(
        proto.ExecutePlanResponse
          .ArrowBatch(rowCount = rows.length.toLong, data = ByteString.copyFrom(bytes))
      ),
      schema = Some(DataTypeProtoConverter.toConnectProtoType(schema))
    )
    val result = new SparkResult(Iterator.single(response), allocator)
    try result.toArray
    finally result.close()

  test("scalar columns round-trip") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("score", DoubleType),
        StructField("active", BooleanType)
      )
    )
    val rows =
      Seq(Row(1L, "alice", 9.5, true), Row(2L, "bob", 7.0, false), Row(3L, "carol", 8.25, true))
    val out = roundTrip(schema, rows)
    assertEquals(out.length, 3)
    assertEquals(out(0).getLong(0), 1L)
    assertEquals(out(0).getString(1), "alice")
    assertEquals(out(1).getDouble(2), 7.0)
    assertEquals(out(2).getBoolean(3), true)
    assertEquals(out(0).schema.fieldNames.toSeq, Seq("id", "name", "score", "active"))
  }

  test("nulls are preserved") {
    val schema = StructType(Array(StructField("a", IntegerType), StructField("b", StringType)))
    val rows = Seq(Row(1, "x"), Row(null, null), Row(3, "z"))
    val out = roundTrip(schema, rows)
    assertEquals(out.length, 3)
    assert(out(1).isNullAt(0))
    assert(out(1).isNullAt(1))
    assertEquals(out(2).getInt(0), 3)
  }

  test("schema is reported even for empty results") {
    val schema = StructType(Array(StructField("x", IntegerType)))
    val out = roundTrip(schema, Seq.empty)
    assertEquals(out.length, 0)
  }

  test("decimal, date and timestamp round-trip") {
    val schema = StructType(
      Array(
        StructField("amt", DecimalType(10, 2)),
        StructField("d", DateType),
        StructField("ts", TimestampType)
      )
    )
    val date = java.sql.Date.valueOf("2024-01-15")
    val ts = java.sql.Timestamp.valueOf("2024-01-15 12:30:45.0")
    val rows = Seq(Row(new java.math.BigDecimal("123.45"), date, ts))
    val out = roundTrip(schema, rows)
    assertEquals(out(0).getDecimal(0).compareTo(new java.math.BigDecimal("123.45")), 0)
    assertEquals(out(0).getDate(1).toString, "2024-01-15")
    assertEquals(out(0).getTimestamp(2), ts)
  }

  test("processedRowCount reflects the batch") {
    val schema = StructType(Array(StructField("x", IntegerType)))
    val allocator = newAllocator()
    val rows = Seq(Row(1), Row(2))
    val bytes = ArrowSerializer.serialize(rows, schema, allocator)
    val response = proto.ExecutePlanResponse(
      responseType = proto.ExecutePlanResponse.ResponseType.ArrowBatch(
        proto.ExecutePlanResponse.ArrowBatch(rowCount = 2L, data = ByteString.copyFrom(bytes))
      ),
      schema = Some(DataTypeProtoConverter.toConnectProtoType(schema))
    )
    val result = new SparkResult(Iterator.single(response), allocator)
    try assertEquals(result.processedRowCount, 2L)
    finally result.close()
  }
