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

import java.time.{Instant, LocalDate, LocalDateTime}

import com.google.protobuf.ByteString
import org.apache.arrow.memory.RootAllocator

import org.apache.spark.connect.proto
import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.*

/**
 * Round-trip coverage for the Arrow data-marshalling path that backs every query result. Local rows
 * are encoded with [[ArrowSerializer]] into an Arrow IPC stream, wrapped in `ExecutePlanResponse`
 * messages exactly as the server would send them, and decoded back into [[Row]]s with
 * [[SparkResult]]. This exercises the two most correctness-critical pieces of the client (Arrow
 * encode and decode) without a live server, asserting per-type fidelity, null handling, and nested
 * structures.
 */
class SparkResultRoundTripSuite extends munit.FunSuite {

  private val allocatorFixture = new munit.Fixture[RootAllocator]("arrow-allocator") {
    private var allocator: RootAllocator = null
    def apply(): RootAllocator = allocator
    override def beforeEach(context: BeforeEach): Unit =
      allocator = new RootAllocator(Long.MaxValue)
    override def afterEach(context: AfterEach): Unit = allocator.close()
  }

  override def munitFixtures: Seq[munit.Fixture[?]] = Seq(allocatorFixture)

  /** Encodes `rows` against `schema` and decodes them back through a fresh [[SparkResult]]. */
  private def roundTrip(schema: StructType, rows: Seq[Seq[Any]]): SparkResult = {
    val allocator = allocatorFixture()
    val bytes = ArrowSerializer.serialize(rows, schema, allocator)
    val schemaResponse = proto
      .ExecutePlanResponse(
        sessionId = "s",
        serverSideSessionId = ""
      )
      .withSchema(DataTypeProtoConverter.toConnectProtoType(schema))
    val batchResponse = proto
      .ExecutePlanResponse(
        sessionId = "s",
        serverSideSessionId = ""
      )
      .withArrowBatch(
        proto.ExecutePlanResponse
          .ArrowBatch(rowCount = rows.size, data = ByteString.copyFrom(bytes))
      )
    new SparkResult(Iterator(schemaResponse, batchResponse), allocator)
  }

  test("decodes the schema reported by the server") {
    val schema = StructType(
      Array(StructField("id", LongType), StructField("name", StringType, nullable = true))
    )
    val result = roundTrip(schema, Seq(Seq(1L, "a")))
    assertEquals(result.schema, schema)
  }

  test("round-trips every primitive type") {
    val schema = StructType(
      Array(
        StructField("b", BooleanType),
        StructField("by", ByteType),
        StructField("s", ShortType),
        StructField("i", IntegerType),
        StructField("l", LongType),
        StructField("f", FloatType),
        StructField("d", DoubleType),
        StructField("str", StringType),
        StructField("bin", BinaryType)
      )
    )
    val bin = Array[Byte](1, 2, 3)
    val result =
      roundTrip(schema, Seq(Seq(true, 1.toByte, 2.toShort, 3, 4L, 1.5f, 2.5d, "hello", bin)))
    val rows = result.toArray
    assertEquals(rows.length, 1)
    val r = rows(0)
    assertEquals(r.getBoolean(0), true)
    assertEquals(r.getByte(1), 1.toByte)
    assertEquals(r.getShort(2), 2.toShort)
    assertEquals(r.getInt(3), 3)
    assertEquals(r.getLong(4), 4L)
    assertEquals(r.getFloat(5), 1.5f)
    assertEquals(r.getDouble(6), 2.5d)
    assertEquals(r.getString(7), "hello")
    assertEquals(r.getAs[Array[Byte]](8).toSeq, bin.toSeq)
  }

  test("round-trips nulls per cell") {
    val schema = StructType(
      Array(
        StructField("i", IntegerType, nullable = true),
        StructField("str", StringType, nullable = true),
        StructField("d", DoubleType, nullable = true)
      )
    )
    val result = roundTrip(schema, Seq(Seq(null, null, null), Seq(7, "x", 1.0d)))
    val rows = result.toArray
    assertEquals(rows.length, 2)
    assert(rows(0).isNullAt(0))
    assert(rows(0).isNullAt(1))
    assert(rows(0).isNullAt(2))
    assertEquals(rows(1).getInt(0), 7)
    assertEquals(rows(1).getString(1), "x")
    assertEquals(rows(1).getDouble(2), 1.0d)
  }

  test("round-trips date and timestamp types") {
    val schema = StructType(
      Array(
        StructField("date", DateType),
        StructField("ts", TimestampType),
        StructField("ntz", TimestampNTZType)
      )
    )
    val date = LocalDate.of(2024, 2, 29)
    val instant = Instant.parse("2024-02-29T12:34:56.000789Z")
    val ldt = LocalDateTime.of(2024, 2, 29, 12, 34, 56)
    val result = roundTrip(schema, Seq(Seq(date, instant, ldt)))
    val r = result.toArray.head
    assertEquals(r.getLocalDate(0), date)
    // Microsecond precision survives the Arrow MICROSECOND timestamp encoding.
    assertEquals(r.getInstant(1), Instant.parse("2024-02-29T12:34:56.000789Z"))
    assertEquals(r.getAs[LocalDateTime](2), ldt)
  }

  test("round-trips decimal with declared scale") {
    val schema = StructType(Array(StructField("amount", DecimalType(10, 2))))
    val value = new java.math.BigDecimal("123.45")
    val result = roundTrip(schema, Seq(Seq(value)))
    val decoded = result.toArray.head.getDecimal(0)
    assertEquals(decoded.compareTo(new java.math.BigDecimal("123.45")), 0)
    assertEquals(decoded.scale(), 2)
  }

  test("round-trips an array column") {
    val schema = StructType(Array(StructField("tags", ArrayType(StringType))))
    val result = roundTrip(schema, Seq(Seq(Seq("a", "b", "c"))))
    assertEquals(result.toArray.head.getSeq[String](0).toSeq, Seq("a", "b", "c"))
  }

  test("round-trips a struct column") {
    val inner = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    val schema = StructType(Array(StructField("person", inner)))
    val result = roundTrip(schema, Seq(Seq(Seq("Alice", 30))))
    val nested: Row = result.toArray.head.getStruct(0)
    assertEquals(nested.getString(0), "Alice")
    assertEquals(nested.getInt(1), 30)
  }

  test("round-trips a map column") {
    val schema = StructType(Array(StructField("counts", MapType(StringType, IntegerType))))
    val result = roundTrip(schema, Seq(Seq(Map("a" -> 1, "b" -> 2))))
    val decoded = result.toArray.head.getMap[String, Int](0)
    assertEquals(decoded.get("a"), Some(1))
    assertEquals(decoded.get("b"), Some(2))
  }

  test("reports an empty result with the declared schema") {
    val schema = StructType(Array(StructField("id", LongType)))
    val result = roundTrip(schema, Seq.empty)
    assertEquals(result.length, 0)
    assertEquals(result.schema, schema)
    assert(!result.iterator.hasNext)
  }
}
