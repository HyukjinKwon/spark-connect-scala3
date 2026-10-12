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
import java.util.TimeZone

import com.google.protobuf.ByteString
import org.apache.arrow.memory.RootAllocator

import org.apache.spark.connect.proto
import org.apache.spark.sql.{Column, Encoder, Row}
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

  Seq("UTC", "America/Los_Angeles").foreach: zone =>
    test(s"round-trips nullable java.sql.Date calendar dates through Arrow in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val encoder = Encoder[java.sql.Date]
        val dates = Seq("1969-12-31", "1000-01-01", "0001-01-01").map(LocalDate.parse)
        val input = dates.map(java.sql.Date.valueOf) :+ null
        val result = roundTrip(encoder.schema, input.map(value => encoder.toRow(value).toSeq))
        val decoded = result.toArray.map(encoder.fromRow).toSeq
        assertEquals(decoded, input)
        assertEquals(decoded.dropRight(1).map(_.toLocalDate), dates)
        assertEquals(decoded.last, null)
      finally TimeZone.setDefault(originalZone)

  test("round-trips nullable java.sql.Timestamp values through Arrow with microsecond precision"):
    val encoder = Encoder[java.sql.Timestamp]
    val input = Seq(java.sql.Timestamp.from(Instant.parse("1969-12-31T23:59:59.999999Z")), null)
    val result = roundTrip(encoder.schema, input.map(value => encoder.toRow(value).toSeq))
    assertEquals(result.toArray.map(encoder.fromRow).toSeq, input)

  Seq("UTC", "America/Los_Angeles").foreach: zone =>
    test(s"decodes historical SQL timestamp fields from independent Arrow instants in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val local = Seq("0001-01-01T00:00:00", "1000-01-01T00:00:00.123456", "1800-01-01T00:00:00")
          .map(LocalDateTime.parse)
        val instants = local.map(_.atZone(java.time.ZoneId.of(zone)).toInstant)
        val wireEncoder = Encoder[Instant]
        val result = roundTrip(wireEncoder.schema, instants.map(i => wireEncoder.toRow(i).toSeq))
        val decoded = result.toArray.map(Encoder[java.sql.Timestamp].fromRow).toSeq
        assertEquals(decoded.map(_.toLocalDateTime), local)
      finally TimeZone.setDefault(originalZone)

    test(s"encodes historical SQL timestamps to Spark Arrow and literal instants in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val local = Seq("0001-01-01T00:00:00", "1000-01-01T00:00:00.123456", "1800-01-01T00:00:00")
          .map(LocalDateTime.parse)
        val expected = local.map(_.atZone(java.time.ZoneId.of(zone)).toInstant)
        val input = local.map(java.sql.Timestamp.valueOf)
        val encoder = Encoder[java.sql.Timestamp]
        val result = roundTrip(encoder.schema, input.map(t => encoder.toRow(t).toSeq))
        assertEquals(result.toArray.map(_.getInstant(0)).toSeq, expected)
        input
          .zip(expected)
          .foreach: (timestamp, instant) =>
            val micros = Math.addExact(
              Math.multiplyExact(instant.getEpochSecond, 1000000L),
              instant.getNano / 1000L
            )
            assertEquals(
              Column.toLiteralProto(timestamp).literalType,
              proto.Expression.Literal.LiteralType.Timestamp(micros)
            )
      finally TimeZone.setDefault(originalZone)

  Seq("UTC", "America/Los_Angeles").foreach: zone =>
    test(s"decodes every SQL date cutover gap day from proleptic Arrow dates in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val input = (4 to 15).map(day => LocalDate.of(1582, 10, day))
        val wireEncoder = Encoder[LocalDate]
        val result = roundTrip(wireEncoder.schema, input.map(d => wireEncoder.toRow(d).toSeq))
        val expected = LocalDate.of(1582, 10, 4) +: Seq.fill(11)(LocalDate.of(1582, 10, 15))
        assertEquals(
          result.toArray.map(Encoder[java.sql.Date].fromRow).map(_.toLocalDate).toSeq,
          expected
        )
      finally TimeZone.setDefault(originalZone)

    test(s"decodes SQL timestamp cutover gaps with fixed calendar fields in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val local = LocalDateTime.parse("1582-10-10T12:34:56.123456")
        val wireEncoder = Encoder[Instant]
        val instant = local.atZone(java.time.ZoneId.of(zone)).toInstant
        val result = roundTrip(wireEncoder.schema, Seq(wireEncoder.toRow(instant).toSeq))
        assertEquals(
          Encoder[java.sql.Timestamp].fromRow(result.toArray.head).toLocalDateTime,
          LocalDateTime.parse("1582-10-15T12:34:56.123456")
        )
      finally TimeZone.setDefault(originalZone)

    test(s"encodes Julian-only SQL timestamp leap days as proleptic March 1 in $zone"):
      val originalZone = TimeZone.getDefault
      try
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        val input = java.sql.Timestamp.valueOf("1000-02-29 01:02:03.123456")
        val expected = LocalDateTime
          .parse("1000-03-01T01:02:03.123456")
          .atZone(java.time.ZoneId.of(zone))
          .toInstant
        val encoder = Encoder[java.sql.Timestamp]
        val result = roundTrip(encoder.schema, Seq(encoder.toRow(input).toSeq))
        assertEquals(result.toArray.head.getInstant(0), expected)
        assertEquals(
          Column.toLiteralProto(input).literalType,
          proto.Expression.Literal.LiteralType
            .Timestamp(expected.getEpochSecond * 1000000L + expected.getNano / 1000L)
        )
      finally TimeZone.setDefault(originalZone)


  test("java.sql.Date encoder still accepts an existing SQL date"):
    val encoder = Encoder[java.sql.Date]
    val input = java.sql.Date.valueOf("1969-12-31")
    assertEquals(encoder.fromRow(encoder.toRow(input)), input)

  test("java.sql.Timestamp encoder still accepts an existing SQL timestamp"):
    val encoder = Encoder[java.sql.Timestamp]
    val input = java.sql.Timestamp.from(Instant.parse("1969-12-31T23:59:59.999999Z"))
    assertEquals(encoder.fromRow(encoder.toRow(input)), input)

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

  test("round-trips a derived nested product through Arrow"):
    case class Detail(name: String, count: Int) derives Encoder
    case class Record(id: Long, detail: Detail) derives Encoder
    val encoder = Encoder[Record]
    val input = Record(1L, Detail("item", 2))
    val result = roundTrip(encoder.schema, Seq(encoder.toRow(input).toSeq))
    assertEquals(encoder.fromRow(result.toArray.head), input)

  test("round-trips an optional nested product through Arrow"):
    case class Detail(name: String, count: Int) derives Encoder
    case class Record(detail: Option[Detail]) derives Encoder
    val encoder = Encoder[Record]
    val input = Seq(Record(Some(Detail("item", 2))), Record(None))
    val result = roundTrip(encoder.schema, input.map(value => encoder.toRow(value).toSeq))
    assertEquals(result.toArray.map(encoder.fromRow).toSeq, input)

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
