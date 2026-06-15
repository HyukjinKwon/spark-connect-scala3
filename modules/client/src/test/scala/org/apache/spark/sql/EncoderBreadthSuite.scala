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

package org.apache.spark.sql

import org.apache.spark.sql.types._

/**
 * Breadth coverage for the built-in [[Encoder]] instances not exercised by
 * [[EncoderDerivationSuite]]: the remaining numeric primitives, the reference-typed atomics
 * (date/time/decimal/binary), tuples, `List`, and the nullability that flows into collection
 * element types. Pure, no server.
 */
class EncoderBreadthSuite extends munit.FunSuite {

  test("numeric primitive encoders map to the right type and are non-nullable") {
    assertEquals(summon[Encoder[Short]].dataType, ShortType: DataType)
    assertEquals(summon[Encoder[Byte]].dataType, ByteType: DataType)
    assertEquals(summon[Encoder[Float]].dataType, FloatType: DataType)
    assertEquals(summon[Encoder[Int]].nullable, false)
    assertEquals(summon[Encoder[Double]].nullable, false)
  }

  test("reference-typed atomics are nullable and map to the right type") {
    assertEquals(summon[Encoder[String]].nullable, true)
    assertEquals(summon[Encoder[Array[Byte]]].dataType, BinaryType: DataType)
    assertEquals(summon[Encoder[Array[Byte]]].nullable, true)
    assertEquals(summon[Encoder[BigDecimal]].dataType, DecimalType(38, 18): DataType)
    assertEquals(summon[Encoder[java.sql.Date]].dataType, DateType: DataType)
    assertEquals(summon[Encoder[java.time.LocalDate]].dataType, DateType: DataType)
    assertEquals(summon[Encoder[java.sql.Timestamp]].dataType, TimestampType: DataType)
    assertEquals(summon[Encoder[java.time.Instant]].dataType, TimestampType: DataType)
    assertEquals(summon[Encoder[java.time.LocalDateTime]].dataType, TimestampNTZType: DataType)
  }

  test("an atomic encoder's schema wraps the type in a single `value` field") {
    assertEquals(
      summon[Encoder[Int]].schema,
      StructType(Array(StructField("value", IntegerType, nullable = false)))
    )
  }

  test("an atomic value round-trips through toRow/fromRow") {
    val e = summon[Encoder[Long]]
    assertEquals(e.fromRow(e.toRow(99L)), 99L)
  }

  test("Seq and List element nullability follows the element encoder") {
    // Int is non-nullable -> containsNull = false; String is nullable -> containsNull = true.
    assertEquals(summon[Encoder[Seq[Int]]].dataType, ArrayType(IntegerType, containsNull = false))
    assertEquals(summon[Encoder[List[String]]].dataType, ArrayType(StringType, containsNull = true))
  }

  test("Map value nullability follows the value encoder") {
    assertEquals(
      summon[Encoder[Map[String, Int]]].dataType,
      MapType(StringType, IntegerType, valueContainsNull = false)
    )
  }

  test("Option encoder is nullable and marshals None to null") {
    val e = summon[Encoder[Option[Int]]]
    assertEquals(e.dataType, IntegerType: DataType)
    assert(e.nullable)
    assertEquals(e.toValue(Some(5)), 5)
    assertEquals(e.toValue(None), null)
    assertEquals(e.fromValue(7), Some(7))
    assertEquals(e.fromValue(null), None)
  }

  test("tuple encoder derives positional field names and round-trips") {
    val e = summon[Encoder[(Int, String)]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("_1", IntegerType, nullable = false),
          StructField("_2", StringType, nullable = true)
        )
      )
    )
    assertEquals(e.fromRow(e.toRow((1, "a"))), (1, "a"))
  }

  test("Seq and Map values round-trip") {
    val se = summon[Encoder[Seq[Int]]]
    assertEquals(se.fromValue(se.toValue(Seq(1, 2, 3))), Seq(1, 2, 3))
    val me = summon[Encoder[Map[String, Int]]]
    assertEquals(me.fromValue(me.toValue(Map("a" -> 1))), Map("a" -> 1))
  }
}
