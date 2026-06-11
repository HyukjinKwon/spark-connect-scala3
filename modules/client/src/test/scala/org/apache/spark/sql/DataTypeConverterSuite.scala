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

import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types._

/**
 * Unit tests that round-trip a client-side [[DataType]] through the protobuf representation:
 * `toConnectProtoType` then `toCatalystType` should yield an equal type. Runs offline.
 */
class DataTypeConverterSuite extends munit.FunSuite {

  private def roundtrip(dt: DataType): DataType =
    DataTypeProtoConverter.toCatalystType(DataTypeProtoConverter.toConnectProtoType(dt))

  private def assertRoundtrip(dt: DataType): Unit =
    assertEquals(roundtrip(dt), dt)

  test("IntegerType round-trips") {
    assertRoundtrip(IntegerType)
  }

  test("StringType round-trips") {
    assertRoundtrip(StringType)
  }

  test("LongType round-trips") {
    assertRoundtrip(LongType)
  }

  test("DoubleType round-trips") {
    assertRoundtrip(DoubleType)
  }

  test("BooleanType round-trips") {
    assertRoundtrip(BooleanType)
  }

  test("ArrayType round-trips") {
    assertRoundtrip(ArrayType(IntegerType, containsNull = true))
  }

  test("MapType round-trips") {
    assertRoundtrip(MapType(StringType, IntegerType, valueContainsNull = true))
  }

  test("DecimalType round-trips") {
    assertRoundtrip(DecimalType(18, 4))
  }

  test("StructType round-trips") {
    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("tags", ArrayType(StringType, containsNull = true), nullable = true)))
    assertRoundtrip(schema)
  }

  test("nested StructType inside an array round-trips") {
    val inner = StructType(Array(StructField("a", IntegerType), StructField("b", StringType)))
    assertRoundtrip(ArrayType(inner, containsNull = false))
  }
}
