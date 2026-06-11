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

import org.apache.spark.sql.types.*

class DataTypeProtoConverterSuite extends munit.FunSuite:

  private def roundTrip(dt: DataType): Unit =
    val proto = DataTypeProtoConverter.toConnectProtoType(dt)
    val back = DataTypeProtoConverter.toCatalystType(proto)
    assertEquals(back, dt, s"round-trip failed for $dt")

  test("atomic types round-trip") {
    Seq(
      NullType,
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      TimestampNTZType,
      CalendarIntervalType,
      VariantType
    ).foreach(roundTrip)
  }

  test("parameterised types round-trip") {
    roundTrip(DecimalType(20, 4))
    roundTrip(DecimalType(38, 18))
    roundTrip(CharType(12))
    roundTrip(VarcharType(255))
    roundTrip(YearMonthIntervalType(YearMonthIntervalType.YEAR, YearMonthIntervalType.MONTH))
    roundTrip(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))
  }

  test("complex types round-trip") {
    roundTrip(ArrayType(IntegerType, containsNull = false))
    roundTrip(MapType(StringType, LongType, valueContainsNull = true))
    roundTrip(
      StructType(
        Array(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType),
          StructField("scores", ArrayType(DoubleType)),
          StructField("props", MapType(StringType, StringType))
        )
      )
    )
  }

  test("nested complex types round-trip") {
    roundTrip(
      ArrayType(
        StructType(Array(StructField("a", IntegerType), StructField("b", ArrayType(StringType))))
      )
    )
    roundTrip(MapType(StringType, MapType(IntegerType, BooleanType)))
  }

  test("struct field metadata is preserved") {
    val withMeta = StructType(
      Array(StructField("c", IntegerType, nullable = true, Metadata("""{"comment":"hi"}""")))
    )
    val proto = DataTypeProtoConverter.toConnectProtoType(withMeta)
    val back = DataTypeProtoConverter.toStructType(proto)
    assertEquals(back("c").metadata.json, """{"comment":"hi"}""")
  }

  test("toStructType wraps non-struct in a single column") {
    val proto = DataTypeProtoConverter.toConnectProtoType(IntegerType)
    assertEquals(DataTypeProtoConverter.toStructType(proto).length, 1)
  }
