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

package org.apache.spark.sql.types

class DataTypeSuite extends munit.FunSuite:

  test("simpleString and sql for atomic types") {
    assertEquals(IntegerType.simpleString, "int")
    assertEquals(IntegerType.sql, "INT")
    assertEquals(LongType.simpleString, "bigint")
    assertEquals(StringType.simpleString, "string")
    assertEquals(DoubleType.sql, "DOUBLE")
    assertEquals(ByteType.simpleString, "tinyint")
    assertEquals(BooleanType.typeName, "boolean")
    assertEquals(TimestampNTZType.simpleString, "timestamp_ntz")
  }

  test("decimal type rendering") {
    assertEquals(DecimalType(10, 2).simpleString, "decimal(10,2)")
    assertEquals(DecimalType(10, 2).sql, "DECIMAL(10,2)")
    assertEquals(DecimalType.SYSTEM_DEFAULT, DecimalType(38, 18))
  }

  test("array/map/struct rendering") {
    assertEquals(ArrayType(IntegerType).simpleString, "array<int>")
    assertEquals(MapType(StringType, LongType).simpleString, "map<string,bigint>")
    val st = StructType(Array(StructField("a", IntegerType), StructField("b", StringType)))
    assertEquals(st.simpleString, "struct<a:int,b:string>")
    assertEquals(st.sql, "STRUCT<a: INT, b: STRING>")
  }

  test("StructType operations") {
    val st = StructType.empty
      .add("id", LongType, nullable = false)
      .add("name", StringType)
      .add(StructField("score", DoubleType))
    assertEquals(st.length, 3)
    assertEquals(st.fieldNames.toSeq, Seq("id", "name", "score"))
    assertEquals(st.fieldIndex("name"), 1)
    assertEquals(st("score").dataType, DoubleType)
    assert(!st("id").nullable)
    intercept[IllegalArgumentException](st("missing"))
  }

  test("StructType equality is by content") {
    val a = StructType(Array(StructField("x", IntegerType)))
    val b = StructType(Array(StructField("x", IntegerType)))
    assertEquals(a, b)
    assertEquals(a.hashCode(), b.hashCode())
  }

  test("DDL parsing: column list") {
    val st = StructType.fromDDL("a INT, b STRING, c ARRAY<INT>")
    assertEquals(st.length, 3)
    assertEquals(st("a").dataType, IntegerType)
    assertEquals(st("b").dataType, StringType)
    assertEquals(st("c").dataType, ArrayType(IntegerType))
  }

  test("DDL parsing: nested struct and map") {
    val dt = DataType.fromDDL("struct<a:int, m:map<string,double>>")
    val st = dt.asInstanceOf[StructType]
    assertEquals(st("a").dataType, IntegerType)
    assertEquals(st("m").dataType, MapType(StringType, DoubleType))
  }

  test("DDL parsing: decimal/char/varchar") {
    assertEquals(DataType.fromDDL("decimal(12,3)"), DecimalType(12, 3))
    assertEquals(DataType.fromDDL("varchar(255)"), VarcharType(255))
    assertEquals(DataType.fromDDL("char(8)"), CharType(8))
  }

  test("type aliases") {
    assertEquals(DataType.fromDDL("integer"), IntegerType)
    assertEquals(DataType.fromDDL("bigint"), LongType)
    assertEquals(DataType.fromDDL("bool"), BooleanType)
  }

  test("JSON serialization") {
    val st = StructType(Array(StructField("a", IntegerType, nullable = false)))
    assert(st.json.contains("\"type\":\"struct\""))
    assert(st.json.contains("\"name\":\"a\""))
    assert(ArrayType(StringType).json.contains("\"type\":\"array\""))
  }
