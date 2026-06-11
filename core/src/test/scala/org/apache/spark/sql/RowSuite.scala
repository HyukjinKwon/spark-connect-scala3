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

import org.apache.spark.sql.types.*

class RowSuite extends munit.FunSuite:

  test("positional accessors") {
    val row = Row(1, "abc", 3.0, true, null)
    assertEquals(row.length, 5)
    assertEquals(row.getInt(0), 1)
    assertEquals(row.getString(1), "abc")
    assertEquals(row.getDouble(2), 3.0)
    assertEquals(row.getBoolean(3), true)
    assert(row.isNullAt(4))
    assert(row.anyNull)
  }

  test("getAnyValAs throws on null primitive") {
    val row = Row(null)
    intercept[NullPointerException](row.getInt(0))
  }

  test("schema-based access via GenericRowWithSchema") {
    val schema = StructType(Array(StructField("id", LongType), StructField("name", StringType)))
    val row: Row = new GenericRowWithSchema(Array(10L, "spark"), schema)
    assertEquals(row.fieldIndex("name"), 1)
    assertEquals(row.getAs[String]("name"), "spark")
    assertEquals(row.getAs[Long]("id"), 10L)
    assertEquals(row.getValuesMap[Any](Seq("id", "name")), Map("id" -> 10L, "name" -> "spark"))
  }

  test("fieldIndex without schema fails") {
    intercept[UnsupportedOperationException](Row(1).fieldIndex("x"))
  }

  test("toSeq, mkString and equality") {
    val a = Row(1, 2, 3)
    val b = Row.fromSeq(Seq(1, 2, 3))
    assertEquals(a.toSeq, Seq(1, 2, 3))
    assertEquals(a.mkString("[", ",", "]"), "[1,2,3]")
    assertEquals(a, b)
    assertEquals(a.hashCode(), b.hashCode())
  }

  test("nested seq/map/struct values") {
    val row = Row(Seq(1, 2, 3), Map("k" -> "v"))
    assertEquals(row.getSeq[Int](0), Seq(1, 2, 3))
    assertEquals(row.getMap[String, String](1), Map("k" -> "v"))
  }

  test("Row.merge concatenates") {
    assertEquals(Row.merge(Row(1, 2), Row(3)), Row(1, 2, 3))
  }
