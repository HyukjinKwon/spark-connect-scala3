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

import org.apache.spark.sql.{GenericRowWithSchema, Row}
import org.apache.spark.sql.types.*

class ShowStringSuite extends munit.FunSuite:

  private val schema = StructType(
    Array(StructField("id", LongType), StructField("name", StringType))
  )
  private def row(values: Any*): Row = new GenericRowWithSchema(values.toArray, schema)

  test("horizontal table layout") {
    val rows = Seq(row(1L, "alice"), row(2L, "bob"))
    val out = ShowString(rows, schema, numRows = 20, truncate = 20, vertical = false)
    val expected =
      """+---+-----+
        || id| name|
        |+---+-----+
        ||  1|alice|
        ||  2|  bob|
        |+---+-----+
        |""".stripMargin
    assertEquals(out, expected)
  }

  test("null rendering") {
    val out = ShowString(Seq(row(1L, null)), schema, 20, 20, false)
    assert(out.contains("null"))
  }

  test("truncation with ellipsis") {
    val cell = ShowString.formatCell("abcdefghijklmnopqrstuvwxyz", truncate = 10)
    assertEquals(cell, "abcdefg...")
    assertEquals(cell.length, 10)
  }

  test("no truncation when truncate = 0") {
    val long = "abcdefghijklmnopqrstuvwxyz"
    assertEquals(ShowString.formatCell(long, truncate = 0), long)
  }

  test("only showing top N footer when more data present") {
    val rows = (1 to 5).map(i => row(i.toLong, s"n$i"))
    val out = ShowString(rows, schema, numRows = 3, truncate = 20, vertical = false)
    assert(out.contains("only showing top 3 rows"))
  }

  test("vertical layout") {
    val out = ShowString(Seq(row(1L, "alice")), schema, 20, 20, vertical = true)
    assert(out.contains("-RECORD 0"))
    assert(out.contains("id"))
    assert(out.contains("alice"))
  }

  test("binary cell rendering as hex") {
    assertEquals(ShowString.formatCell(Array[Byte](1, 2, 15), truncate = 0), "[01 02 0F]")
  }

  test("array and map cell rendering") {
    assertEquals(ShowString.formatCell(Seq(1, 2, 3), truncate = 0), "[1, 2, 3]")
    assertEquals(ShowString.formatCell(Map("a" -> 1), truncate = 0), "{a -> 1}")
  }
