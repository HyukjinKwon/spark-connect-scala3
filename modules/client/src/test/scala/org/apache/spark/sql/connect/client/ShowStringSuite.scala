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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

class ShowStringSuite extends munit.FunSuite {

  private val schema =
    StructType(Array(StructField("id", LongType), StructField("name", StringType)))

  test("horizontal table with min column width 3") {
    val out = ShowString(Seq(Row(1L, "alice"), Row(2L, "bob")), schema, 20, 20, vertical = false)
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

  test("empty result still renders header") {
    val out = ShowString(Seq.empty, schema, 20, 20, vertical = false)
    val expected =
      """+---+----+
        || id|name|
        |+---+----+
        |+---+----+
        |""".stripMargin
    assertEquals(out, expected)
  }

  test("only showing top N footer when there is more data") {
    val rows = (1 to 5).map(i => Row(i.toLong, s"n$i"))
    val out = ShowString(rows, schema, numRows = 3, truncate = 20, vertical = false)
    assert(out.contains("only showing top 3 rows"), out)
    assert(out.contains("n3"), out)
    assert(!out.contains("n4"), out)
  }

  test("truncation adds an ellipsis at the requested width") {
    assertEquals(ShowString.formatCell("abcdefghijklmnopqrstuvwxyz", 10), "abcdefg...")
    assertEquals(ShowString.formatCell("short", 10), "short")
    assertEquals(ShowString.formatCell("nolimit-aaaaaaaaaaaaaaaaaa", 0), "nolimit-aaaaaaaaaaaaaaaaaa")
  }

  test("null and collection cells") {
    assertEquals(ShowString.formatCell(null, 20), "null")
    assertEquals(ShowString.formatCell(Seq(1, 2, 3), 20), "[1, 2, 3]")
    assertEquals(ShowString.formatCell(Array[Byte](1, 2, 15), 0), "[01 02 0F]")
  }

  test("vertical layout") {
    val out = ShowString(Seq(Row(1L, "alice")), schema, 20, 20, vertical = true)
    assert(out.contains("-RECORD 0"), out)
    assert(out.contains("name | alice"), out)
  }
}
