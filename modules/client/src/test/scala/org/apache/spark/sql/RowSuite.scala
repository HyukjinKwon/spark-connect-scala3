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

/** Hermetic coverage for [[Row]] / [[GenericRow]] / [[GenericRowWithSchema]] accessors. */
class RowSuite extends munit.FunSuite {

  test("apply / fromSeq / empty and basic shape") {
    val r = Row(1, "a", 2.0)
    assertEquals(r.length, 3)
    assertEquals(r.size, 3)
    assertEquals(r.get(1), "a")
    assertEquals(r(0), 1: Any)
    assertEquals(Row.fromSeq(Seq(1, 2)).toSeq, Seq[Any](1, 2))
    assertEquals(Row.empty.length, 0)
  }

  test("typed primitive accessors widen via getAnyValAs") {
    val r = Row(true, 1.toByte, 2.toShort, 3, 4L, 5.0f, 6.0d, "s")
    assertEquals(r.getBoolean(0), true)
    assertEquals(r.getByte(1), 1.toByte)
    assertEquals(r.getShort(2), 2.toShort)
    assertEquals(r.getInt(3), 3)
    assertEquals(r.getLong(4), 4L)
    assertEquals(r.getFloat(5), 5.0f)
    assertEquals(r.getDouble(6), 6.0d)
    assertEquals(r.getString(7), "s")
  }

  test("a primitive accessor on a null throws NPE; getString returns null") {
    val r = Row(null, null)
    assert(r.isNullAt(0))
    intercept[NullPointerException](r.getInt(0))
    assertEquals(r.getString(1), null)
  }

  test("complex-type accessors") {
    val r = Row(Seq(1, 2, 3), Map("k" -> 9), Row("nested"))
    assertEquals(r.getSeq[Int](0), Seq(1, 2, 3))
    assertEquals(r.getList[Int](0).size, 3)
    assertEquals(r.getMap[String, Int](1)("k"), 9)
    assertEquals(r.getStruct(2).getString(0), "nested")
  }

  test("anyNull, toSeq, and mkString variants") {
    assert(!Row(1, 2).anyNull)
    assert(Row(1, null).anyNull)
    assertEquals(Row(1, "a").mkString, "1a")
    assertEquals(Row(1, "a").mkString(","), "1,a")
    assertEquals(Row(1, "a").mkString("[", ";", "]"), "[1;a]")
    assertEquals(Row(1, "a").toString, "[1,a]")
  }

  test("equality and hashCode are by content") {
    assertEquals(Row(1, "a"), Row(1, "a"))
    assertEquals(Row(1, "a").hashCode(), Row(1, "a").hashCode())
    assertNotEquals(Row(1, "a"), Row(1, "b"))
    assertNotEquals(Row(1), Row(1, 2))
  }

  test("fieldIndex / getAs(name) / getValuesMap require a schema") {
    val schema = StructType(Array(StructField("a", IntegerType), StructField("b", StringType)))
    val r = new GenericRowWithSchema(Array(1, "x"), schema)
    assertEquals(r.fieldIndex("b"), 1)
    assertEquals(r.getAs[String]("b"), "x")
    assertEquals(r.getValuesMap[Any](Seq("a", "b")), Map("a" -> 1, "b" -> "x"))
    assertEquals(r.schema, schema)

    intercept[UnsupportedOperationException](Row(1).fieldIndex("a"))
  }

  test("unapplySeq enables pattern matching") {
    val matched = Row(1, "a") match {
      case Row(x, y) => s"$x-$y"
      case _ => "no"
    }
    assertEquals(matched, "1-a")
  }
}
