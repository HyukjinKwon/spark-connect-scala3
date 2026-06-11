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

import org.apache.spark.sql.functions.lit

/**
 * Unit tests asserting that [[functions.lit]] encodes Scala values into the correct
 * `proto.Expression.Literal` variant. Runs offline.
 */
class LiteralSuite extends munit.FunSuite {

  private def literalOf(value: Any) = {
    val c = lit(value)
    assert(c.expr.exprType.isLiteral, s"expected a Literal for $value")
    c.expr.getLiteral
  }

  test("boolean literal") {
    val l = literalOf(true)
    assert(l.literalType.isBoolean)
    assertEquals(l.getBoolean, true)
  }

  test("int literal") {
    val l = literalOf(42)
    assert(l.literalType.isInteger)
    assertEquals(l.getInteger, 42)
  }

  test("long literal") {
    val l = literalOf(42L)
    assert(l.literalType.isLong)
    assertEquals(l.getLong, 42L)
  }

  test("double literal") {
    val l = literalOf(3.14d)
    assert(l.literalType.isDouble)
    assertEquals(l.getDouble, 3.14d)
  }

  test("string literal") {
    val l = literalOf("hello")
    assert(l.literalType.isString)
    assertEquals(l.getString, "hello")
  }

  test("null literal") {
    val l = literalOf(null)
    assert(l.literalType.isNull)
  }
}
