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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

/**
 * Coverage for the [[Column]] operator surface. Each operator is built and must produce a defined
 * expression; a few also assert the exact `UnresolvedFunction` name. Pure plan building, no server.
 */
class ColumnOpsSuite extends munit.FunSuite {

  private val a: Column = col("a")
  private val b: Column = col("b")
  private def defined(c: Column): Boolean = c.expr.exprType.isDefined
  private def fname(c: Column): String = c.expr.getUnresolvedFunction.functionName

  test("arithmetic operators") {
    Seq(a + b, a - b, a * b, a / b, a % b, -a).foreach(c => assert(defined(c)))
    assertEquals(fname(a + b), "+")
    assertEquals(fname(a * b), "*")
  }

  test("comparison operators") {
    Seq(a === b, a =!= b, a.notEqual(b), a < b, a <= b, a > b, a >= b, a.eqNullSafe(b))
      .foreach(c => assert(defined(c)))
  }

  test("boolean operators") {
    Seq(a && b, a || b, a.and(b), a.or(b), !a).foreach(c => assert(defined(c)))
  }

  test("comparison/boolean operators map to the exact function names (no inversion)") {
    assertEquals(fname(a === b), "==")
    assertEquals(fname(a =!= b), "!=")
    assertEquals(fname(a.notEqual(b)), "!=")
    assertEquals(fname(a < b), "<")
    assertEquals(fname(a <= b), "<=")
    assertEquals(fname(a > b), ">")
    assertEquals(fname(a >= b), ">=")
    assertEquals(fname(a <=> b), "<=>")
    assertEquals(fname(a.eqNullSafe(b)), "<=>")
    assertEquals(fname(a && b), "and")
    assertEquals(fname(a || b), "or")
    assertEquals(fname(!a), "not")
    assertEquals(fname(-a), "negative")
  }

  test("arithmetic and bitwise operators map to the exact function names") {
    assertEquals(fname(a - b), "-")
    assertEquals(fname(a / b), "/")
    assertEquals(fname(a % b), "%")
    assertEquals(fname(a.bitwiseAND(b)), "&")
    assertEquals(fname(a.bitwiseOR(b)), "|")
    assertEquals(fname(a.bitwiseXOR(b)), "^")
  }

  test("string and null predicates map to the exact function names") {
    assertEquals(fname(a.like("x%")), "like")
    assertEquals(fname(a.rlike("x.*")), "rlike")
    assertEquals(fname(a.ilike("X%")), "ilike")
    assertEquals(fname(a.contains("x")), "contains")
    assertEquals(fname(a.startsWith("x")), "startswith")
    assertEquals(fname(a.endsWith("x")), "endswith")
    assertEquals(fname(a.isNull), "isNull")
    assertEquals(fname(a.isNotNull), "isNotNull")
    assertEquals(fname(a.isNaN), "isNaN")
    assertEquals(fname(a.isin(1, 2, 3)), "in")
  }

  test("predicates") {
    Seq(
      a.isNull,
      a.isNotNull,
      a.isNaN,
      a.isin(1, 2, 3),
      a.between(1, 10),
      a.like("x%"),
      a.rlike("x.*"),
      a.ilike("X%"),
      a.contains("x"),
      a.startsWith("x"),
      a.endsWith("x")
    ).foreach(c => assert(defined(c)))
  }

  test("projections, casts, and ordering") {
    Seq(
      a.cast("int"),
      a.substr(1, 3),
      a.getItem(0),
      a.getField("f"),
      a.alias("x"),
      a.as("y"),
      a.name("z"),
      a.asc,
      a.desc,
      a.asc_nulls_first,
      a.asc_nulls_last,
      a.desc_nulls_first,
      a.desc_nulls_last,
      a.bitwiseAND(b),
      a.bitwiseOR(b),
      a.bitwiseXOR(b)
    ).foreach(c => assert(defined(c)))
  }

  test("conditional and window") {
    val w = Window.partitionBy(a).orderBy(b)
    Seq(when(a > b, 1).otherwise(0), sum(a).over(w), row_number().over(w))
      .foreach(c => assert(defined(c)))
  }
}
