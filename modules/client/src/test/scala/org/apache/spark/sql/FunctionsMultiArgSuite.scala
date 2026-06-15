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

import org.apache.spark.connect.proto
import org.apache.spark.sql.functions._

/**
 * Coverage for multi-argument [[functions]] entry points, asserting both the underlying function
 * name (which often differs from the Scala method name, e.g. `pow` -> `power`) and the argument
 * arity / ordering. Pure plan building, no server. Guards against copy-paste name/argument bugs.
 */
class FunctionsMultiArgSuite extends munit.FunSuite {

  private val a: Column = col("a")
  private val b: Column = col("b")
  private def fname(c: Column): String = c.expr.getUnresolvedFunction.functionName
  private def args(c: Column): Seq[proto.Expression] = c.expr.getUnresolvedFunction.arguments.toSeq
  private def litLong(e: proto.Expression): Long = e.getLiteral.getLong
  private def litInt(e: proto.Expression): Int = e.getLiteral.getInteger
  private def litStr(e: proto.Expression): String = e.getLiteral.getString

  test("pow maps to the `power` function regardless of overload") {
    assertEquals(fname(pow(a, b)), "power")
    assertEquals(fname(pow(a, 2.0)), "power")
    assertEquals(fname(pow(2.0, b)), "power")
    assertEquals(fname(pow("a", "b")), "power")
  }

  test("variadic functions keep their name and arity") {
    assertEquals(fname(coalesce(a, b)), "coalesce")
    assertEquals(args(coalesce(a, b)).size, 2)
    assertEquals(fname(greatest(a, b)), "greatest")
    assertEquals(fname(least(a, b)), "least")
    assertEquals(fname(concat(a, b)), "concat")
    assertEquals(fname(nanvl(a, b)), "nanvl")
    assertEquals(fname(atan2(a, b)), "atan2")
    assertEquals(fname(hypot(a, b)), "hypot")
  }

  test("round / bround append the scale as a literal") {
    assertEquals(fname(round(a, 2)), "round")
    assertEquals(litInt(args(round(a, 2))(1)), 2)
    assertEquals(fname(bround(a, 3)), "bround")
    assertEquals(litInt(args(bround(a, 3))(1)), 3)
  }

  test("trim with an explicit trim string prepends the literal (Spark Connect order)") {
    val c = trim(a, "xy")
    assertEquals(fname(c), "trim")
    assertEquals(litStr(args(c).head), "xy")
    assertEquals(args(c).size, 2)
  }

  test("concat_ws and format_string lead with the separator/format literal") {
    val cw = concat_ws("-", a, b)
    assertEquals(fname(cw), "concat_ws")
    assertEquals(litStr(args(cw).head), "-")
    assertEquals(args(cw).size, 3)
    val fs = format_string("%s=%s", a, b)
    assertEquals(fname(fs), "format_string")
    assertEquals(litStr(args(fs).head), "%s=%s")
  }

  test("date arithmetic uses the right names and argument order") {
    assertEquals(fname(date_add(a, 5)), "date_add")
    assertEquals(litInt(args(date_add(a, 5))(1)), 5)
    assertEquals(fname(date_sub(a, 5)), "date_sub")
    // datediff(end, start) and months_between(end, start): the receiver `a` is the end date.
    assertEquals(fname(datediff(a, b)), "datediff")
    assertEquals(fname(months_between(a, b)), "months_between")
  }

  test("locate places the search literal first, then the column") {
    val c = locate("x", a)
    assertEquals(fname(c), "locate")
    assertEquals(litStr(args(c).head), "x")
    val withPos = locate("x", a, 3)
    assertEquals(litInt(args(withPos).last), 3)
  }

  test("string regex / split functions keep their names") {
    assertEquals(fname(regexp_replace(a, "p", "r")), "regexp_replace")
    assertEquals(fname(regexp_extract(a, "p", 1)), "regexp_extract")
    assertEquals(fname(split(a, ",")), "split")
    assertEquals(fname(substring(a, 1, 3)), "substring")
    assertEquals(fname(translate(a, "ab", "cd")), "translate")
  }

  test("when builds a `when` expression") {
    assertEquals(fname(when(a > b, 1)), "when")
    assertEquals(fname(when(a > b, 1).otherwise(0)), "when")
  }
}
