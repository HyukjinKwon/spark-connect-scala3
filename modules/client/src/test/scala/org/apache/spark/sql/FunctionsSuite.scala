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

import org.apache.spark.sql.functions.*

/**
 * Breadth coverage for the [[functions]] object: asserts that a representative function from each
 * category builds an `UnresolvedFunction` carrying the correct SQL function name. Pure plan
 * construction, so no server is required.
 */
class FunctionsSuite extends munit.FunSuite {

  private val a = col("a")
  private val b = col("b")

  private def name(c: Column): String = c.expr.getUnresolvedFunction.functionName

  test("math function names") {
    assertEquals(name(abs(a)), "abs")
    assertEquals(name(sqrt(a)), "sqrt")
    assertEquals(name(cbrt(a)), "cbrt")
    assertEquals(name(exp(a)), "exp")
    assertEquals(name(ceil(a)), "ceil")
    assertEquals(name(floor(a)), "floor")
    assertEquals(name(sin(a)), "sin")
    assertEquals(name(cos(a)), "cos")
    assertEquals(name(signum(a)), "signum")
    assertEquals(name(round(a)), "round")
  }

  test("aggregate function names") {
    assertEquals(name(sum(a)), "sum")
    assertEquals(name(avg(a)), "avg")
    assertEquals(name(max(a)), "max")
    assertEquals(name(min(a)), "min")
    assertEquals(name(count(a)), "count")
    assertEquals(name(stddev(a)), "stddev")
    assertEquals(name(variance(a)), "variance")
    assertEquals(name(collect_list(a)), "collect_list")
    assertEquals(name(collect_set(a)), "collect_set")
    assertEquals(name(corr(a, b)), "corr")
  }

  test("string function names") {
    assertEquals(name(upper(a)), "upper")
    assertEquals(name(lower(a)), "lower")
    assertEquals(name(length(a)), "length")
    assertEquals(name(trim(a)), "trim")
    assertEquals(name(ltrim(a)), "ltrim")
    assertEquals(name(rtrim(a)), "rtrim")
    assertEquals(name(ascii(a)), "ascii")
    assertEquals(name(reverse(a)), "reverse")
    assertEquals(name(initcap(a)), "initcap")
    assertEquals(name(concat(a, b)), "concat")
  }

  test("datetime function names") {
    assertEquals(name(year(a)), "year")
    assertEquals(name(month(a)), "month")
    assertEquals(name(dayofmonth(a)), "dayofmonth")
    assertEquals(name(hour(a)), "hour")
    assertEquals(name(minute(a)), "minute")
    assertEquals(name(second(a)), "second")
    assertEquals(name(quarter(a)), "quarter")
    assertEquals(name(to_date(a)), "to_date")
  }

  test("collection and misc function names") {
    assertEquals(name(size(a)), "size")
    assertEquals(name(explode(a)), "explode")
    assertEquals(name(flatten(a)), "flatten")
    assertEquals(name(array_distinct(a)), "array_distinct")
    assertEquals(name(array_max(a)), "array_max")
    assertEquals(name(map_keys(a)), "map_keys")
    assertEquals(name(map_values(a)), "map_values")
    assertEquals(name(array(a, b)), "array")
    assertEquals(name(coalesce(a, b)), "coalesce")
    assertEquals(name(isnull(a)), "isnull")
  }
}
