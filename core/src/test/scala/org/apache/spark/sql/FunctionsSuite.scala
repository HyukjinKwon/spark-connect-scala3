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

import java.time.LocalDate

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Unit tests for the [[functions]] object, literals, higher-order functions and window
 * specifications.
 */
class FunctionsSuite extends munit.FunSuite {

  private def fnName(c: Column): String = c.expr.getUnresolvedFunction.functionName

  test("literals encode primitive types") {
    assertEquals(lit(true).expr.getLiteral.getBoolean, true)
    assertEquals(lit(42).expr.getLiteral.getInteger, 42)
    assertEquals(lit(42L).expr.getLiteral.getLong, 42L)
    assertEquals(lit(3.14).expr.getLiteral.getDouble, 3.14)
    assertEquals(lit("hi").expr.getLiteral.getString, "hi")
    assert(lit(null).expr.getLiteral.literalType.isNull)
  }

  test("lit passes Columns through unchanged") {
    val c = col("a")
    assertEquals(lit(c).expr, c.expr)
  }

  test("lit encodes a LocalDate as days since epoch") {
    assertEquals(lit(LocalDate.of(1970, 1, 11)).expr.getLiteral.getDate, 10)
  }

  test("lit encodes an array literal with element type") {
    val arr = lit(Array(1, 2, 3)).expr.getLiteral.getArray
    assertEquals(arr.elements.size, 3)
    assert(arr.getElementType.kind.isInteger)
  }

  test("expr produces an expression string") {
    assertEquals(expr("a + 1").expr.getExpressionString.expression, "a + 1")
  }

  test("aggregate function names") {
    assertEquals(fnName(sum(col("a"))), "sum")
    assertEquals(fnName(avg(col("a"))), "avg")
    assertEquals(fnName(mean(col("a"))), "avg")
    assertEquals(fnName(max(col("a"))), "max")
    assertEquals(fnName(min(col("a"))), "min")
    assertEquals(fnName(count(col("a"))), "count")
    assertEquals(fnName(stddev(col("a"))), "stddev")
  }

  test("countDistinct / sum_distinct set the distinct flag") {
    assert(countDistinct(col("a")).expr.getUnresolvedFunction.isDistinct)
    assert(sum_distinct(col("a")).expr.getUnresolvedFunction.isDistinct)
    assertEquals(countDistinct(col("a")).expr.getUnresolvedFunction.functionName, "count")
  }

  test("math / string / datetime function names") {
    assertEquals(fnName(sqrt(col("a"))), "sqrt")
    assertEquals(fnName(pow(col("a"), 2.0)), "power")
    assertEquals(fnName(round(col("a"), 2)), "round")
    assertEquals(fnName(upper(col("a"))), "upper")
    assertEquals(fnName(concat_ws("-", col("a"), col("b"))), "concat_ws")
    assertEquals(fnName(regexp_replace(col("a"), "x", "y")), "regexp_replace")
    assertEquals(fnName(to_date(col("a"))), "to_date")
    assertEquals(fnName(date_add(col("a"), 5)), "date_add")
    assertEquals(fnName(year(col("a"))), "year")
  }

  test("collection function names") {
    assertEquals(fnName(array(col("a"), col("b"))), "array")
    assertEquals(fnName(array_contains(col("a"), 1)), "array_contains")
    assertEquals(fnName(explode(col("a"))), "explode")
    assertEquals(fnName(size(col("a"))), "size")
    assertEquals(fnName(map_keys(col("a"))), "map_keys")
  }

  test("when / otherwise build a chained 'when' call") {
    val c = when(col("a") > 1, "big").when(col("a") > 0, "small").otherwise("none")
    val f = c.expr.getUnresolvedFunction
    assertEquals(f.functionName, "when")
    // 2 (cond,val) + 2 (cond,val) + 1 (otherwise) = 5 args
    assertEquals(f.arguments.size, 5)
  }

  test("when before otherwise has an even number of arguments") {
    val f = when(col("a") > 1, "big").expr.getUnresolvedFunction
    assertEquals(f.arguments.size, 2)
  }

  test("higher-order transform builds a lambda function") {
    val c = transform(col("arr"), x => x + 1)
    val f = c.expr.getUnresolvedFunction
    assertEquals(f.functionName, "transform")
    assertEquals(f.arguments.size, 2)
    val lambda = f.arguments(1).getLambdaFunction
    assertEquals(lambda.arguments.size, 1)
    assertEquals(lambda.arguments.head.nameParts.toList, List("x"))
    // body: x + 1
    assertEquals(lambda.getFunction.getUnresolvedFunction.functionName, "+")
  }

  test("two-argument lambda (zip_with)") {
    val c = zip_with(col("a"), col("b"), (l, r) => l + r)
    val lambda = c.expr.getUnresolvedFunction.arguments(2).getLambdaFunction
    assertEquals(lambda.arguments.size, 2)
    assertEquals(lambda.arguments.map(_.nameParts.head).toList, List("x", "y"))
  }

  test("call_function builds a CallFunction expression") {
    val c = call_function("my_udf", col("a"), col("b"))
    assertEquals(c.expr.getCallFunction.functionName, "my_udf")
    assertEquals(c.expr.getCallFunction.arguments.size, 2)
  }

  test("window functions over a WindowSpec") {
    val spec = Window.partitionBy("dept").orderBy(col("salary").desc)
    val c = rank().over(spec)
    val w = c.expr.getWindow
    assertEquals(w.getWindowFunction.getUnresolvedFunction.functionName, "rank")
    assertEquals(w.partitionSpec.size, 1)
    assertEquals(w.orderSpec.size, 1)
    assertEquals(
      w.orderSpec.head.direction,
      org.apache.spark.connect.proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
    )
  }

  test("rowsBetween sets a row frame with bounded/unbounded boundaries") {
    val spec = Window.partitionBy("a").orderBy("b").rowsBetween(Window.unboundedPreceding, 0)
    val frame = rank().over(spec).expr.getWindow.getFrameSpec
    assert(frame.getLower.getUnbounded)
    assert(frame.getUpper.getCurrentRow)
  }
}
