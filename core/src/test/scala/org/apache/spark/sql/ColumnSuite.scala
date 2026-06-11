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
import org.apache.spark.connect.proto.Expression.SortOrder.{NullOrdering, SortDirection}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Unit tests for [[Column]] expression building. These assert the structure of the generated
 * `proto.Expression` and need no Spark Connect server.
 */
class ColumnSuite extends munit.FunSuite {

  test("col produces an unresolved attribute") {
    assertEquals(col("a").expr.getUnresolvedAttribute.unparsedIdentifier, "a")
  }

  test("star and qualified star") {
    assert(col("*").expr.exprType.isUnresolvedStar)
    assertEquals(col("t.*").expr.getUnresolvedStar.getUnparsedTarget, "t.*")
  }

  test("arithmetic operators map to SQL function names") {
    assertEquals((col("a") + 1).expr.getUnresolvedFunction.functionName, "+")
    assertEquals((col("a") - 1).expr.getUnresolvedFunction.functionName, "-")
    assertEquals((col("a") * 2).expr.getUnresolvedFunction.functionName, "*")
    assertEquals((col("a") / 2).expr.getUnresolvedFunction.functionName, "/")
    assertEquals((col("a") % 2).expr.getUnresolvedFunction.functionName, "%")
    val negated = -col("a")
    assertEquals(negated.expr.getUnresolvedFunction.functionName, "negative")
  }

  test("comparison operators") {
    assertEquals((col("a") === 1).expr.getUnresolvedFunction.functionName, "=")
    assertEquals((col("a") =!= 1).expr.getUnresolvedFunction.functionName, "!")
    assertEquals((col("a") > 1).expr.getUnresolvedFunction.functionName, ">")
    assertEquals((col("a") <= 1).expr.getUnresolvedFunction.functionName, "<=")
    assertEquals((col("a") <=> 1).expr.getUnresolvedFunction.functionName, "<=>")
  }

  test("boolean operators use named functions") {
    assertEquals((col("a") && col("b")).expr.getUnresolvedFunction.functionName, "and")
    assertEquals((col("a") || col("b")).expr.getUnresolvedFunction.functionName, "or")
    val notA = !col("a")
    assertEquals(notA.expr.getUnresolvedFunction.functionName, "!")
  }

  test("arguments are wired through") {
    val e = (col("a") + lit(1)).expr.getUnresolvedFunction
    assertEquals(e.arguments.size, 2)
    assertEquals(e.arguments.head.getUnresolvedAttribute.unparsedIdentifier, "a")
    assertEquals(e.arguments(1).getLiteral.getInteger, 1)
  }

  test("alias wraps in Alias with a single name") {
    val a = col("a").as("b").expr.getAlias
    assertEquals(a.name.toList, List("b"))
    assertEquals(a.getExpr.getUnresolvedAttribute.unparsedIdentifier, "a")
  }

  test("cast to DDL string uses type_str") {
    assertEquals(col("a").cast("int").expr.getCast.getTypeStr, "int")
  }

  test("cast to DataType uses proto type") {
    val c = col("a").cast(IntegerType).expr.getCast
    assert(c.getType.kind.isInteger)
  }

  test("try_cast sets TRY eval mode") {
    assertEquals(
      col("a").try_cast("int").expr.getCast.evalMode,
      proto.Expression.Cast.EvalMode.EVAL_MODE_TRY
    )
  }

  test("sort ordering directions and null ordering") {
    val asc = col("a").asc.expr.getSortOrder
    assertEquals(asc.direction, SortDirection.SORT_DIRECTION_ASCENDING)
    assertEquals(asc.nullOrdering, NullOrdering.SORT_NULLS_FIRST)
    val desc = col("a").desc.expr.getSortOrder
    assertEquals(desc.direction, SortDirection.SORT_DIRECTION_DESCENDING)
    assertEquals(desc.nullOrdering, NullOrdering.SORT_NULLS_LAST)
  }

  test("isNull / isNotNull / isNaN") {
    assertEquals(col("a").isNull.expr.getUnresolvedFunction.functionName, "isNull")
    assertEquals(col("a").isNotNull.expr.getUnresolvedFunction.functionName, "isNotNull")
    assertEquals(col("a").isNaN.expr.getUnresolvedFunction.functionName, "isNaN")
  }

  test("isin builds an 'in' call including the column") {
    val f = col("a").isin(1, 2, 3).expr.getUnresolvedFunction
    assertEquals(f.functionName, "in")
    assertEquals(f.arguments.size, 4)
  }

  test("getItem / getField produce extract-value expressions") {
    assert(col("a")("k").expr.exprType.isUnresolvedExtractValue)
    assert(col("a").getField("f").expr.exprType.isUnresolvedExtractValue)
  }

  test("between expands to >= AND <=") {
    val f = col("a").between(1, 10).expr.getUnresolvedFunction
    assertEquals(f.functionName, "and")
    assertEquals(f.arguments.head.getUnresolvedFunction.functionName, ">=")
    assertEquals(f.arguments(1).getUnresolvedFunction.functionName, "<=")
  }

  test("bitwise operators") {
    assertEquals(col("a").bitwiseAND(col("b")).expr.getUnresolvedFunction.functionName, "&")
    assertEquals(col("a").bitwiseOR(col("b")).expr.getUnresolvedFunction.functionName, "|")
    assertEquals(col("a").bitwiseXOR(col("b")).expr.getUnresolvedFunction.functionName, "^")
  }

  test("withField / dropFields produce UpdateFields") {
    assertEquals(col("s").withField("x", lit(1)).expr.getUpdateFields.fieldName, "x")
    assert(col("s").dropFields("x").expr.exprType.isUpdateFields)
  }
}
