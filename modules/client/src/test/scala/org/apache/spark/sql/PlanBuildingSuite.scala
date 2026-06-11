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
 * Unit tests that build DataFrame plans WITHOUT a server. Constructing a [[SparkSession]] via the
 * builder opens a gRPC channel lazily but issues no RPC; chaining transformations only mutates the
 * in-memory protobuf [[proto.Relation]] tree, so we can assert on plan structure entirely offline.
 *
 * Each test builds its own session pointed at a bogus remote and stops it in a finally block.
 */
class PlanBuildingSuite extends munit.FunSuite {

  /** Builds a fresh, non-connecting session and stops it after the body runs. */
  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    try body(spark)
    finally spark.stop()
  }

  test("range produces a Range relation") {
    withSession { spark =>
      val df = spark.range(10)
      assert(df.relation.relType.isRange)
      assertEquals(df.relation.getRange.end, 10L)
    }
  }

  test("select produces a Project") {
    withSession { spark =>
      val df = spark.range(10).select(col("id"))
      assert(df.relation.relType.isProject)
      assertEquals(df.relation.getProject.expressions.size, 1)
    }
  }

  test("filter produces a Filter") {
    withSession { spark =>
      val df = spark.range(10).filter(col("id") > 5)
      assert(df.relation.relType.isFilter)
      assert(df.relation.getFilter.condition.isDefined)
    }
  }

  test("limit produces a Limit with the right N") {
    withSession { spark =>
      val df = spark.range(10).limit(3)
      assert(df.relation.relType.isLimit)
      assertEquals(df.relation.getLimit.limit, 3)
    }
  }

  test("offset produces an Offset with the right N") {
    withSession { spark =>
      val df = spark.range(10).offset(2)
      assertEquals(df.relation.getOffset.offset, 2)
    }
  }

  test("sort produces a Sort") {
    withSession { spark =>
      val df = spark.range(10).sort(col("id").desc)
      assert(df.relation.relType.isSort)
      assertEquals(df.relation.getSort.order.size, 1)
      assertEquals(
        df.relation.getSort.order.head.direction,
        proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING)
    }
  }

  test("groupBy().agg produces an Aggregate") {
    withSession { spark =>
      val df = spark.range(10).groupBy(col("id")).agg(count(lit(1)))
      assert(df.relation.relType.isAggregate)
      val agg = df.relation.getAggregate
      assertEquals(agg.groupType, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
      assertEquals(agg.groupingExpressions.size, 1)
      assertEquals(agg.aggregateExpressions.size, 1)
    }
  }

  test("rollup produces a ROLLUP aggregate") {
    withSession { spark =>
      val df = spark.range(10).rollup(col("id")).agg(count(lit(1)))
      assertEquals(
        df.relation.getAggregate.groupType,
        proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
    }
  }

  test("cube produces a CUBE aggregate") {
    withSession { spark =>
      val df = spark.range(10).cube(col("id")).agg(count(lit(1)))
      assertEquals(
        df.relation.getAggregate.groupType,
        proto.Aggregate.GroupType.GROUP_TYPE_CUBE)
    }
  }

  test("inner join produces a Join with JOIN_TYPE_INNER") {
    withSession { spark =>
      val left = spark.range(10)
      val right = spark.range(10)
      val df = left.join(right, col("id") === col("id"), "inner")
      assert(df.relation.relType.isJoin)
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_INNER)
      assert(df.relation.getJoin.joinCondition.isDefined)
    }
  }

  test("left join produces JOIN_TYPE_LEFT_OUTER") {
    withSession { spark =>
      val df = spark.range(10).join(spark.range(10), col("id") === col("id"), "left")
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER)
    }
  }

  test("right join produces JOIN_TYPE_RIGHT_OUTER") {
    withSession { spark =>
      val df = spark.range(10).join(spark.range(10), col("id") === col("id"), "right")
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER)
    }
  }

  test("outer join produces JOIN_TYPE_FULL_OUTER") {
    withSession { spark =>
      val df = spark.range(10).join(spark.range(10), col("id") === col("id"), "outer")
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_FULL_OUTER)
    }
  }

  test("semi join produces JOIN_TYPE_LEFT_SEMI") {
    withSession { spark =>
      val df = spark.range(10).join(spark.range(10), col("id") === col("id"), "semi")
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI)
    }
  }

  test("anti join produces JOIN_TYPE_LEFT_ANTI") {
    withSession { spark =>
      val df = spark.range(10).join(spark.range(10), col("id") === col("id"), "anti")
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI)
    }
  }

  test("crossJoin produces JOIN_TYPE_CROSS") {
    withSession { spark =>
      val df = spark.range(10).crossJoin(spark.range(10))
      assertEquals(df.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_CROSS)
    }
  }

  test("union produces a SetOperation with SET_OP_TYPE_UNION") {
    withSession { spark =>
      val df = spark.range(10).union(spark.range(10))
      assert(df.relation.relType.isSetOp)
      assertEquals(
        df.relation.getSetOp.setOpType,
        proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
    }
  }

  test("intersect produces SET_OP_TYPE_INTERSECT") {
    withSession { spark =>
      val df = spark.range(10).intersect(spark.range(10))
      assertEquals(
        df.relation.getSetOp.setOpType,
        proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT)
    }
  }

  test("except produces SET_OP_TYPE_EXCEPT") {
    withSession { spark =>
      val df = spark.range(10).except(spark.range(10))
      assertEquals(
        df.relation.getSetOp.setOpType,
        proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT)
    }
  }

  test("distinct produces a Deduplicate") {
    withSession { spark =>
      val df = spark.range(10).distinct()
      assert(df.relation.relType.isDeduplicate)
      assertEquals(df.relation.getDeduplicate.allColumnsAsKeys, Some(true))
    }
  }

  test("sql produces an Sql relation") {
    withSession { spark =>
      val df = spark.sql("select 1")
      assertEquals(df.relation.getSql.query, "select 1")
    }
  }

  test("col + 1 is an UnresolvedFunction named '+'") {
    val c = col("x") + 1
    assert(c.expr.exprType.isUnresolvedFunction)
    assertEquals(c.expr.getUnresolvedFunction.functionName, "+")
    assertEquals(c.expr.getUnresolvedFunction.arguments.size, 2)
  }

  test("lit(5) is an integer Literal with value 5") {
    val c = lit(5)
    assert(c.expr.exprType.isLiteral)
    val literal = c.expr.getLiteral
    assert(literal.literalType.isInteger)
    assertEquals(literal.getInteger, 5)
  }

  test("col(a).as(b) is an Alias") {
    val c = col("a").as("b")
    assert(c.expr.exprType.isAlias)
    assertEquals(c.expr.getAlias.name, Seq("b"))
  }

  test("col(name) is an UnresolvedAttribute") {
    val c = col("name")
    assert(c.expr.exprType.isUnresolvedAttribute)
    assertEquals(c.expr.getUnresolvedAttribute.unparsedIdentifier, "name")
  }

  test("=== is an UnresolvedFunction named '=='") {
    val c = col("a") === col("b")
    assertEquals(c.expr.getUnresolvedFunction.functionName, "==")
  }

  test("chained transformations nest the input relation") {
    withSession { spark =>
      val df = spark.range(10).filter(col("id") > 2).select(col("id"))
      // top is a Project whose input is a Filter whose input is a Range
      assert(df.relation.relType.isProject)
      val filterRel = df.relation.getProject.getInput
      assert(filterRel.relType.isFilter)
      assert(filterRel.getFilter.getInput.relType.isRange)
    }
  }
}
