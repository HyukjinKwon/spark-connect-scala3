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
 * Offline plan-building coverage for [[RelationalGroupedDataset]]: the named aggregate shortcuts,
 * the map form of `agg`, and pivot. No server.
 */
class GroupedDatasetSuite extends munit.FunSuite {

  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    try body(spark)
    finally spark.stop()
  }

  test("count() aggregates with a single count expression") {
    withSession { spark =>
      val agg = spark.range(10).groupBy(col("id")).count().relation.getAggregate
      assertEquals(agg.groupType, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
      assertEquals(agg.aggregateExpressions.size, 1)
    }
  }

  test("named numeric aggregates build one expression per column") {
    withSession { spark =>
      val df = spark.range(10).select(col("id").as("a"), col("id").as("b"))
      for (
        g <- Seq[RelationalGroupedDataset => DataFrame](
          _.sum("a", "b"),
          _.avg("a", "b"),
          _.mean("a", "b"),
          _.max("a", "b"),
          _.min("a", "b")
        )
      ) {
        val agg = g(df.groupBy(col("a"))).relation.getAggregate
        assertEquals(agg.aggregateExpressions.size, 2)
      }
    }
  }

  test("agg(Map) maps column -> function name") {
    withSession { spark =>
      val agg =
        spark.range(10).groupBy(col("id")).agg(Map("id" -> "max")).relation.getAggregate
      assertEquals(agg.aggregateExpressions.size, 1)
      assertEquals(
        agg.aggregateExpressions.head.getUnresolvedFunction.functionName,
        "max"
      )
    }
  }

  test("pivot sets GROUP_TYPE_PIVOT and the pivot column/values") {
    withSession { spark =>
      val df = spark.range(10)
      val pivoted = df.groupBy(col("id")).pivot("id", Seq(1L, 2L)).agg(count(lit(1)))
      val agg = pivoted.relation.getAggregate
      assertEquals(agg.groupType, proto.Aggregate.GroupType.GROUP_TYPE_PIVOT)
      assert(agg.pivot.isDefined)
      assertEquals(agg.getPivot.values.size, 2)
    }
  }
}
