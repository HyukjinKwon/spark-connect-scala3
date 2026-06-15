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
 * Further offline plan-building coverage complementing [[PlanBuildingSuite]]: the transformation
 * surface of [[Dataset]], plus `na.*` and `stat.*` relations. Each builds an in-memory
 * [[proto.Relation]] tree with no RPC, so we assert the relation kind and key fields.
 */
class PlanBuildingExtraSuite extends munit.FunSuite {

  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    try body(spark)
    finally spark.stop()
  }

  test("set-operation variants carry the right isAll / byName flags") {
    withSession { spark =>
      val a = spark.range(10)
      val b = spark.range(10)
      assertEquals(a.union(b).relation.getSetOp.isAll, Some(true))
      val byName = a.unionByName(b)
      assertEquals(byName.relation.getSetOp.byName, Some(true))
      assertEquals(a.intersectAll(b).relation.getSetOp.isAll, Some(true))
      assertEquals(a.intersect(b).relation.getSetOp.isAll, Some(false))
      assertEquals(a.exceptAll(b).relation.getSetOp.isAll, Some(true))
      assertEquals(a.except(b).relation.getSetOp.isAll, Some(false))
    }
  }

  test("withColumn / withColumnRenamed build the matching relations") {
    withSession { spark =>
      val df = spark.range(10)
      assert(df.withColumn("c", lit(1)).relation.relType.isWithColumns)
      assert(df.withColumnRenamed("id", "k").relation.relType.isWithColumnsRenamed)
      assert(df.withColumnsRenamed(Map("id" -> "k")).relation.relType.isWithColumnsRenamed)
    }
  }

  test("drop by name and by column build a Drop") {
    withSession { spark =>
      val df = spark.range(10)
      assertEquals(df.drop("id").relation.getDrop.columnNames.toSeq, Seq("id"))
      assertEquals(df.drop(col("id")).relation.getDrop.columns.size, 1)
    }
  }

  test("dropDuplicates with columns builds a Deduplicate over those columns") {
    withSession { spark =>
      val dedup = spark.range(10).dropDuplicates(Seq("id"))
      assert(dedup.relation.relType.isDeduplicate)
      assertEquals(dedup.relation.getDeduplicate.columnNames.toSeq, Seq("id"))
    }
  }

  test("selectExpr and where(String) build Project / Filter") {
    withSession { spark =>
      assert(spark.range(10).selectExpr("id + 1 as next").relation.relType.isProject)
      assert(spark.range(10).where("id > 5").relation.relType.isFilter)
    }
  }

  test("repartition shuffles, coalesce does not") {
    withSession { spark =>
      val df = spark.range(10)
      assertEquals(df.repartition(4).relation.getRepartition.shuffle, Some(true))
      assertEquals(df.repartition(4).relation.getRepartition.numPartitions, 4)
      assertEquals(df.coalesce(2).relation.getRepartition.shuffle, Some(false))
      assert(df.repartition(4, col("id")).relation.relType.isRepartitionByExpression)
      assert(df.repartitionByRange(col("id")).relation.relType.isRepartitionByExpression)
    }
  }

  test("sample carries withReplacement, fraction and seed") {
    withSession { spark =>
      val s = spark.range(100).sample(withReplacement = true, fraction = 0.3, seed = 42L)
      val sample = s.relation.getSample
      assertEquals(sample.withReplacement, Some(true))
      assertEquals(sample.upperBound, 0.3)
      assertEquals(sample.seed, Some(42L))
    }
  }

  test("toDF(names), as(alias), hint, describe and summary build their relations") {
    withSession { spark =>
      val df = spark.range(10)
      assertEquals(df.toDF("x").relation.getToDf.columnNames.toSeq, Seq("x"))
      assertEquals(df.as("t").relation.getSubqueryAlias.alias, "t")
      assert(df.hint("broadcast").relation.relType.isHint)
      assert(df.describe("id").relation.relType.isDescribe)
      assert(df.summary("count", "min").relation.relType.isSummary)
    }
  }

  test("na.drop / na.fill / na.replace build NA relations") {
    withSession { spark =>
      val df = spark.range(10)
      assert(df.na.drop().relation.relType.isDropNa)
      assertEquals(df.na.drop(2).relation.getDropNa.minNonNulls, Some(2))
      assert(df.na.fill(0L).relation.relType.isFillNa)
      assert(df.na.fill(Map("id" -> 0L)).relation.relType.isFillNa)
      assert(df.na.replace("id", Map(1L -> 2L)).relation.relType.isReplace)
    }
  }

  test("stat.crosstab / freqItems / sampleBy build stat relations") {
    withSession { spark =>
      val df = spark.range(10)
      assert(df.stat.crosstab("id", "id").relation.relType.isCrosstab)
      assert(df.stat.freqItems(Seq("id")).relation.relType.isFreqItems)
      assert(df.stat.sampleBy("id", Map(1L -> 0.5), 1L).relation.relType.isSampleBy)
    }
  }
}
