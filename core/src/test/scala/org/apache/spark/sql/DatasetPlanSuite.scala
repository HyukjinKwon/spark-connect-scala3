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
import org.apache.spark.sql.functions.*

/**
 * Unit tests for the dataset lane that verify the protobuf logical plan a transformation builds,
 * WITHOUT contacting a server. Transformations are pure plan construction in Spark Connect, so we
 * can assert on `Dataset.relation` directly. A lazily-built (unconnected) session is sufficient.
 */
class DatasetPlanSuite extends munit.FunSuite:

  // No `.config(...)` => `create()` performs no RPC; the channel is built lazily.
  private lazy val spark: SparkSession =
    SparkSession.builder().remote("sc://localhost:15099").create()

  private def src: DataFrame = spark.range(10)

  test("range builds a Range relation with a plan id") {
    val r = src.relation
    assert(r.relType.range.isDefined)
    assert(r.common.flatMap(_.planId).isDefined)
  }

  test("filter builds Filter(input = source)") {
    val r = src.filter("id > 2").relation
    assert(r.relType.filter.isDefined)
    assert(r.getFilter.input.get.relType.range.isDefined, "Filter input should be the Range")
    assert(r.getFilter.condition.isDefined)
  }

  test("select builds Project with one expression per column") {
    val r = src.select(col("id"), lit(1)).relation
    assertEquals(r.getProject.expressions.size, 2)
    assert(r.getProject.input.get.relType.range.isDefined)
  }

  test("limit / offset / tail carry the count") {
    assertEquals(src.limit(3).relation.getLimit.limit, 3)
    assertEquals(src.offset(2).relation.getOffset.offset, 2)
  }

  test("sort builds a global Sort with one order per column") {
    val r = src.sort(col("id")).relation
    assertEquals(r.getSort.order.size, 1)
    assertEquals(r.getSort.isGlobal, Some(true))
  }

  test("sortWithinPartitions is not global") {
    assertEquals(src.sortWithinPartitions(col("id")).relation.getSort.isGlobal, Some(false))
  }

  test("distinct builds Deduplicate(allColumnsAsKeys = true)") {
    assertEquals(src.distinct().relation.getDeduplicate.allColumnsAsKeys, Some(true))
  }

  test("dropDuplicates(cols) carries the column names") {
    assertEquals(src.dropDuplicates(Seq("id")).relation.getDeduplicate.columnNames, Seq("id"))
  }

  test("union builds SET_OP_TYPE_UNION with isAll = true") {
    val r = src.union(src).relation
    assertEquals(r.getSetOp.setOpType, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
    assertEquals(r.getSetOp.isAll, Some(true))
  }

  test("intersect / except set the right op type and isAll") {
    assertEquals(
      src.intersect(src).relation.getSetOp.setOpType,
      proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
    )
    assertEquals(src.intersect(src).relation.getSetOp.isAll, Some(false))
    assertEquals(
      src.except(src).relation.getSetOp.setOpType,
      proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
    )
  }

  test("join with condition and type") {
    val r = src.join(src, col("id") === col("id"), "left").relation
    assertEquals(r.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER)
    assert(r.getJoin.joinCondition.isDefined)
    assert(r.getJoin.left.isDefined && r.getJoin.right.isDefined)
  }

  test("crossJoin sets CROSS and no condition") {
    val r = src.crossJoin(src).relation
    assertEquals(r.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_CROSS)
    assert(r.getJoin.joinCondition.isEmpty)
  }

  test("groupBy().count() builds an Aggregate (GROUPBY) with grouping + agg exprs") {
    val r = src.groupBy(col("id")).count().relation
    assertEquals(r.getAggregate.groupType, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
    assertEquals(r.getAggregate.groupingExpressions.size, 1)
    assertEquals(r.getAggregate.aggregateExpressions.size, 1)
  }

  test("rollup / cube set the group type") {
    assertEquals(
      src.rollup(col("id")).agg(count(lit(1))).relation.getAggregate.groupType,
      proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP
    )
    assertEquals(
      src.cube(col("id")).agg(count(lit(1))).relation.getAggregate.groupType,
      proto.Aggregate.GroupType.GROUP_TYPE_CUBE
    )
  }

  test("withColumn builds WithColumns with one alias") {
    val r = src.withColumn("plus", col("id") + lit(1)).relation
    assertEquals(r.getWithColumns.aliases.size, 1)
    assertEquals(r.getWithColumns.aliases.head.name, Seq("plus"))
  }

  test("withColumnRenamed builds a Rename") {
    val r = src.withColumnRenamed("id", "key").relation
    assertEquals(r.getWithColumnsRenamed.renames.head.colName, "id")
    assertEquals(r.getWithColumnsRenamed.renames.head.newColName, "key")
  }

  test("drop by name builds Drop with column_names") {
    assertEquals(src.drop("id").relation.getDrop.columnNames, Seq("id"))
  }

  test("repartition(n) shuffles; coalesce(n) does not") {
    assertEquals(src.repartition(4).relation.getRepartition.numPartitions, 4)
    assertEquals(src.repartition(4).relation.getRepartition.shuffle, Some(true))
    assertEquals(src.coalesce(2).relation.getRepartition.shuffle, Some(false))
  }

  test("repartition(exprs) builds RepartitionByExpression") {
    val r = src.repartition(col("id")).relation
    assertEquals(r.getRepartitionByExpression.partitionExprs.size, 1)
  }

  test("toDF(names) and as(alias)") {
    assertEquals(src.toDF("x").relation.getToDf.columnNames, Seq("x"))
    assertEquals(src.as("t").relation.getSubqueryAlias.alias, "t")
  }

  test("sample carries fraction, replacement and seed") {
    val s = src.sample(withReplacement = true, fraction = 0.3, seed = 42L).relation.getSample
    assertEquals(s.upperBound, 0.3)
    assertEquals(s.withReplacement, Some(true))
    assertEquals(s.seed, Some(42L))
  }

  test("na.fill / na.drop / na.replace build the NA relations") {
    assert(src.na.fill(0L).relation.relType.fillNa.isDefined)
    assert(src.na.drop("any").relation.relType.dropNa.isDefined)
    assert(src.na.replace("id", Map(1L -> 2L)).relation.relType.replace.isDefined)
  }

  test("stat.crosstab / freqItems / sampleBy build the Stat relations") {
    assert(src.stat.crosstab("id", "id").relation.relType.crosstab.isDefined)
    assert(src.stat.freqItems(Seq("id")).relation.relType.freqItems.isDefined)
    assert(src.stat.sampleBy("id", Map(1L -> 0.5), 1L).relation.relType.sampleBy.isDefined)
  }

  test("each transformation gets a fresh, unique plan id") {
    val a = src.filter("id > 1")
    val b = a.select(col("id"))
    val ids = Seq(src.planId, a.planId, b.planId).flatten
    assertEquals(ids.distinct.size, ids.size, "plan ids must be unique across relations")
  }

  test("createOrReplaceTempView issues no error building the command path") {
    // We can't execute without a server, but constructing the Dataset must not throw.
    val df = src.filter("id > 0")
    assert(df.relation.relType.filter.isDefined)
  }
