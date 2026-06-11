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
import org.apache.spark.sql.functions._

/**
 * End-to-end tests that run against a real Spark Connect server.
 *
 * These are gated on the `SPARK_CONNECT_TEST_REMOTE` environment variable: when it is unset the
 * whole suite is ignored, so the default unit-test run never needs a server. CI runs them by
 * matching `*Integration*` with the variable set (e.g. to `sc://localhost:15002`).
 */
class IntegrationSuite extends munit.FunSuite {

  override def munitIgnore: Boolean = sys.env.get("SPARK_CONNECT_TEST_REMOTE").isEmpty

  private val remote: String =
    sys.env.getOrElse("SPARK_CONNECT_TEST_REMOTE", "sc://localhost:15002")

  private var spark: SparkSession = null

  override def beforeAll(): Unit = {
    if (!munitIgnore) {
      spark = SparkSession.builder.remote(remote).create()
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("range + filter + collect") {
    val rows = spark.range(10).filter(col("id") % 2 === 0).collect()
    assertEquals(rows.length, 5)
    assertEquals(rows.map(_.getLong(0)).toSeq, Seq(0L, 2L, 4L, 6L, 8L))
  }

  test("select with expressions") {
    val rows = spark.range(3).select((col("id") + 1).as("next")).collect()
    assertEquals(rows.map(_.getLong(0)).toSeq, Seq(1L, 2L, 3L))
  }

  test("show returns without error") {
    spark.range(5).show()
  }

  test("groupBy/agg counts and sums per group") {
    val rows = spark
      .range(1, 10)
      .select(col("id"), (col("id") % 3).as("b"))
      .groupBy("b")
      .agg(count(lit(1)).as("n"), sum("id").as("t"))
      .orderBy("b")
      .collect()
    assertEquals(rows.length, 3)
  }

  test("sql returns a literal row") {
    val row = spark.sql("select 1 a, 'x' b").collect().head
    assertEquals(row.getInt(0), 1)
    assertEquals(row.getString(1), "x")
  }

  test("join two ranges on a condition") {
    val left = spark.range(5).select(col("id").as("l"))
    val right = spark.range(5).select(col("id").as("r"))
    val rows = left.join(right, col("l") === col("r"), "inner").collect()
    assertEquals(rows.length, 5)
  }

  test("distinct removes duplicate rows") {
    val df = spark.range(3).union(spark.range(3))
    assertEquals(df.count(), 6L)
    assertEquals(df.distinct().count(), 3L)
  }

  test("union concatenates rows") {
    val df = spark.range(2).union(spark.range(2))
    assertEquals(df.count(), 4L)
  }

  test("limit caps the number of rows") {
    assertEquals(spark.range(100).limit(7).count(), 7L)
  }

  test("count counts all rows") {
    assertEquals(spark.range(42).count(), 42L)
  }

  test("window row_number over partitionBy/orderBy") {
    val w = Window.partitionBy(col("g")).orderBy(col("id"))
    val rows = spark
      .range(6)
      .select(col("id"), (col("id") % 2).as("g"))
      .select(col("g"), row_number().over(w).as("rn"))
      .collect()
    assertEquals(rows.length, 6)
    // Each partition has 3 rows, so the max row_number is 3.
    assert(rows.forall(r => r.getInt(1) >= 1 && r.getInt(1) <= 3))
  }

  test("string functions upper/lower/length/concat") {
    val df = spark.sql("select 'Hello' as s")
    val row = df
      .select(
        upper(col("s")).as("u"),
        lower(col("s")).as("l"),
        length(col("s")).as("len"),
        concat(col("s"), lit("!")).as("c"))
      .collect()
      .head
    assertEquals(row.getString(0), "HELLO")
    assertEquals(row.getString(1), "hello")
    assertEquals(row.getInt(2), 5)
    assertEquals(row.getString(3), "Hello!")
  }

  test("createOrReplaceTempView then query the view") {
    spark.range(4).createOrReplaceTempView("scs3_view")
    val n = spark.sql("select count(*) as c from scs3_view").collect().head.getLong(0)
    assertEquals(n, 4L)
  }
}
