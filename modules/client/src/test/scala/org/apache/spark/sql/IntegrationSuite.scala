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
import org.apache.spark.sql.types._

/** Top-level case class used by the typed-Dataset (`as[T]` / `createDataset`) tests. */
case class ItPerson(id: Long, name: String)

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

  override def beforeAll(): Unit =
    if (!munitIgnore) {
      spark = SparkSession.builder.remote(remote).create()
    }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

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
        concat(col("s"), lit("!")).as("c")
      )
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

  test("createDataFrame round-trips local rows including nulls") {
    val schema = StructType(
      Array(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("score", DoubleType)
      )
    )
    val df = spark.createDataFrame(
      Seq(Row(1, "alice", 9.5), Row(2, null, 7.0), Row(3, "carol", 3.0)),
      schema
    )
    val rows = df.orderBy("id").collect()
    assertEquals(rows.length, 3)
    assertEquals(rows(0).getInt(0), 1)
    assertEquals(rows(0).getString(1), "alice")
    assertEquals(rows(0).getDouble(2), 9.5)
    assert(rows(1).isNullAt(1))
  }

  test("na.drop and na.fill handle missing values") {
    val schema = StructType(Array(StructField("a", StringType), StructField("b", IntegerType)))
    val df = spark.createDataFrame(Seq(Row("x", 1), Row(null, 2), Row("y", null)), schema)
    assertEquals(df.na.drop().count(), 1L)
    assertEquals(df.na.fill("NA").filter(col("a") === "NA").count(), 1L)
  }

  test("stat.corr computes correlation") {
    val df = spark.range(1, 100).select(col("id"), (col("id") * 2).as("d"))
    assert(math.abs(df.stat.corr("id", "d") - 1.0) < 1e-6)
  }

  test("describe and summary produce statistics") {
    val df = spark.range(1, 50).select(col("id"))
    assert(df.describe("id").collect().nonEmpty)
    assert(df.summary().collect().nonEmpty)
  }

  test("catalog lists tables and reports existence") {
    spark.range(5).createOrReplaceTempView("scs3_cat_view")
    assert(spark.catalog.tableExists("scs3_cat_view"))
    assert(spark.catalog.listTables().collect().exists(_.getString(0) == "scs3_cat_view"))
    assertEquals(spark.table("scs3_cat_view").count(), 5L)
    assert(spark.catalog.currentDatabase.nonEmpty)
  }

  test("observe collects named metrics") {
    // Observed metrics are only delivered over Spark Connect from Spark 4.0 onward.
    assume(atLeastSpark(4, 0), "Observed metrics over Spark Connect require Spark 4.0 or newer")
    val obs = new Observation("it_metrics")
    spark.range(10).observe(obs, count(lit(1)).as("cnt"), sum("id").as("total")).collect()
    val metrics = obs.get
    assertEquals(metrics("cnt"), 10L)
    assertEquals(metrics("total"), 45L)
  }

  test("write.parquet and read.parquet round-trip") {
    val dir = java.nio.file.Files.createTempDirectory("scs3-it").toString + "/data"
    spark.range(20).select(col("id"), (col("id") % 4).as("g")).write.mode("overwrite").parquet(dir)
    assertEquals(spark.read.parquet(dir).count(), 20L)
  }

  test("declarative pipeline creates a dataflow graph (Spark 4.1+)") {
    assume(atLeastSpark(4, 1), "Declarative pipelines require Spark 4.1 or newer")
    val pipeline = spark.pipeline()
    assert(pipeline != null)
  }

  test("server analysis errors surface as AnalysisException with a clean message") {
    val e = intercept[AnalysisException] {
      spark.sql("select * from a_table_that_surely_does_not_exist_scs3").collect()
    }
    assert(!e.getMessage.startsWith("INTERNAL"), s"gRPC status code leaked: ${e.getMessage}")
    assert(e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"), e.getMessage)
  }

  test("as[T] decodes rows into a case class") {
    val people = spark.sql("select 1L as id, 'alice' as name").as[ItPerson].collect()
    assertEquals(people.toSeq, Seq(ItPerson(1L, "alice")))
  }

  test("as[Long] decodes a single-column dataset") {
    assertEquals(spark.range(4).as[Long].collect().toSeq, Seq(0L, 1L, 2L, 3L))
  }

  test("createDataset round-trips typed values") {
    val ds = spark.createDataset(Seq(ItPerson(1L, "a"), ItPerson(2L, "b")))
    assertEquals(ds.collect().toSeq, Seq(ItPerson(1L, "a"), ItPerson(2L, "b")))
  }

  /** True if the connected server is at least the given Spark major.minor version. */
  private def atLeastSpark(major: Int, minor: Int): Boolean = {
    val parts = spark.version.split("\\.")
    val maj = parts.lift(0).flatMap(_.takeWhile(_.isDigit).toIntOption).getOrElse(0)
    val min = parts.lift(1).flatMap(_.takeWhile(_.isDigit).toIntOption).getOrElse(0)
    maj > major || (maj == major && min >= minor)
  }
}
