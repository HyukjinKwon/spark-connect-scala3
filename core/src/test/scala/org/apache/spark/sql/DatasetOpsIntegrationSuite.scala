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
 * End-to-end validation of [[Dataset]] transformations against a real Spark Connect server.
 * Complements [[SparkConnectIntegrationSuite]] with the relational operators owned by the dataset
 * lane (join, set ops, ordering, dedup, drop, na, stat). Skipped automatically when no server is
 * reachable (defaults to `sc://localhost:15099`, override with `SPARK_REMOTE`).
 */
class DatasetOpsIntegrationSuite extends munit.FunSuite:

  private val remote: String = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15099")

  private def serverReachable: Boolean =
    try
      val uri = new java.net.URI(remote.replaceFirst("^sc://", "http://"))
      val host = Option(uri.getHost).getOrElse("localhost")
      val port = if uri.getPort > 0 then uri.getPort else 15002
      val sock = new java.net.Socket()
      sock.connect(new java.net.InetSocketAddress(host, port), 500)
      sock.close()
      true
    catch case _: Throwable => false

  override def munitIgnore: Boolean = !serverReachable

  private lazy val spark: SparkSession = SparkSession.builder().remote(remote).getOrCreate()

  override def afterAll(): Unit =
    try spark.close()
    catch case _: Throwable => ()

  test("orderBy + limit returns rows in order") {
    val rows = spark.range(10).orderBy(col("id").desc).limit(3).collect()
    assertEquals(rows.map(_.getLong(0)).toSeq, Seq(9L, 8L, 7L))
  }

  test("distinct removes duplicates") {
    val df = spark.range(5).select((col("id") % lit(2)).as("m"))
    assertEquals(df.distinct().count(), 2L)
  }

  test("union concatenates (all)") {
    val df = spark.range(3)
    assertEquals(df.union(df).count(), 6L)
  }

  test("intersect / except") {
    val a = spark.range(0, 6)
    val b = spark.range(3, 9)
    assertEquals(a.intersect(b).count(), 3L) // {3,4,5}
    assertEquals(a.except(b).count(), 3L) // {0,1,2}
  }

  test("inner join on a condition") {
    val left = spark.range(5).select(col("id").as("k"))
    val right = spark.range(3).select(col("id").as("k"))
    val joined = left.join(right, left("k") === right("k"), "inner")
    assertEquals(joined.count(), 3L)
  }

  test("drop removes a column") {
    val df = spark.range(3).withColumn("twice", col("id") * lit(2))
    assertEquals(df.columns.toSeq, Seq("id", "twice"))
    assertEquals(df.drop("twice").columns.toSeq, Seq("id"))
  }

  test("groupBy with multiple aggregates") {
    val df = spark.range(6).select((col("id") % lit(2)).as("g"), col("id"))
    val agg = df.groupBy(col("g")).agg(count(lit(1)).as("c"), max(col("id")).as("mx"))
    assertEquals(agg.count(), 2L)
    assertEquals(agg.columns.toSeq, Seq("g", "c", "mx"))
  }

  test("dropDuplicates on a subset of columns") {
    val df = spark.range(6).select((col("id") % lit(2)).as("g"))
    assertEquals(df.dropDuplicates(Seq("g")).count(), 2L)
  }

  test("na.fill replaces nulls") {
    val df = spark.sql("SELECT * FROM VALUES (1, 'a'), (2, NULL) AS t(id, name)")
    val filled = df.na.fill("missing")
    val names = filled.orderBy(col("id")).collect().map(_.getString(1)).toSeq
    assertEquals(names, Seq("a", "missing"))
  }

  test("stat.corr computes correlation") {
    val df = spark.range(10).select(col("id").as("x"), (col("id") * lit(2)).as("y"))
    assert(math.abs(df.stat.corr("x", "y") - 1.0) < 1e-6)
  }

  test("toDF renames columns positionally") {
    val df = spark.range(2).toDF("renamed")
    assertEquals(df.columns.toSeq, Seq("renamed"))
  }

  test("grouped stddev and variance aggregate numeric columns") {
    val df = spark.range(1, 5).select((col("id") % lit(2)).as("g"), col("id").as("v"))
    val r = df.groupBy(col("g")).stddev("v").collect()
    assertEquals(r.length, 2)
    // population variance over all numeric columns also works (uses schema to pick numeric cols)
    assert(df.groupBy(col("g")).variance("v").columns.contains("var_samp(v)"))
  }

  test("create_map / map_contains_key round-trip through the server") {
    val df = spark
      .range(1)
      .select(create_map(lit("k"), lit(1)).as("m"))
      .select(map_contains_key(col("m"), "k").as("has"))
    assertEquals(df.collect().head.getBoolean(0), true)
  }

  test("regexp_substr extracts the matched substring") {
    val df = spark.sql("SELECT 'a1b2' AS s").select(regexp_substr(col("s"), lit("[0-9]+")).as("d"))
    assertEquals(df.collect().head.getString(0), "1")
  }

  test("unpivot reshapes wide to long") {
    val df = spark.sql("SELECT 1 AS id, 10 AS a, 20 AS b")
    val long = df.unpivot(Array(col("id")), Array(col("a"), col("b")), "key", "val")
    assertEquals(long.count(), 2L)
  }
