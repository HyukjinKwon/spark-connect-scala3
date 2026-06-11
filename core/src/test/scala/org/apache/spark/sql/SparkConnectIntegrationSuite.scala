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
import org.apache.spark.sql.types.*

/**
 * End-to-end tests that talk to a real Spark Connect server.
 *
 * They are skipped automatically unless a server is reachable. By default the suite probes
 * `sc://localhost:15099`; override with the `SPARK_REMOTE` environment variable (e.g.
 * `SPARK_REMOTE=sc://localhost:15002`).
 *
 * These tests are the ultimate validation of the whole client stack: plan building (dataset/
 * functions), the gRPC transport (foundation), and Arrow result decoding (results).
 */
class SparkConnectIntegrationSuite extends munit.FunSuite:

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

  private lazy val spark: SparkSession =
    SparkSession.builder().remote(remote).getOrCreate()

  override def afterAll(): Unit =
    try spark.close()
    catch case _: Throwable => ()

  test("range: count and collect") {
    val df = spark.range(10)
    assertEquals(df.count(), 10L)
    val rows = df.collect()
    assertEquals(rows.length, 10)
    assertEquals(rows.map(_.getLong(0)).toSeq, (0L until 10L).toSeq)
  }

  test("select + filter + withColumn") {
    val df = spark
      .range(5)
      .select(col("id"), (col("id") * 2).as("doubled"))
      .filter(col("id") > 1)
    val rows = df.collect()
    assertEquals(rows.length, 3)
    assertEquals(
      rows.map(r => (r.getLong(0), r.getLong(1))).toSeq,
      Seq((2L, 4L), (3L, 6L), (4L, 8L))
    )
  }

  test("groupBy + aggregate") {
    val df = spark
      .range(6)
      .withColumn("grp", col("id") % 2)
      .groupBy(col("grp"))
      .agg(count(lit(1)).as("n"), sum(col("id")).as("s"))
      .orderBy(col("grp"))
    val rows = df.collect()
    assertEquals(rows.length, 2)
  }

  test("sql query") {
    val rows = spark.sql("select 1 as a, 'x' as b").collect()
    assertEquals(rows.length, 1)
    assertEquals(rows(0).getInt(0), 1)
    assertEquals(rows(0).getString(1), "x")
  }

  test("schema is reported with correct types") {
    val schema = spark.range(1).select(col("id").cast(StringType).as("s")).schema
    assertEquals(schema.fieldNames.toSeq, Seq("s"))
    assertEquals(schema("s").dataType, StringType)
  }

  test("show produces a table string") {
    // Just assert it does not throw and produces output.
    spark.range(3).show()
  }
