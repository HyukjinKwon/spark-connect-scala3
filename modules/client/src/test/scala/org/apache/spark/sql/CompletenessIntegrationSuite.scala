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

/**
 * End-to-end tests for the catalog metadata getters and the probabilistic sketches, against a real
 * Spark Connect server. Skipped automatically when no server is reachable (defaults to
 * `sc://localhost:15099`, override with `SPARK_REMOTE`).
 */
class CompletenessIntegrationSuite extends munit.FunSuite:

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

  private lazy val spark: SparkSession = SparkSession.builder.remote(remote).getOrCreate()

  override def afterAll(): Unit =
    try spark.close()
    catch case _: Throwable => ()

  test("Catalog.getTable on a temporary view") {
    spark.range(5).createOrReplaceTempView("completeness_view")
    val t = spark.catalog.getTable("completeness_view")
    assertEquals(t.name, "completeness_view")
    assert(t.isTemporary, s"expected a temporary view, got $t")
  }

  test("Catalog.getDatabase('default')") {
    val d = spark.catalog.getDatabase("default")
    assertEquals(d.name, "default")
    assert(d.locationUri != null && d.locationUri.nonEmpty, s"expected a location, got $d")
  }

  test("Catalog.getFunction for a built-in") {
    val f = spark.catalog.getFunction("abs")
    assertEquals(f.name, "abs")
  }

  test("stat.countMinSketch estimates frequencies") {
    val cms = spark.range(1000).stat.countMinSketch("id", 0.001, 0.99, 42)
    assertEquals(cms.totalCount(), 1000L)
    assert(cms.estimateCount(5L) >= 1L, "5 is present so its estimate must be >= 1")
  }

  test("stat.bloomFilter membership") {
    val bf = spark.range(1000).stat.bloomFilter("id", 1000L, 0.03)
    assert(bf.mightContain(5L), "5 is present so mightContain must be true")
    assert(!bf.mightContain(10_000_000L), "absent key should (almost surely) be reported absent")
  }
