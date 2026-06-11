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
 * End-to-end tests for the catalog metadata getters, the probabilistic sketches, and
 * [[RuntimeConfig]] against a real Spark Connect server. Runs under the shared [[RemoteSparkSuite]]
 * harness, so it executes in the CI integration matrix (Spark 3.5/4.0/4.1) and is skipped when no
 * `SPARK_CONNECT_TEST_REMOTE` server is configured.
 */
class CompletenessIntegrationSuite extends RemoteSparkSuite {

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
    // Built-in function lookup via the catalog is reliable on Spark 4.0+.
    whenServerAtLeast(4, 0) {
      val f = spark.catalog.getFunction("abs")
      assertEquals(f.name, "abs")
    }
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

  test("RuntimeConfig set / get / getAll / getOption / default") {
    spark.conf.set("spark.sql.shuffle.partitions", "7")
    assertEquals(spark.conf.get("spark.sql.shuffle.partitions"), "7")
    assert(
      spark.conf.getAll.contains("spark.sql.shuffle.partitions"),
      "getAll should include a configured key"
    )
    assertEquals(spark.conf.get("spark.connect.scala3.nonexistent", "fallback"), "fallback")
    assertEquals(spark.conf.getOption("spark.connect.scala3.nonexistent"), None)
    // unset restores the default without error.
    spark.conf.unset("spark.sql.shuffle.partitions")
  }
}
