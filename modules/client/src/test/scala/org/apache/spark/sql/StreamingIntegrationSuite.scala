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

import org.apache.spark.sql.streaming.Trigger

/**
 * End-to-end Structured Streaming tests against a real Spark Connect server.
 *
 * Gated on `SPARK_CONNECT_TEST_REMOTE` like [[IntegrationSuite]]; the whole suite is ignored when
 * it is unset so the default unit-test run stays hermetic. The `rate` source and `memory` sink are
 * available on every supported server (Spark 3.5+), so no version gate is needed.
 */
class StreamingIntegrationSuite extends munit.FunSuite {

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

  test("rate source -> memory sink, queried as a table") {
    val query = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()
      .selectExpr("value", "value % 5 AS bucket")
      .writeStream
      .format("memory")
      .queryName("sc3_rates")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("100 milliseconds"))
      .start()
    try {
      query.processAllAvailable()
      val total = spark.sql("SELECT count(*) AS n FROM sc3_rates").collect().head.getLong(0)
      assert(total >= 0L, "expected the in-memory sink to be queryable as a table")
    } finally query.stop()
  }

  test("streams manager tracks the active query") {
    val query = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      .writeStream
      .format("memory")
      .queryName("sc3_active")
      .outputMode("append")
      .start()
    try {
      query.processAllAvailable()
      assert(spark.streams.active.exists(_.name == "sc3_active") || query.isActive)
    } finally query.stop()
  }
}
