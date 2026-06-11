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
 * End-to-end validation of [[Observation]] against a real Spark Connect server. Skipped
 * automatically when no server is reachable (defaults to `sc://localhost:15099`, override with
 * `SPARK_REMOTE`).
 */
class ObservationIntegrationSuite extends munit.FunSuite:

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

  test("Observation captures named aggregate metrics after an action") {
    val obs = Observation("metrics")
    val df = spark
      .range(100)
      .observe(obs, count(lit(1)).as("rows"), max(col("id")).as("max_id"))
    df.collect()
    val metrics = obs.get
    assertEquals(metrics.size, 2)
    assert(metrics.values.toSet.contains(100L), s"expected rows=100 in $metrics")
    assert(metrics.values.toSet.contains(99L), s"expected max_id=99 in $metrics")
  }

  test("getOption is empty before any action") {
    val obs = Observation()
    spark.range(10).observe(obs, count(lit(1)).as("rows"))
    assertEquals(obs.getOption, None)
  }
