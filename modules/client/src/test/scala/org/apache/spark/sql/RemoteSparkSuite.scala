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
 * Base class for integration suites that talk to a live Spark Connect server. Gated on
 * `SPARK_CONNECT_TEST_REMOTE` (e.g. `sc://localhost:15002`); the whole suite is ignored when it is
 * unset, keeping unit-only builds hermetic. Use [[withSparkAtLeast]] to gate features that need a
 * newer server so the suite stays green across Spark 3.5, 4.0, and 4.1.
 */
abstract class RemoteSparkSuite extends munit.FunSuite {

  override def munitIgnore: Boolean = sys.env.get("SPARK_CONNECT_TEST_REMOTE").isEmpty

  private val remote: String =
    sys.env.getOrElse("SPARK_CONNECT_TEST_REMOTE", "sc://localhost:15002")

  protected var spark: SparkSession = null

  override def beforeAll(): Unit =
    if (!munitIgnore) spark = SparkSession.builder.remote(remote).create()

  override def afterAll(): Unit =
    if (spark != null) spark.close()

  /** True when the live server's Spark version is at least `major.minor`. */
  protected def serverAtLeast(major: Int, minor: Int): Boolean = {
    val parts = spark.version.split("[.-]").take(2).map(s => s.toIntOption.getOrElse(0))
    val svMajor = parts.headOption.getOrElse(0)
    val svMinor = parts.lift(1).getOrElse(0)
    svMajor > major || (svMajor == major && svMinor >= minor)
  }

  /** Runs `body` only when the server is at least `major.minor`, otherwise skips quietly. */
  protected def whenServerAtLeast(major: Int, minor: Int)(body: => Unit): Unit =
    if (serverAtLeast(major, minor)) body

  /**
   * Passes the live session as a stable value so test bodies can `import s.implicits.*` (the
   * `spark` field is a `var`, which Scala 3 will not accept as an import prefix).
   */
  protected def withSpark(body: SparkSession => Unit): Unit = body(spark)
}
