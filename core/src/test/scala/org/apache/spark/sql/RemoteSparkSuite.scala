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
 * Base class for integration suites that exercise a real Spark Connect server. The server is
 * selected by the `SPARK_REMOTE` environment variable (for example `sc://localhost:15002`); when it
 * is unset the suite's tests are skipped, so a unit-only build stays hermetic.
 *
 * Subclasses use [[withSpark]] for version-independent features and [[withSparkAtLeast]] to gate
 * features that require a newer server, so the same suite is green across Spark 3.5, 4.0, and 4.1.
 */
abstract class RemoteSparkSuite extends munit.FunSuite {

  protected val remote: Option[String] = sys.env.get("SPARK_REMOTE").filter(_.startsWith("sc://"))
  protected var spark: SparkSession = _

  override def beforeAll(): Unit =
    remote.foreach(r => spark = SparkSession.builder().remote(r).getOrCreate())

  override def afterAll(): Unit =
    if (spark != null) spark.close()

  /** Runs `body` against the live session, or skips when no server is configured. */
  protected def withSpark(body: SparkSession => Unit): Unit =
    if (remote.isDefined) body(spark)

  /** True when the live server's Spark version is at least `major.minor`. */
  protected def serverAtLeast(major: Int, minor: Int): Boolean =
    remote.isDefined && {
      val parts = spark.version.split("[.-]").take(2).map(s => s.toIntOption.getOrElse(0))
      val svMajor = parts.headOption.getOrElse(0)
      val svMinor = parts.lift(1).getOrElse(0)
      svMajor > major || (svMajor == major && svMinor >= minor)
    }

  /** Runs `body` only when the server is at least `major.minor`, otherwise skips. */
  protected def withSparkAtLeast(major: Int, minor: Int)(body: SparkSession => Unit): Unit =
    if (remote.isDefined && serverAtLeast(major, minor)) body(spark)
}
