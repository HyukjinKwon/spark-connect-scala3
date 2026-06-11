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
 * End-to-end tests for Spark Declarative Pipelines (SDP).
 *
 * Skipped unless `SPARK_REMOTE` points at a running server, AND that server is Apache Spark 4.1 or
 * newer (the Declarative Pipelines protocol was introduced in 4.1). This keeps the suite green on
 * the 3.5.x and 4.0.x rows of the integration matrix, where the feature does not exist.
 */
class PipelinesIntegrationSuite extends munit.FunSuite {

  private val remote: Option[String] = sys.env.get("SPARK_REMOTE").filter(_.startsWith("sc://"))

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    remote.foreach(r => spark = SparkSession.builder().remote(r).getOrCreate())

  override def afterAll(): Unit =
    if (spark != null) spark.close()

  /** Pipelines require Spark >= 4.1. */
  private def supportsPipelines(version: String): Boolean = {
    val nums = version.split("\\.").map(_.takeWhile(_.isDigit)).filter(_.nonEmpty).map(_.toInt)
    nums match {
      case Array(major, minor, _*) => major > 4 || (major == 4 && minor >= 1)
      case Array(major) => major > 4
      case _ => false
    }
  }

  private def withPipelines(body: SparkSession => Unit): Unit =
    remote match {
      case Some(_) if spark != null && supportsPipelines(spark.version) => body(spark)
      case _ => // skip: no server configured, or server older than 4.1
    }

  test("declarative pipeline runs materialized views end to end") {
    withPipelines { s =>
      import s.implicits.*
      val pipe = s.pipeline()
      pipe.createMaterializedView("sc3_it_numbers", s.range(0, 50))
      pipe.createMaterializedView(
        "sc3_it_evens",
        pipe.read("sc3_it_numbers").where($"id" % 2 === 0)
      )

      val storage = s"file:///tmp/sc3-pipeline-it-${System.nanoTime()}"
      val events = pipe.startRun(storage = storage)

      assert(
        events.exists(_.message.contains("COMPLETED")),
        s"expected a COMPLETED event, got: ${events.map(_.message).mkString(", ")}"
      )
      assertEquals(s.read.table("sc3_it_evens").count(), 25L)
    }
  }
}
