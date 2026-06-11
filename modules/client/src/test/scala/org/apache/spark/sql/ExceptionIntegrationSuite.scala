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
 * End-to-end checks that server errors surface as Spark exceptions, and (on Spark 4.0+) that the
 * `FetchErrorDetails` round-trip attaches the real server-side stack trace. Gated on
 * `SPARK_CONNECT_TEST_REMOTE` like [[IntegrationSuite]].
 */
class ExceptionIntegrationSuite extends munit.FunSuite {

  override def munitIgnore: Boolean = sys.env.get("SPARK_CONNECT_TEST_REMOTE").isEmpty

  private val remote: String =
    sys.env.getOrElse("SPARK_CONNECT_TEST_REMOTE", "sc://localhost:15002")

  private var spark: SparkSession = null

  private def serverVersion: String =
    sys.env.getOrElse("SPARK_CONNECT_TEST_VERSION", spark.version)

  private def serverAtLeast(major: Int, minor: Int): Boolean = {
    val parts =
      serverVersion.split("\\.").map(_.takeWhile(_.isDigit)).filter(_.nonEmpty).map(_.toInt)
    val maj = parts.lift(0).getOrElse(0)
    val min = parts.lift(1).getOrElse(0)
    maj > major || (maj == major && min >= minor)
  }

  override def beforeAll(): Unit =
    if (!munitIgnore) spark = SparkSession.builder.remote(remote).create()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  test("analysis error surfaces as AnalysisException with a clean message") {
    val e = intercept[AnalysisException] {
      spark.sql("select * from a_table_that_surely_does_not_exist_scs3").collect()
    }
    assert(!e.getMessage.startsWith("INTERNAL"), s"gRPC status code leaked: ${e.getMessage}")
    assert(e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"), e.getMessage)
  }

  test("parse error surfaces as ParseException") {
    intercept[org.apache.spark.sql.catalyst.parser.ParseException] {
      spark.sql("slect 1").collect()
    }
  }

  test("FetchErrorDetails attaches the server-side stack trace (Spark 4.0+)") {
    assume(serverAtLeast(4, 0), "FetchErrorDetails is reliably populated on Spark 4.0+")
    val e = intercept[AnalysisException] {
      spark.sql("select * from a_table_that_surely_does_not_exist_scs3").collect()
    }
    assert(
      e.getStackTrace.exists(_.getClassName.startsWith("org.apache.spark")),
      "expected a server-side org.apache.spark stack frame, got:\n" +
        e.getStackTrace.take(10).mkString("\n")
    )
  }
}
