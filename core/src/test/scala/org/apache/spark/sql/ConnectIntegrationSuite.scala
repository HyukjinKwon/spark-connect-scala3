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

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * End-to-end tests that run against a real Spark Connect server.
 *
 * These are skipped unless the `SPARK_REMOTE` environment variable points at a running server (e.g.
 * `sc://localhost:15099`), so unit-only CI runs stay hermetic. The dedicated integration CI job
 * starts a server and sets `SPARK_REMOTE`.
 */
class ConnectIntegrationSuite extends munit.FunSuite {

  private val remote: Option[String] = sys.env.get("SPARK_REMOTE").filter(_.startsWith("sc://"))

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    remote.foreach(r => spark = SparkSession.builder().remote(r).getOrCreate())

  override def afterAll(): Unit =
    if (spark != null) spark.close()

  private def withSpark(body: SparkSession => Unit): Unit =
    remote match {
      case None => // server not configured; skip
      case Some(_) => body(spark)
    }

  test("server version is reported") {
    withSpark { s =>
      assert(s.version.nonEmpty, "expected a non-empty Spark version")
    }
  }

  test("range().count()") {
    withSpark { s =>
      assertEquals(s.range(10).count(), 10L)
    }
  }

  test("range + filter + collect") {
    withSpark { s =>
      val rows = s.range(0, 10).filter("id % 2 == 0").collect()
      assertEquals(rows.map(_.getLong(0)).toSeq, Seq(0L, 2L, 4L, 6L, 8L))
    }
  }

  test("sql select") {
    withSpark { s =>
      val rows = s.sql("select 1 as a, 'x' as b").collect()
      assertEquals(rows.length, 1)
      assertEquals(rows(0).getInt(0), 1)
      assertEquals(rows(0).getString(1), "x")
    }
  }

  test("createDataFrame round-trips local rows") {
    withSpark { s =>
      val schema =
        StructType(Array(StructField("id", IntegerType), StructField("name", StringType)))
      val df = s.createDataFrame(Seq(Row(1, "a"), Row(2, "b")), schema)
      val rows = df.orderBy("id").collect()
      assertEquals(rows.length, 2)
      assertEquals(rows(0).getString(1), "a")
      assertEquals(rows(1).getInt(0), 2)
    }
  }

  test("implicits: Seq(...).toDF and $\"col\"") {
    withSpark { s =>
      import s.implicits._
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name")
      val rows = df.filter($"id" >= 2).select($"name").orderBy($"name").collect()
      assertEquals(rows.map(_.getString(0)).toSeq, Seq("b", "c"))
    }
  }

  test("conf set/get round-trips") {
    withSpark { s =>
      s.conf.set("spark.sql.shuffle.partitions", "7")
      assertEquals(s.conf.get("spark.sql.shuffle.partitions"), "7")
    }
  }

  test("catalog current database and table lifecycle") {
    withSpark { s =>
      assert(s.catalog.currentDatabase.nonEmpty)
      s.range(3).createOrReplaceTempView("scala3_tmp")
      assert(s.catalog.tableExists("scala3_tmp"))
      assertEquals(s.table("scala3_tmp").count(), 3L)
      assert(s.catalog.dropTempView("scala3_tmp"))
    }
  }
}
