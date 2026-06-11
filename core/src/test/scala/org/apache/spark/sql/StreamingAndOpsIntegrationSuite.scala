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
 * End-to-end tests for Structured Streaming and the extended Dataset operations (unpivot, toJSON,
 * persist, checkpoint, repartitionByRange, sameSemantics). Skipped unless `SPARK_REMOTE` points at
 * a running Spark Connect server.
 */
class StreamingAndOpsIntegrationSuite extends munit.FunSuite {

  private val remote: Option[String] = sys.env.get("SPARK_REMOTE").filter(_.startsWith("sc://"))
  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    remote.foreach(r => spark = SparkSession.builder().remote(r).getOrCreate())

  override def afterAll(): Unit =
    if (spark != null) spark.close()

  private def withSpark(body: SparkSession => Unit): Unit =
    if (remote.isDefined) body(spark)

  /** Whether the live server is at least the given Spark version (for feature gating). */
  private def serverAtLeast(major: Int, minor: Int): Boolean =
    remote.isDefined && {
      val parts = spark.version.split("[.-]").take(2).map(s => s.toIntOption.getOrElse(0))
      val (svMajor, svMinor) = (parts.headOption.getOrElse(0), parts.lift(1).getOrElse(0))
      svMajor > major || (svMajor == major && svMinor >= minor)
    }

  /** Run only when the server is at least the given Spark version, otherwise skip. */
  private def withSparkAtLeast(major: Int, minor: Int)(body: SparkSession => Unit): Unit =
    if (remote.isDefined && serverAtLeast(major, minor)) body(spark)

  test("structured streaming: rate source -> memory sink") {
    withSpark { s =>
      val query = s.readStream
        .format("rate")
        .option("rowsPerSecond", "20")
        .load()
        .writeStream
        .format("memory")
        .queryName("sc3_rate_test")
        .outputMode("append")
        .start()
      try {
        Thread.sleep(2500)
        assert(query.isActive, "streaming query should be active")
        assert(query.id.nonEmpty, "query should have an id")
        // The memory sink is queryable as a table.
        assert(s.table("sc3_rate_test").count() >= 0L)
      } finally query.stop()
      assert(!query.isActive, "streaming query should be stopped")
    }
  }

  test("streaming query manager lists active queries") {
    withSpark { s =>
      val q = s.readStream
        .format("rate")
        .load()
        .writeStream
        .format("memory")
        .queryName("sc3_mgr_test")
        .start()
      try assert(s.streams.active.exists(_.id == q.id))
      finally q.stop()
    }
  }

  test("unpivot wide -> long") {
    withSpark { s =>
      import s.implicits._
      val df = Seq((1, 10, 100), (2, 20, 200)).toDF("id", "a", "b")
      val long = df.unpivot(Array($"id"), Array($"a", $"b"), "key", "value").orderBy($"id", $"key")
      val rows = long.collect()
      assertEquals(rows.length, 4)
      assertEquals(rows.map(_.getString(1)).distinct.sorted.toSeq, Seq("a", "b"))
    }
  }

  test("toJSON") {
    withSpark { s =>
      val js = s.range(0, 1).toJSON.collect()
      assert(js(0).getString(0).contains("\"id\""), js(0).getString(0))
    }
  }

  test("persist / storageLevel / unpersist") {
    withSpark { s =>
      val df = s.range(10).persist()
      assertEquals(df.count(), 10L)
      val level = df.storageLevel
      assert(level.useMemory || level.useDisk, level.toString)
      df.unpersist()
    }
  }

  test("repartitionByRange and sameSemantics / semanticHash") {
    withSpark { s =>
      import s.implicits._
      val a = s.range(0, 20).repartitionByRange(4, $"id")
      assertEquals(a.count(), 20L)
      assert(s.range(5).sameSemantics(s.range(5)))
      assert(s.range(5).semanticHash() == s.range(5).semanticHash())
    }
  }

  test("local checkpoint (Spark 4.0+)") {
    // The Connect CheckpointCommand was introduced in Spark 4.0.
    withSparkAtLeast(4, 0) { s =>
      val df = s.range(0, 25).localCheckpoint()
      assertEquals(df.count(), 25L)
    }
  }

  test("transform chaining") {
    withSpark { s =>
      val out = s.range(10).transform(_.filter("id > 4")).transform(_.limit(3))
      assertEquals(out.count(), 3L)
    }
  }
}
