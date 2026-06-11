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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

/**
 * Per-component live coverage (functions, columns, window, I/O) plus the extended Dataset
 * operations (implicits, unpivot, transpose, persist, checkpoint, repartitionByRange, toJSON,
 * randomSplit). Version-sensitive features are gated so the suite is green on Spark 3.5, 4.0, 4.1.
 */
class FeatureCoverageIntegrationSuite extends RemoteSparkSuite {

  test("implicits: Seq(...).toDF and $\"col\"") {
    withSpark { s =>
      import s.implicits.*
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name")
      val rows = df.filter($"id" >= 2).select($"name").orderBy($"name").collect()
      assertEquals(rows.map(_.getString(0)).toSeq, Seq("b", "c"))
    }
  }

  test("functions: string, math, conditional, aggregate") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq((" Spark ", -4.0, 2))
        .toDF("s", "x", "g")
        .select(
          upper(trim($"s")).as("u"),
          length(trim($"s")).as("len"),
          round(abs($"x"), 1).as("absx"),
          when($"g" === 2, lit("two")).otherwise(lit("other")).as("w")
        )
        .collect()
        .head
      assertEquals(r.getString(0), "SPARK")
      assertEquals(r.getInt(1), 5)
      assertEquals(r.getDouble(2), 4.0)
      assertEquals(r.getString(3), "two")

      val agg = s.range(0, 10).agg(sum(col("id")).as("s"), count(lit(1)).as("c")).collect().head
      assertEquals(agg.getLong(0), 45L)
      assertEquals(agg.getLong(1), 10L)
    }
  }

  test("column operators: arithmetic, comparison, like, cast, isin") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq((10, "spark-connect"))
        .toDF("n", "str")
        .select(
          ($"n" + 5).as("plus"),
          ($"n" > 3).as("gt"),
          $"str".like("spark%").as("like"),
          $"str".substr(1, 5).cast("string").as("sub"),
          lit("b").isin("a", "b").as("inset")
        )
        .collect()
        .head
      assertEquals(r.getInt(0), 15)
      assertEquals(r.getBoolean(1), true)
      assertEquals(r.getBoolean(2), true)
      assertEquals(r.getString(3), "spark")
      assertEquals(r.getBoolean(4), true)
    }
  }

  test("window functions: row_number and running sum") {
    withSpark { s =>
      import s.implicits.*
      val df = Seq(("a", 10), ("a", 30), ("a", 20), ("b", 5)).toDF("g", "v")
      val w = Window.partitionBy($"g").orderBy($"v".desc)
      val rows = df
        .select($"g", $"v", row_number().over(w).as("rn"), sum($"v").over(w).as("run"))
        .orderBy($"g", $"rn")
        .collect()
      assertEquals(rows(0).getInt(1), 30)
      assertEquals(rows(0).getInt(2), 1)
      assertEquals(rows(0).getLong(3), 30L)
      assertEquals(rows(1).getLong(3), 50L)
    }
  }

  test("unpivot wide to long") {
    withSpark { s =>
      import s.implicits.*
      val df = Seq((1, 10, 100), (2, 20, 200)).toDF("id", "a", "b")
      val long = df.unpivot(Array($"id"), Array($"a", $"b"), "key", "value").orderBy($"id", $"key")
      assertEquals(long.count(), 4L)
    }
  }

  test("toJSON") {
    withSpark { s =>
      assert(s.range(0, 1).toJSON.collect().head.getString(0).contains("\"id\""))
    }
  }

  test("persist / storageLevel / unpersist") {
    withSpark { s =>
      val df = s.range(10).persist()
      assertEquals(df.count(), 10L)
      assert(df.storageLevel.useMemory || df.storageLevel.useDisk)
      df.unpersist()
    }
  }

  test("repartitionByRange and randomSplit") {
    withSpark { s =>
      import s.implicits.*
      assertEquals(s.range(0, 20).repartitionByRange(4, $"id").count(), 20L)
      val splits = s.range(0, 100).randomSplit(Array(0.5, 0.5), seed = 42L)
      assertEquals(splits.map(_.count()).sum, 100L)
    }
  }

  test("transpose (Spark 4.0+)") {
    withSpark { s =>
      whenServerAtLeast(4, 0) {
        import s.implicits.*
        val df = Seq(("m", 1, 2)).toDF("k", "a", "b")
        assert(df.transpose().count() >= 1L)
      }
    }
  }

  test("local checkpoint (Spark 4.0+)") {
    withSpark { s =>
      whenServerAtLeast(4, 0) {
        assertEquals(s.range(0, 25).localCheckpoint().count(), 25L)
      }
    }
  }

  test("I/O: parquet, csv, json roundtrip and saveAsTable") {
    withSpark { s =>
      import s.implicits.*
      val base = s"${System.getProperty("java.io.tmpdir")}/sc3-cov-${java.util.UUID.randomUUID()}"
      s.range(0, 5).write.mode("overwrite").parquet(s"$base-pq")
      assertEquals(s.read.parquet(s"$base-pq").count(), 5L)
      Seq((1, "a"), (2, "b"))
        .toDF("id", "name")
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$base-csv")
      assertEquals(s.read.option("header", "true").csv(s"$base-csv").count(), 2L)
      s.range(0, 3).write.mode("overwrite").json(s"$base-json")
      assertEquals(s.read.json(s"$base-json").count(), 3L)
      val table = s"sc3_cov_${System.nanoTime()}"
      try {
        s.range(0, 4).write.mode("overwrite").saveAsTable(table)
        assertEquals(s.table(table).count(), 4L)
      } finally s.sql(s"DROP TABLE IF EXISTS $table").collect()
    }
  }
}
