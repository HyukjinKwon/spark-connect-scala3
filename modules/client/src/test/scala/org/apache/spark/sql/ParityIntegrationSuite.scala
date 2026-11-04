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

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalog.{Table, TablePartition}
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** A case class for the typed parity integration tests. */
case class ItPair(id: Long, name: String)

/**
 * Live coverage of the APIs added for Apache Spark 4.2 / 4.3 parity. Every feature is gated on the
 * server version that introduced it, so the suite stays green across Spark 3.5 - 4.3.
 */
class ParityIntegrationSuite extends RemoteSparkSuite {

  test("range returns a typed Dataset and typed operations keep their element type") {
    withSpark { s =>
      val ids: Array[java.lang.Long] = s.range(0, 6).filter(col("id") % 2 === 0).collect()
      assertEquals(ids.map(_.longValue).toSeq, Seq(0L, 2L, 4L))
      val pairs = s.createDataset(Seq(ItPair(1L, "a"), ItPair(2L, "b")))
      val kept: Array[ItPair] = pairs.where(col("id") > 1).union(pairs.limit(0)).collect()
      assertEquals(kept.toSeq, Seq(ItPair(2L, "b")))
      val tail: Array[ItPair] = pairs.orderBy("id").tail(1)
      assertEquals(tail.toSeq, Seq(ItPair(2L, "b")))
    }
  }

  test("typed select and joinWith decode tuples") {
    withSpark { s =>
      val df = s.range(3).toDF()
      val typed: Array[(Long, String)] =
        df.select(col("id").as[Long], col("id").cast("string").as[String]).orderBy("id").collect()
      assertEquals(typed.toSeq, Seq(0L -> "0", 1L -> "1", 2L -> "2"))
      val left = s.createDataset(Seq(ItPair(1L, "a"), ItPair(5L, "z")))
      val right = s.range(3)
      val joined = left.joinWith(right, left("id") === right("id"), "left").collect()
      assertEquals(
        joined.map { case (p, r) => (p.id, Option(r).map(_.longValue)) }.toSet,
        Set(1L -> Some(1L), 5L -> None)
      )
    }
  }

  test("spark.sql runs commands eagerly, like Apache Spark") {
    withSpark { s =>
      val view = s"sc3_eager_${System.nanoTime()}"
      s.sql(s"CREATE OR REPLACE TEMPORARY VIEW $view AS SELECT 1 AS a") // no action needed
      try {
        assert(s.catalog.tableExists(view))
        assertEquals(s.sql(s"SELECT * FROM $view WHERE a = ?", Array(1)).count(), 1L)
        assertEquals(s.sql(s"SELECT * FROM $view WHERE a = :v", Map("v" -> 2)).count(), 0L)
      } finally s.catalog.dropTempView(view)
    }
  }

  test("catalog listings are typed and createTable runs eagerly") {
    withSpark { s =>
      val name = s"sc3_ct_${System.nanoTime()}"
      val schema = new StructType().add("id", LongType)
      val created = s.catalog.createTable(name, "parquet", schema, Map.empty[String, String])
      try {
        assert(s.catalog.tableExists(name))
        assertEquals(created.columns.toSeq, Seq("id"))
        val tables: Array[Table] = s.catalog.listTables().collect()
        assert(tables.exists(_.name == name), tables.map(_.name).mkString(","))
        val cols = s.catalog.listColumns(name).collect()
        assertEquals(cols.map(c => (c.name, c.dataType)).toSeq, Seq("id" -> "bigint"))
        assert(s.catalog.listDatabases().collect().exists(_.name == "default"))
        assert(s.catalog.listFunctions().collect().exists(_.name == "abs"))
        assert(s.catalog.listCatalogs().collect().nonEmpty)
        assert(s.catalog.currentCatalog().nonEmpty)
      } finally s.sql(s"DROP TABLE IF EXISTS $name")
    }
  }

  test("Spark 4.2 catalog operations") {
    whenServerAtLeast(4, 2) {
      withSpark { s =>
        val db = s"sc3_db_${System.nanoTime()}"
        s.catalog.createDatabase(db, ifNotExists = false, Map("comment" -> "parity"))
        try {
          assert(s.catalog.databaseExists(db))
          assert(s.catalog.listDatabases(db).collect().exists(_.name == db))
          s.sql(s"CREATE TABLE $db.t (a INT, p INT) USING parquet PARTITIONED BY (p)")
          s.sql(s"INSERT INTO $db.t VALUES (1, 10), (2, 20)")
          val parts: Array[TablePartition] = s.catalog.listPartitions(s"$db.t").collect()
          assertEquals(parts.map(_.partition).toSet, Set("p=10", "p=20"))
          assert(s.catalog.getCreateTableString(s"$db.t").contains("CREATE TABLE"))
          assert(s.catalog.getTableProperties(s"$db.t").isInstanceOf[java.util.Map[?, ?]])
          s.catalog.analyzeTable(s"$db.t", noScan = true)
          s.catalog.truncateTable(s"$db.t")
          assertEquals(s.table(s"$db.t").count(), 0L)
          s.sql(s"CREATE VIEW $db.v AS SELECT * FROM $db.t")
          assertEquals(s.catalog.listViews(db).collect().map(_.name).toSeq, Seq("v"))
          assertEquals(s.catalog.listViews(db, "nope*").count(), 0L)
          s.catalog.dropView(s"$db.v")
          s.catalog.dropTable(s"$db.t")
          s.catalog.dropTable(s"$db.t", ifExists = true)
          assert(!s.catalog.tableExists(s"$db.t"))
        } finally s.catalog.dropDatabase(db, ifExists = true, cascade = true)
        assert(!s.catalog.databaseExists(db))
      }
    }
  }

  test("zipWithIndex and nearestByJoin (Spark 4.2+)") {
    whenServerAtLeast(4, 2) {
      withSpark { s =>
        val indexed = s.range(10, 13).zipWithIndex("i").orderBy("i").collect()
        assertEquals(
          indexed.map(r => (r.getLong(0), r.getLong(1))).toSeq,
          Seq(10L -> 0L, 11L -> 1L, 12L -> 2L)
        )
        val l = s.range(3).toDF()
        val r = s.range(0, 30, 10).toDF()
        val nearest = l.nearestByJoin(r, abs(l("id") - r("id")), 1, "exact", "distance")
        assertEquals(nearest.count(), 3L)
      }
    }
  }

  test("zip combines projections of the same source (Spark 4.3+)") {
    whenServerAtLeast(4, 3) {
      withSpark { s =>
        val base = s.range(3)
        val zipped = base.select(col("id")).zip(base.select((col("id") * 10).as("t")))
        assertEquals(
          zipped.orderBy("id").collect().map(r => (r.getLong(0), r.getLong(1))).toSeq,
          Seq(0L -> 0L, 1L -> 10L, 2L -> 20L)
        )
      }
    }
  }

  test("scalar, exists, and IN subqueries (Spark 4.0+)") {
    whenServerAtLeast(4, 0) {
      withSpark { s =>
        val t = s.range(10)
        val maxSmall = s.range(3).select(max(col("id")))
        assertEquals(t.where(col("id") <= maxSmall.scalar()).count(), 3L)
        assertEquals(t.where(s.range(1).exists()).count(), 10L)
        assertEquals(t.where(col("id").isin(s.range(2).toDF())).count(), 2L)
      }
    }
  }

  test("tags, interrupts, operation ids, and operation statuses") {
    withSpark { s =>
      s.addTag("sc3_tag")
      try {
        assertEquals(s.range(3).count(), 3L)
        assertEquals(s.interruptTag("sc3_tag"), Seq.empty)
      } finally s.clearTags()
      val error = intercept[AnalysisException](s.sql("SELECT no_such_column").collect())
      assert(SparkConnectClient.getOperationId(error).isDefined)
      assertEquals(s.interruptOperation(java.util.UUID.randomUUID().toString), Seq.empty)
      whenServerAtLeast(4, 2) {
        val statuses = s.client.getOperationStatuses()
        assertEquals(statuses.sessionId, s.sessionId)
      }
    }
  }

  test("XML from a Dataset[String] (Spark 4.2+) and textFile") {
    withSpark { s =>
      val dir = Files.createTempDirectory("sc3-text")
      val file = dir.resolve("lines.txt")
      Files.write(file, Seq("a", "b").asJava)
      val lines: Array[String] = s.read.textFile(file.toString).collect()
      assertEquals(lines.toSeq.sorted, Seq("a", "b"))
      whenServerAtLeast(4, 2) {
        val xml = s.createDataset(Seq("<row><a>1</a></row>", "<row><a>2</a></row>"))
        val parsed = s.read.option("rowTag", "row").schema("a INT").xml(xml)
        assertEquals(parsed.collect().map(_.getInt(0)).toSeq.sorted, Seq(1, 2))
      }
    }
  }

  test("cloneSession copies session state (Spark 4.1+)") {
    whenServerAtLeast(4, 1) {
      withSpark { s =>
        val session = s.newSession()
        try {
          session.conf.set("spark.sql.shuffle.partitions", "7")
          val clone = session.cloneSession()
          try assertEquals(clone.conf.get("spark.sql.shuffle.partitions"), "7")
          finally clone.close()
        } finally session.close()
      }
    }
  }

  test("table-valued functions and empty datasets") {
    withSpark { s =>
      val schema = new StructType().add("a", IntegerType)
      assertEquals(s.createDataFrame(Seq.empty[Row], schema).columns.toSeq, Seq("a"))
      assertEquals(s.emptyDataset[ItPair].count(), 0L)
      whenServerAtLeast(4, 0) {
        assertEquals(s.tvf.explode(array(lit(1), lit(2))).count(), 2L)
      }
    }
  }

  test("functions added in Spark 4.2 / 4.3 evaluate on the server") {
    withSpark { s =>
      whenServerAtLeast(4, 3) {
        val row = s
          .range(1)
          .select(
            collect_union(array(lit(1), lit(2))).as("u"),
            is_valid_variant(parse_json(lit("""{"a":1}"""))).as("v"),
            instr(lit("banana"), lit("an"), 3, 1).as("i")
          )
          .collect()
          .head
        assertEquals(row.getSeq[Int](0).sorted, Seq(1, 2))
        assertEquals(row.getBoolean(1), true)
        assertEquals(row.getInt(2), 4)
      }
      whenServerAtLeast(4, 2) {
        val k = s.range(5).select(max_by(col("id"), col("id"), 2).as("top")).collect().head
        assertEquals(k.getSeq[Long](0).sorted, Seq(3L, 4L))
      }
    }
  }

  test("TIME values decode as java.time.LocalTime (Spark 4.1+)") {
    whenServerAtLeast(4, 1) {
      withSpark { s =>
        val session = s.newSession()
        try {
          session.conf.set("spark.sql.timeType.enabled", "true")
          val df = session.sql("SELECT TIME'12:34:56' AS t")
          assertEquals(df.schema.head.dataType, TimeType(6))
          assertEquals(df.collect().head.get(0), java.time.LocalTime.of(12, 34, 56))
        } finally session.close()
      }
    }
  }

  test("Auto CDC flows are accepted by the pipeline graph (Spark 4.2+)") {
    whenServerAtLeast(4, 2) {
      withSpark { s =>
        val pipe = s.pipeline()
        try {
          intercept[IllegalArgumentException](
            pipe.createAutoCdcFlow(
              "t",
              "src",
              keys = Seq(col("k")),
              sequenceBy = col("ts"),
              columnList = Seq(col("a")),
              exceptColumnList = Seq(col("b"))
            )
          )
          pipe.createTemporaryView(
            "sc3_cdc_src",
            Some(s.readStream.format("rate").load().select(col("value").as("k"), col("timestamp")))
          )
          pipe.createTable("sc3_cdc_target")
          val flow = pipe.createAutoCdcFlow(
            "sc3_cdc_target",
            "sc3_cdc_src",
            keys = Seq(col("k")),
            sequenceBy = col("timestamp"),
            storedAsScdType = Some(1)
          )
          assert(flow.endsWith("sc3_cdc_target"), flow)
        } finally pipe.drop()
      }
    }
  }
}
