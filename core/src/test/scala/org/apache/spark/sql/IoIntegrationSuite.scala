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
 * Live coverage for [[DataFrameReader]] / [[DataFrameWriter]] roundtrips and table I/O. Paths are
 * resolved on the server (the test server runs in local mode), so a unique temp directory is used.
 */
class IoIntegrationSuite extends RemoteSparkSuite {

  private def tmpDir(suffix: String): String =
    s"${System.getProperty("java.io.tmpdir")}/sc3-io-${java.util.UUID.randomUUID()}-$suffix"

  test("parquet write/read roundtrip") {
    withSpark { s =>
      val path = tmpDir("parquet")
      s.range(0, 5).write.mode("overwrite").parquet(path)
      assertEquals(s.read.parquet(path).count(), 5L)
    }
  }

  test("csv write/read roundtrip with header") {
    withSpark { s =>
      import s.implicits.*
      val path = tmpDir("csv")
      Seq((1, "a"), (2, "b"))
        .toDF("id", "name")
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
      val back = s.read.option("header", "true").option("inferSchema", "true").csv(path)
      assertEquals(back.count(), 2L)
      assert(back.columns.toSeq.contains("name"))
    }
  }

  test("json write/read roundtrip") {
    withSpark { s =>
      val path = tmpDir("json")
      s.range(0, 3).write.mode("overwrite").json(path)
      assertEquals(s.read.json(path).count(), 3L)
    }
  }

  test("saveAsTable and read back via spark.table") {
    withSpark { s =>
      val table = s"sc3_io_tbl_${System.nanoTime()}"
      try {
        s.range(0, 4).write.mode("overwrite").saveAsTable(table)
        assert(s.catalog.tableExists(table))
        assertEquals(s.table(table).count(), 4L)
      } finally s.sql(s"DROP TABLE IF EXISTS $table").collect()
    }
  }
}
