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

import org.apache.spark.connect.proto
import org.apache.spark.sql.functions._

/**
 * Unit tests for the v2 writer (`df.writeTo(...)`) command wiring. Builds the `WriteOperationV2`
 * protobuf offline (no server, no RPC) and asserts every field, so the writer is verified even
 * though end-to-end append/overwrite need a v2 catalog the test server does not provide.
 */
class DataFrameWriterV2Suite extends munit.FunSuite {

  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    try body(spark)
    finally spark.stop()
  }

  test("builds a WriteOperationV2 carrying every configured option") {
    withSession { spark =>
      val writer = spark
        .range(5)
        .writeTo("cat.db.tbl")
        .using("parquet")
        .option("path", "/tmp/x")
        .options(Map("compression" -> "snappy"))
        .tableProperty("owner", "spark")
        .partitionedBy(col("id"))
        .clusterBy("id")
      val op = writer.buildWriteOperation(proto.WriteOperationV2.Mode.MODE_CREATE)

      assertEquals(op.tableName, "cat.db.tbl")
      assertEquals(op.provider, Some("parquet"))
      assertEquals(op.options("path"), "/tmp/x")
      assertEquals(op.options("compression"), "snappy")
      assertEquals(op.tableProperties("owner"), "spark")
      assertEquals(op.partitioningColumns.size, 1)
      assertEquals(op.clusteringColumns, Seq("id"))
      assertEquals(op.mode, proto.WriteOperationV2.Mode.MODE_CREATE)
      assert(op.input.isDefined, "the source relation must be attached")
      assert(op.overwriteCondition.isEmpty)
    }
  }

  test("each mode is carried through, and overwrite attaches its condition") {
    withSession { spark =>
      val w = spark.range(1).writeTo("t")
      assertEquals(
        w.buildWriteOperation(proto.WriteOperationV2.Mode.MODE_APPEND).mode,
        proto.WriteOperationV2.Mode.MODE_APPEND
      )
      assertEquals(
        w.buildWriteOperation(proto.WriteOperationV2.Mode.MODE_REPLACE).mode,
        proto.WriteOperationV2.Mode.MODE_REPLACE
      )
      assertEquals(
        w.buildWriteOperation(proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE).mode,
        proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE
      )
      assertEquals(
        w.buildWriteOperation(proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS).mode,
        proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS
      )

      val overwrite = w.buildWriteOperation(
        proto.WriteOperationV2.Mode.MODE_OVERWRITE,
        Some(col("id").>(lit(0)).expr)
      )
      assertEquals(overwrite.mode, proto.WriteOperationV2.Mode.MODE_OVERWRITE)
      assert(overwrite.overwriteCondition.isDefined, "overwrite must carry its filter condition")
    }
  }

  test("defaults: no provider, empty options/properties/partitioning/clustering") {
    withSession { spark =>
      val op =
        spark.range(1).writeTo("t").buildWriteOperation(proto.WriteOperationV2.Mode.MODE_CREATE)
      assertEquals(op.provider, None)
      assert(op.options.isEmpty)
      assert(op.tableProperties.isEmpty)
      assert(op.partitioningColumns.isEmpty)
      assert(op.clusteringColumns.isEmpty)
    }
  }
}
