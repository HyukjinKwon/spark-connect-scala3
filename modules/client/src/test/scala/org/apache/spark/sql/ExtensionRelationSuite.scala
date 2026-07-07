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

import com.google.protobuf.any.{Any => ProtoAny}

import org.apache.spark.connect.proto

/**
 * Unit tests for the Spark Connect plugin extension point (issue #7). These build extension
 * relations WITHOUT a server, mirroring the PySpark client's `plan.extension.Pack(msg)` +
 * `DataFrame(plan, session)` flow.
 *
 * We stand in for a plugin's own generated message with [[proto.Range]] (any ScalaPB
 * `GeneratedMessage` can be packed); a real plugin would pack its own protobuf type that the
 * server-side `RelationPlugin` knows how to unpack.
 */
class ExtensionRelationSuite extends munit.FunSuite {

  /** Builds a fresh, non-connecting session and stops it after the body runs. */
  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    try body(spark)
    finally spark.stop()
  }

  test("newDataFrame(extension) wraps a packed message in an Extension relation") {
    withSession { spark =>
      val payload = proto.Range(start = Some(0L), end = 10L, step = 1L)
      val df = spark.newDataFrame(ProtoAny.pack(payload))

      assert(df.relation.relType.isExtension, "relType should be Extension")
      val any = df.relation.getExtension
      assert(any.is(proto.Range), "packed Any should carry the plugin message type")
      assertEquals(any.unpack(proto.Range), payload)
    }
  }

  test("extension relations get a unique plan id, like every other relation") {
    withSession { spark =>
      val df1 = spark.newDataFrame(ProtoAny.pack(proto.Range(end = 1L)))
      val df2 = spark.newDataFrame(ProtoAny.pack(proto.Range(end = 2L)))

      val id1 = df1.relation.common.flatMap(_.planId)
      val id2 = df2.relation.common.flatMap(_.planId)
      assert(id1.isDefined && id2.isDefined, "extension relations must carry a plan id")
      assertNotEquals(id1, id2, "each relation must get a distinct plan id")
    }
  }

  test("newDataFrame(relType) accepts an arbitrary Extension RelType") {
    withSession { spark =>
      val any = ProtoAny.pack(proto.Range(end = 3L))
      val df = spark.newDataFrame(proto.Relation.RelType.Extension(any))
      assert(df.relation.relType.isExtension)
      assertEquals(df.relation.getExtension.unpack(proto.Range).end, 3L)
    }
  }

  test("newRelation tags a RelType with a fresh plan id") {
    withSession { spark =>
      val rel = spark.newRelation(proto.Relation.RelType.Range(proto.Range(end = 5L)))
      assert(rel.common.flatMap(_.planId).isDefined)
      assert(rel.relType.isRange)
    }
  }

  test("plugins can embed an existing DataFrame's relation as a plugin input") {
    withSession { spark =>
      // A plugin message that references an upstream DataFrame -- here we reuse proto.SetOperation
      // purely as a stand-in container that holds two child Relations.
      val vertices = spark.range(4)
      val edges = spark.range(8)
      val pluginMessage = proto.SetOperation(
        leftInput = Some(vertices.relation),
        rightInput = Some(edges.relation)
      )
      val df = spark.newDataFrame(ProtoAny.pack(pluginMessage))

      val unpacked = df.relation.getExtension.unpack(proto.SetOperation)
      assertEquals(unpacked.leftInput.flatMap(_.relType.range).map(_.end), Some(4L))
      assertEquals(unpacked.rightInput.flatMap(_.relType.range).map(_.end), Some(8L))
    }
  }

  test("Dataset.plan exposes the root plan wrapping the relation") {
    withSession { spark =>
      val df = spark.newDataFrame(ProtoAny.pack(proto.Range(end = 7L)))
      val plan = df.plan
      assert(plan.opType.isRoot, "plan should wrap the relation as a Root op")
      assertEquals(plan.getRoot, df.relation)
    }
  }
}
