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
package org.apache.spark.ml.clustering

import org.apache.spark.ml.MlRemoteSuite
import org.apache.spark.ml.MlTestData

class LDAPowerIterationIntegrationSuite extends MlRemoteSuite {

  test("LDA with the online optimizer fits a local model") {
    withSpark { spark =>
      val counts = spark
        .range(12)
        .select(
          org.apache.spark.ml.functions
            .array_to_vector(
              org.apache.spark.sql.functions.array(
                (org.apache.spark.sql.functions.col("id") % 3).cast("double"),
                org.apache.spark.sql.functions.lit(1.0),
                org.apache.spark.sql.functions.lit(2.0)
              )
            )
            .as("features")
        )
      val model = new LDA().setK(2).setMaxIter(3).setSeed(1L).fit(counts)
      assert(model.isInstanceOf[LocalLDAModel])
      assertEquals(model.topicsMatrix.numCols, 2)
      assert(model.vocabSize > 0)
      assert(model.logPerplexity(counts) > -1e9)
      assert(model.describeTopics(2).count() == 2L)
      model.release()
    }
  }

  test("LDA with the em optimizer fits a distributed model that converts to local") {
    withSpark { spark =>
      assume(!mlCacheMemoryControlRejectsFit, mlCacheMemoryControlReason)
      val counts = spark
        .range(12)
        .select(
          org.apache.spark.ml.functions
            .array_to_vector(
              org.apache.spark.sql.functions.array(
                (org.apache.spark.sql.functions.col("id") % 3).cast("double"),
                org.apache.spark.sql.functions.lit(1.0),
                org.apache.spark.sql.functions.lit(2.0)
              )
            )
            .as("features")
        )
      val model = new LDA().setK(2).setMaxIter(2).setOptimizer("em").setSeed(1L).fit(counts)
      assert(model.isInstanceOf[DistributedLDAModel])
      val local = model.asInstanceOf[DistributedLDAModel].toLocal
      assert(local.isInstanceOf[LocalLDAModel])
      model.release()
      local.release()
    }
  }

  test("PowerIterationClustering assigns clusters to the vertices of an edge list") {
    withSpark { spark =>
      val edges = spark.sql(
        "SELECT CAST(id AS BIGINT) AS src, CAST((id + 1) % 4 AS BIGINT) AS dst, 1.0 AS weight FROM range(8)"
      )
      val pic = new PowerIterationClustering().setK(2).setMaxIter(5).setWeightCol("weight")
      val clusters = pic.assignClusters(edges)
      assertEquals(clusters.columns.toSeq, Seq("id", "cluster"))
      assert(clusters.count() > 0L)
    }
  }

  test("LDA and PowerIterationClustering defaults match Spark") {
    assertEquals(new LDA().getK, 10)
    assertEquals(new LDA().getOptimizer, "online")
    assertEquals(new PowerIterationClustering().getInitMode, "random")
  }
}
