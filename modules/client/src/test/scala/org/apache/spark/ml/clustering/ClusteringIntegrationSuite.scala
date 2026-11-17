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

import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.MlRemoteSuite

class ClusteringIntegrationSuite extends MlRemoteSuite {

  test("KMeans separates two blobs and reports a training summary") {
    withSpark { spark =>
      val df = MlTestData.blobs(spark)
      val model = new KMeans().setK(2).setMaxIter(10).setSeed(1L).fit(df)
      assertEquals(model.clusterCenters.length, 2)
      val near = model.predict(Vectors.dense(0.0, 0.0))
      val far = model.predict(Vectors.dense(100.0, 0.0))
      assert(near != far)
      assertEquals(model.numFeatures, 2)
      assertEquals(model.hasSummary, true)
      assertEquals(model.summary.k, 2)
      assertEquals(model.summary.clusterSizes.sum, 24L)
      assert(model.summary.trainingCost >= 0.0)
      model.release()
    }
  }

  test("BisectingKMeans fits two clusters and computes a cost") {
    withSpark { spark =>
      val df = MlTestData.blobs(spark)
      val model = new BisectingKMeans().setK(2).setSeed(1L).fit(df)
      assertEquals(model.clusterCenters.length, 2)
      assert(model.computeCost(df) >= 0.0)
      model.release()
    }
  }

  test("GaussianMixture returns weights and per-component probabilities") {
    withSpark { spark =>
      val df = MlTestData.blobs(spark)
      val model = new GaussianMixture().setK(2).setMaxIter(5).setSeed(1L).fit(df)
      assertEquals(model.weights.length, 2)
      assert(math.abs(model.weights.sum - 1.0) < 1e-6)
      assertEquals(model.predictProbability(Vectors.dense(0.0, 0.0)).size, 2)
      assertEquals(model.gaussiansDF.count(), 2L)
      model.release()
    }
  }
}
