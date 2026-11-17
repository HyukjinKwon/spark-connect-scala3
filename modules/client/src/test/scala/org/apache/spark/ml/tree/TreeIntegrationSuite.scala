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
package org.apache.spark.ml.tree

import org.apache.spark.ml.MlRemoteSuite
import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.classification.{
  DecisionTreeClassificationModel,
  DecisionTreeClassifier,
  GBTClassificationModel,
  GBTClassifier,
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{
  DecisionTreeRegressionModel,
  DecisionTreeRegressor,
  GBTRegressionModel,
  GBTRegressor,
  RandomForestRegressionModel,
  RandomForestRegressor
}

class TreeIntegrationSuite extends MlRemoteSuite {

  test("DecisionTreeClassifier: attributes, debug string and leaves") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new DecisionTreeClassifier().setMaxDepth(3).setSeed(1L).fit(df)
      assert(model.numNodes >= 1)
      assert(model.depth <= 3)
      assertEquals(model.featureImportances.size, 2)
      assert(model.toDebugString.nonEmpty)
      assert(model.predictLeaf(Vectors.dense(0.0, 0.0)) >= 0.0)
      assertEquals(model.transform(df).count(), 40L)
      model.release()
    }
  }

  test("RandomForestClassifier: trees, weights, total nodes and evaluate") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new RandomForestClassifier().setNumTrees(3).setMaxDepth(2).setSeed(1L).fit(df)
      assertEquals(model.getNumTrees, 3)
      assertEquals(model.trees.length, 3)
      assert(model.trees.forall(_.isInstanceOf[DecisionTreeClassificationModel]))
      assertEquals(model.treeWeights.length, 3)
      assert(model.totalNumNodes > 0)
      assertEquals(model.featureImportances.size, 2)
      assert(model.evaluate(df).accuracy >= 0.0)
      model.release()
    }
  }

  test("GBTClassifier: trees, weights and per-iteration loss") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new GBTClassifier().setMaxIter(3).setMaxDepth(2).setSeed(1L).fit(df)
      assertEquals(model.trees.length, 3)
      assert(model.trees.forall(_.isInstanceOf[DecisionTreeRegressionModel]))
      assertEquals(model.treeWeights.length, 3)
      assertEquals(model.evaluateEachIteration(df).length, 3)
      model.release()
    }
  }

  test("DecisionTreeRegressor and RandomForestRegressor fit a linear relation") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val tree = new DecisionTreeRegressor().setMaxDepth(4).fit(df)
      assert(tree.transform(df).count() == 30L)
      assert(tree.numNodes >= 1)
      tree.release()
      val forest = new RandomForestRegressor().setNumTrees(2).setMaxDepth(3).setSeed(1L).fit(df)
      assertEquals(forest.trees.length, 2)
      assert(forest.trees.forall(_.isInstanceOf[DecisionTreeRegressionModel]))
      forest.release()
    }
  }

  test("GBTRegressor fits boosted regression trees") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val gbt = new GBTRegressor().setMaxIter(3).setMaxDepth(2).setSeed(1L).fit(df)
      assertEquals(gbt.trees.length, 3)
      assertEquals(gbt.evaluateEachIteration(df).length, 3)
      gbt.release()
    }
  }

  test("tree defaults match Spark") {
    val dt = new DecisionTreeClassifier()
    assertEquals(dt.getMaxDepth, 5)
    assertEquals(dt.getMaxBins, 32)
    assertEquals(dt.getImpurity, "gini")
    assertEquals(new DecisionTreeRegressor().getImpurity, "variance")
    assertEquals(new RandomForestClassifier().getNumTrees, 20)
    assertEquals(new GBTRegressor().getLossType, "squared")
    assertEquals(new GBTClassifier().getStepSize, 0.1)
  }
}
