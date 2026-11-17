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
package org.apache.spark.ml.classification

import org.apache.spark.ml.{MlRemoteSuite, MlTestData}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

class ClassifierFamiliesIntegrationSuite extends MlRemoteSuite {

  test("FMClassifier fits factors and evaluates") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new FMClassifier().setMaxIter(5).setFactorSize(2).setSeed(1L).fit(df)
      assertEquals(model.factors.numRows, 2)
      assertEquals(model.linear.size, 2)
      assert(model.evaluate(df).accuracy >= 0.0)
      model.release()
    }
  }

  test("MultilayerPerceptronClassifier fits weights and evaluates") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new MultilayerPerceptronClassifier()
        .setLayers(Array(2, 4, 2))
        .setMaxIter(5)
        .setSeed(1L)
        .fit(df)
      assert(model.weights.size > 0)
      assert(model.evaluate(df).accuracy >= 0.0)
      model.release()
    }
  }

  test("LinearSVC evaluate returns a classification summary") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LinearSVC().setMaxIter(5).fit(df)
      assert(model.evaluate(df).weightedFMeasure >= 0.0)
      model.release()
    }
  }

  test("OneVsRest reduces a multiclass problem to one binary model per class") {
    withSpark { spark =>
      val df = MlTestData.multiclass(spark)
      val ovr = new OneVsRest().setClassifier(new LogisticRegression().setMaxIter(5))
      val model = ovr.fit(df)
      assertEquals(model.numClasses, 3)
      assertEquals(model.models.length, 3)
      val predictions = model.transform(df)
      assert(predictions.columns.contains("prediction"))
      val acc =
        new MulticlassClassificationEvaluator().setMetricName("accuracy").evaluate(predictions)
      assert(acc >= 0.0 && acc <= 1.0)
      model.release()
    }
  }

  test("OneVsRest uses the classifier's params and defaults") {
    val ovr = new OneVsRest()
    assertEquals(ovr.getParallelism, 1)
    assertEquals(ovr.getFeaturesCol, "features")
  }
}
