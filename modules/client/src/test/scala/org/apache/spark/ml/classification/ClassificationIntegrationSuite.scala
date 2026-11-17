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

import org.apache.spark.ml.{MlTestData, PipelineStage}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.MlRemoteSuite

class ClassificationIntegrationSuite extends MlRemoteSuite {

  test("LogisticRegression: fit, attributes, transform, and summary") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LogisticRegression().setMaxIter(10).setRegParam(0.01).fit(df)
      assertEquals(model.numClasses, 2)
      assertEquals(model.numFeatures, 2)
      assertEquals(model.getMaxIter, 10)
      assertEquals(model.coefficients.size, 2)
      val predictions = model.transform(df)
      assert(predictions.columns.contains("prediction"))
      assertEquals(predictions.count(), 40L)
      assert(model.summary.objectiveHistory.nonEmpty)
      assert(model.summary.accuracy >= 0.0 && model.summary.accuracy <= 1.0)
      assert(model.binarySummary.areaUnderROC >= 0.5)
      model.release()
    }
  }

  test("LogisticRegression: predict on a local vector") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LogisticRegression().setMaxIter(10).fit(df)
      val probability = model.predictProbability(org.apache.spark.ml.linalg.Vectors.dense(0.0, 0.0))
      assertEquals(probability.size, 2)
      assert(math.abs(probability.toArray.sum - 1.0) < 1e-6)
      model.release()
    }
  }

  test("LinearSVC: fit and read coefficients") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LinearSVC().setMaxIter(10).setThreshold(0.0).fit(df)
      assertEquals(model.coefficients.size, 2)
      assert(model.transform(df).columns.contains("prediction"))
      model.release()
    }
  }

  test("NaiveBayes: fit, theta and pi attributes") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new NaiveBayes().setModelType("gaussian").fit(df)
      assertEquals(model.pi.size, 2)
      assertEquals(model.theta.numRows, 2)
      assertEquals(model.theta.numCols, 2)
      assert(model.transform(df).count() == 40L)
      model.release()
    }
  }

  test("LogisticRegression on multiclass data") {
    withSpark { spark =>
      val model = new LogisticRegression()
        .setMaxIter(5)
        .setFamily("multinomial")
        .fit(MlTestData.multiclass(spark))
      assertEquals(model.numClasses, 3)
      assertEquals(model.coefficientMatrix.numRows, 3)
      assertEquals(model.interceptVector.size, 3)
      model.release()
    }
  }

  test("Estimator and model params are readable and copyable") {
    withSpark { _ =>
      val lr = new LogisticRegression().setRegParam(0.5).setMaxIter(7)
      assertEquals(lr.getRegParam, 0.5)
      assertEquals(lr.getElasticNetParam, 0.0)
      val copied = lr.copy(org.apache.spark.ml.param.ParamMap(lr.maxIter -> 3))
      assertEquals(copied.getMaxIter, 3)
      assertEquals(lr.getMaxIter, 7)
      assertEquals(copied.uid, lr.uid)
      val stages: Seq[PipelineStage] = Seq(lr)
      assertEquals(stages.head.uid, lr.uid)
    }
  }

  test("Vector results decode from Row values") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val first: Vector =
        org.apache.spark.ml.linalg.VectorUDT.deserialize(df.select("features").head().getStruct(0))
      assertEquals(first.size, 2)
    }
  }
}
