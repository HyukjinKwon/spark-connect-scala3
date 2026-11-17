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
package org.apache.spark.ml.evaluation

import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.MlRemoteSuite

class EvaluatorIntegrationSuite extends MlRemoteSuite {

  test("RegressionEvaluator reports rmse, r2 and direction") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val model = new LinearRegression().setMaxIter(50).fit(df)
      val predictions = model.transform(df)
      val rmse = new RegressionEvaluator().setMetricName("rmse").evaluate(predictions)
      assert(rmse >= 0.0)
      val r2 = new RegressionEvaluator().setMetricName("r2").setLabelCol("label")
      assert(!r2.isLargerBetter == false)
      assert(r2.evaluate(predictions) <= 1.0)
      assertEquals(new RegressionEvaluator().isLargerBetter, false)
      model.release()
    }
  }

  test("BinaryClassificationEvaluator and MulticlassClassificationEvaluator score predictions") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LogisticRegression().setMaxIter(10).fit(df)
      val predictions = model.transform(df)
      val auc =
        new BinaryClassificationEvaluator().setMetricName("areaUnderROC").evaluate(predictions)
      assert(auc >= 0.0 && auc <= 1.0)
      val accuracy =
        new MulticlassClassificationEvaluator().setMetricName("accuracy").evaluate(predictions)
      assert(accuracy >= 0.0 && accuracy <= 1.0)
      val f1 = new MulticlassClassificationEvaluator().evaluate(predictions)
      assert(f1 >= 0.0 && f1 <= 1.0)
      model.release()
    }
  }

  test("ClusteringEvaluator scores a silhouette") {
    withSpark { spark =>
      val df = MlTestData.blobs(spark)
      val model = new KMeans().setK(2).setSeed(1L).fit(df)
      val silhouette = new ClusteringEvaluator().evaluate(model.transform(df))
      assert(silhouette > 0.5)
      model.release()
    }
  }

  test("RankingEvaluator and MultilabelClassificationEvaluator score array columns") {
    withSpark { spark =>
      val ranking = spark.sql(
        "SELECT array(CAST(1 AS DOUBLE), CAST(2 AS DOUBLE), CAST(3 AS DOUBLE)) AS prediction, array(CAST(1 AS DOUBLE), CAST(3 AS DOUBLE)) AS label"
      )
      val map = new RankingEvaluator().setMetricName("meanAveragePrecision").evaluate(ranking)
      assert(map >= 0.0 && map <= 1.0)
      val multilabel = spark.sql(
        "SELECT array(CAST(0 AS DOUBLE), CAST(1 AS DOUBLE)) AS prediction, array(CAST(0 AS DOUBLE), CAST(1 AS DOUBLE)) AS label"
      )
      val f1 =
        new MultilabelClassificationEvaluator().setMetricName("f1Measure").evaluate(multilabel)
      assert(math.abs(f1 - 1.0) < 1e-9)
    }
  }
}
