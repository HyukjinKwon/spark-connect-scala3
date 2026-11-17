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
package org.apache.spark.ml.regression

import org.apache.spark.ml.MlRemoteSuite
import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.linalg.Vectors

class RegressionFamiliesIntegrationSuite extends MlRemoteSuite {

  test("FMRegressor fits factors") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val model = new FMRegressor().setMaxIter(5).setFactorSize(2).setSeed(1L).fit(df)
      assertEquals(model.factors.numRows, 2)
      assertEquals(model.transform(df).count(), 30L)
      model.release()
    }
  }

  test("AFTSurvivalRegression fits coefficients and predicts quantiles") {
    withSpark { spark =>
      val df = spark
        .sql(
          "SELECT CAST(id + 1 AS DOUBLE) AS label, CAST(id % 2 AS DOUBLE) AS censor, " +
            "CAST(id AS DOUBLE) AS x FROM range(20)"
        )
        .select(
          org.apache.spark.sql.functions.col("label"),
          org.apache.spark.sql.functions.col("censor"),
          org.apache.spark.ml.functions
            .array_to_vector(
              org.apache.spark.sql.functions.array(org.apache.spark.sql.functions.col("x"))
            )
            .as("features")
        )
      val model = new AFTSurvivalRegression().setMaxIter(10).fit(df)
      assertEquals(model.coefficients.size, 1)
      assert(model.scale > 0.0)
      assertEquals(model.predictQuantiles(Vectors.dense(3.0)).size, 9)
      model.release()
    }
  }

  test("IsotonicRegression fits boundaries and predicts") {
    withSpark { spark =>
      val df = spark
        .sql("SELECT CAST(id AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x FROM range(10)")
        .select(
          org.apache.spark.sql.functions.col("label"),
          org.apache.spark.ml.functions
            .array_to_vector(
              org.apache.spark.sql.functions.array(org.apache.spark.sql.functions.col("x"))
            )
            .as("features")
        )
      val model = new IsotonicRegression().setFeaturesCol("features").fit(df)
      assert(model.boundaries.size > 0)
      assertEquals(model.predict(3.0), 3.0, 1e-9)
      model.release()
    }
  }

  test("GeneralizedLinearRegression fits, summarises and evaluates") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val model = new GeneralizedLinearRegression().setMaxIter(5).fit(df)
      assertEquals(model.coefficients.size, 2)
      assert(model.hasSummary)
      assert(model.summary.deviance >= 0.0)
      assertEquals(model.summary.predictions.count(), 30L)
      assertEquals(model.evaluate(df).numInstances, 30L)
      model.release()
    }
  }

  test("LinearRegression evaluate returns a regression summary") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val model = new LinearRegression().setMaxIter(20).fit(df)
      assert(model.evaluate(df).rootMeanSquaredError >= 0.0)
      model.release()
    }
  }

  test("GeneralizedLinearRegression defaults match Spark") {
    val glr = new GeneralizedLinearRegression()
    assertEquals(glr.getFamily, "gaussian")
    assertEquals(glr.getSolver, "irls")
    assertEquals(glr.getMaxIter, 25)
  }
}
