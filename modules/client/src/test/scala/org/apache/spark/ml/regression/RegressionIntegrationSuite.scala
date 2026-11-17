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

import org.apache.spark.ml.MlTestData
import org.apache.spark.ml.MlRemoteSuite

class RegressionIntegrationSuite extends MlRemoteSuite {

  test("LinearRegression fits a linear relation and reports a summary") {
    withSpark { spark =>
      val df = MlTestData.linear(spark)
      val model = new LinearRegression().setMaxIter(50).setRegParam(0.0).fit(df)
      assertEquals(model.coefficients.size, 2)
      assertEquals(model.hasSummary, true)
      val summary = model.summary
      assert(summary.r2 > 0.9)
      assert(summary.rootMeanSquaredError >= 0.0)
      assertEquals(summary.predictions.count(), 30L)
      assertEquals(model.transform(df).count(), 30L)
      model.release()
    }
  }

  test("LinearRegression defaults match Spark and loss is validated") {
    val lr = new LinearRegression()
    assertEquals(lr.getSolver, "auto")
    assertEquals(lr.getLoss, "squaredError")
    assertEquals(lr.getEpsilon, 1.35)
    assertEquals(lr.getFeaturesCol, "features")
    assertEquals(lr.getLabelCol, "label")
    intercept[IllegalArgumentException] {
      lr.setLoss("bogus")
    }
  }
}
