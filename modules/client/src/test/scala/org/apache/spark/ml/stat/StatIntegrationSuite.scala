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
package org.apache.spark.ml.stat

import org.apache.spark.ml.MlTestData
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.MlRemoteSuite

class StatIntegrationSuite extends MlRemoteSuite {

  test("Correlation returns a correlation matrix for a vector column") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val result = Correlation.corr(df, "features")
      assertEquals(result.count(), 1L)
      val spearman = Correlation.corr(df, "features", "spearman")
      assertEquals(spearman.count(), 1L)
    }
  }

  test("ChiSquareTest reports one row per feature") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      assertEquals(ChiSquareTest.test(df, "features", "label", flatten = true).count(), 2L)
      assertEquals(ChiSquareTest.test(df, "features", "label").count(), 1L)
    }
  }

  test("KolmogorovSmirnovTest tests a numeric column against a normal distribution") {
    withSpark { spark =>
      val df = spark.sql("SELECT CAST(id AS DOUBLE) AS sample FROM range(20)")
      val result = KolmogorovSmirnovTest.test(df, "sample", "norm", 10.0, 6.0)
      assertEquals(result.count(), 1L)
    }
  }

  test("Summarizer computes per-element statistics over a vector column") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val row =
        df.select(Summarizer.metrics("mean", "count").summary(col("features")).as("s")).head()
      // The struct has one field per requested metric, in request order: mean, then count.
      val s = row.getStruct(0)
      assertEquals(s.getLong(1), 40L)
      val mean = org.apache.spark.ml.linalg.VectorUDT.deserialize(s.getStruct(0))
      assertEquals(mean.size, 2)
      assertEquals(df.select(Summarizer.mean(col("features"))).count(), 1L)
    }
  }

  test("vector_to_array and array_to_vector convert between array and vector columns") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val arrays = df.select(org.apache.spark.ml.functions.vector_to_array(col("features")).as("a"))
      assertEquals(arrays.head().getSeq[Double](0).length, 2)
      val back = arrays.select(org.apache.spark.ml.functions.array_to_vector(col("a")).as("v"))
      assertEquals(back.count(), 40L)
    }
  }
}
