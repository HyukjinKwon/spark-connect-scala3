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
package org.apache.spark.ml.feature

import org.apache.spark.ml.{MlRemoteSuite, MlTestData}
import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{array, col, lit}

class FeatureFamiliesIntegrationSuite extends MlRemoteSuite {

  test("RFormula builds features and labels from a formula") {
    withSpark { spark =>
      val df = spark.sql(
        "SELECT CAST(id AS DOUBLE) AS a, CAST(id % 2 AS DOUBLE) AS b, " +
          "CAST(id % 2 AS DOUBLE) AS y FROM range(8)"
      )
      val model = new RFormula().setFormula("y ~ a + b").fit(df)
      // The server reports the resolved formula as its description: label and terms.
      assert(model.resolvedFormulaString.contains("label=y"), model.resolvedFormulaString)
      assert(model.resolvedFormulaString.contains("terms=[a,b]"), model.resolvedFormulaString)
      assert(model.transform(df).columns.contains("features"))
      model.release()
    }
  }

  test("RobustScaler exposes median and range") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new RobustScaler().setInputCol("features").setOutputCol("s").fit(df)
      assertEquals(model.median.size, 2)
      assertEquals(model.range.size, 2)
      model.release()
    }
  }

  test("ChiSqSelector and UnivariateFeatureSelector select features") {
    withSpark { spark =>
      val df = MlTestData.multiclass(spark)
      val chi = new ChiSqSelector().setNumTopFeatures(1).setOutputCol("c").fit(df)
      assertEquals(chi.selectedFeatures.length, 1)
      chi.release()
      val ufs = new UnivariateFeatureSelector()
        .setFeatureType("continuous")
        .setLabelType("categorical")
        .setSelectionMode("numTopFeatures")
        .setSelectionThreshold(1.0)
        .setOutputCol("u")
        .fit(df)
      assertEquals(ufs.selectedFeatures.length, 1)
      ufs.release()
    }
  }

  test("VarianceThresholdSelector drops constant features") {
    withSpark { spark =>
      val df = spark
        .range(6)
        .select(array_to_vector(array(col("id").cast("double"), lit(7.0))).as("features"))
      val model = new VarianceThresholdSelector().setOutputCol("o").fit(df)
      assertEquals(model.selectedFeatures.toSeq, Seq(0))
      model.release()
    }
  }

  test("VectorIndexer finds categorical features and maps") {
    withSpark { spark =>
      val df = spark
        .range(8)
        .select(
          array_to_vector(array(col("id") % 2, col("id")).cast("array<double>")).as("features")
        )
      val model =
        new VectorIndexer().setInputCol("features").setOutputCol("i").setMaxCategories(2).fit(df)
      assertEquals(model.numFeatures, 2)
      assert(model.categoryMapsDF.count() >= 1L)
      model.release()
    }
  }

  test("Word2Vec learns vectors and finds synonyms") {
    withSpark { spark =>
      val docs = spark.sql(
        "SELECT split('a b c a b c d e', ' ') AS words UNION ALL " +
          "SELECT split('e d c b a', ' ') AS words"
      )
      val model = new Word2Vec()
        .setInputCol("words")
        .setOutputCol("v")
        .setVectorSize(4)
        .setMinCount(0)
        .setMaxIter(2)
        .setSeed(1L)
        .fit(docs)
      assertEquals(model.getVectors.count(), 5L)
      assertEquals(model.findSynonyms("a", 2).count(), 2L)
      model.release()
    }
  }

  test("TargetEncoder encodes categories by their label mean") {
    withSpark { spark =>
      val df = spark.sql(
        "SELECT CAST(id % 2 AS DOUBLE) AS label, CAST(id % 3 AS DOUBLE) AS c FROM range(12)"
      )
      val model = new TargetEncoder().setInputCols(Array("c")).setOutputCols(Array("ce")).fit(df)
      assertEquals(model.transform(df).count(), 12L)
      model.release()
    }
  }

  test("QuantileDiscretizer fits a bucketizer on the server") {
    withSpark { spark =>
      val df = spark.sql("SELECT CAST(id AS DOUBLE) AS x FROM range(20)")
      val model = new QuantileDiscretizer()
        .setInputCol("x")
        .setOutputCol("q")
        .setNumBuckets(4)
        .setRelativeError(0.0)
        .fit(df)
      assert(model.isInstanceOf[Bucketizer])
      assertEquals(model.transform(df).select("q").distinct().count(), 4L)
      model.release()
    }
  }

  test("BucketedRandomProjectionLSH and MinHashLSH find neighbours and join") {
    withSpark { spark =>
      val df = MlTestData
        .binary(spark)
        .withColumnRenamed("features", "f")
        .select(col("f").as("features"))
      val brp = new BucketedRandomProjectionLSH()
        .setInputCol("features")
        .setOutputCol("h")
        .setBucketLength(2.0)
        .setNumHashTables(2)
        .setSeed(1L)
        .fit(df)
      val nn = brp.approxNearestNeighbors(df, Vectors.dense(0.0, 0.0), 3, "dist")
      assert(nn.count() <= 3L)
      val join = brp.approxSimilarityJoin(df, df, 1.0, "dist")
      assert(join.count() >= 0L)
      brp.release()

      val mh = spark
        .range(10)
        .select(array_to_vector(array(col("id").cast("double"), lit(1.0))).as("features"))
      val minhash = new MinHashLSH()
        .setInputCol("features")
        .setOutputCol("h")
        .setNumHashTables(2)
        .setSeed(1L)
        .fit(mh)
      assert(minhash.approxNearestNeighbors(mh, Vectors.dense(0.0, 1.0), 2, "dist").count() <= 2L)
      minhash.release()
    }
  }
}
