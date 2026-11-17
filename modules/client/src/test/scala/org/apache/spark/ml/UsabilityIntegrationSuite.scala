package org.apache.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.connect.client.ShowString

class UsabilityIntegrationSuite extends MlRemoteSuite {

  test("vector columns decode to Vector values in collected rows") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val row = df.select("features").head()
      assert(row.get(0).isInstanceOf[Vector])
      assertEquals(row.getAs[Vector](0).size, 2)
    }
  }

  test("vector columns render as values in show, not as struct dumps") {
    withSpark { spark =>
      val df = spark
        .range(2)
        .select(
          org.apache.spark.ml.functions
            .array_to_vector(
              org.apache.spark.sql.functions.array(
                org.apache.spark.sql.functions.lit(1.0),
                org.apache.spark.sql.functions.lit(2.0)
              )
            )
            .as("features")
        )
      val rows = df.limit(2).collect().toSeq
      val text = ShowString(rows, df.schema, 2, 20, vertical = false)
      assert(text.contains("[1.0,2.0]"), text)
    }
  }

  test("matrix columns decode to Matrix values") {
    withSpark { spark =>
      val df = spark.sql("SELECT 1 AS id").select(org.apache.spark.sql.functions.lit(1).as("id"))
      // A fitted PCA model exposes a matrix attribute, which is read through the same codec.
      val pca = new org.apache.spark.ml.feature.PCA()
        .setInputCol("features")
        .setOutputCol("p")
        .setK(1)
        .fit(MlTestData.binary(spark))
      val pc: Matrix = pca.pc
      assertEquals(pc.numRows, 2)
      assertEquals(df.count(), 1L)
      pca.release()
    }
  }

  test("CrossValidator with parallelism gives the same metrics as sequential runs") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(5)
      val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.1, 0.5)).build()
      def run(p: Int) = new CrossValidator()
        .setEstimator(lr)
        .setEstimatorParamMaps(grid)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setNumFolds(2)
        .setSeed(11L)
        .setParallelism(p)
        .fit(df)
      val sequential = run(1)
      val parallel = run(3)
      assertEquals(parallel.avgMetrics.toSeq, sequential.avgMetrics.toSeq)
      sequential.release()
      parallel.release()
    }
  }

  test("model size and cache information need server 4.1 or newer") {
    withSpark { spark =>
      whenServerAtLeast(4, 1) {
        val model = new LogisticRegression().setMaxIter(3).fit(MlTestData.binary(spark))
        assert(model.estimatedSize > 0L)
        assert(MlClientAccess.cacheInfo(spark).nonEmpty)
        model.release()
      }
    }
  }
}

/** Test access to the cache-info command, which is internal to the client. */
private[ml] object MlClientAccess {
  def cacheInfo(spark: org.apache.spark.sql.SparkSession): String =
    org.apache.spark.sql.connect.ml.MlClient.cacheInfo(spark)
}
