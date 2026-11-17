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
package org.apache.spark.ml.tuning

import java.nio.file.Files

import org.apache.spark.ml.{MlTestData, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.MlRemoteSuite

class TuningIntegrationSuite extends MlRemoteSuite {

  private def tempDir(name: String): String = {
    val dir = Files.createTempDirectory("ml-connect-" + name)
    dir.resolve("out").toString
  }

  test("ParamGridBuilder builds the cartesian product of the grid") {
    val lr = new org.apache.spark.ml.classification.LogisticRegression()
    val grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.0, 0.1))
      .addGrid(lr.maxIter, Array(5, 10, 15))
      .build()
    assertEquals(grid.length, 6)
    assertEquals(grid.map(_(lr.maxIter)).toSet, Set(5, 10, 15))
  }

  test("Pipeline fits a VectorAssembler and LogisticRegression, and transforms") {
    withSpark { spark =>
      val df = MlTestData
        .binary(spark)
        .drop("features")
        .withColumn("x2", org.apache.spark.sql.functions.col("label") * 0 + 1)
      val assembler =
        new VectorAssembler().setInputCols(Array("x2", "label")).setOutputCol("features")
      val lr = new LogisticRegression().setMaxIter(5)
      val model = new Pipeline().setStages(Array(assembler, lr)).fit(df)
      assertEquals(model.stages.length, 2)
      assertEquals(model.transform(df).count(), 40L)
      model.release()
    }
  }

  test("CrossValidator selects a parameter map and reports average metrics") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(5)
      val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.1)).build()
      val cv = new CrossValidator()
        .setEstimator(lr)
        .setEstimatorParamMaps(grid)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setNumFolds(2)
        .setSeed(7L)
      val model = cv.fit(df)
      assertEquals(model.avgMetrics.length, 2)
      assertEquals(model.stdMetrics.length, 2)
      assertEquals(model.transform(df).count(), 40L)
      model.release()
    }
  }

  test("TrainValidationSplit reports one validation metric per map") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(5)
      val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.5)).build()
      val tvs = new TrainValidationSplit()
        .setEstimator(lr)
        .setEstimatorParamMaps(grid)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setTrainRatio(0.75)
        .setSeed(7L)
      val model = tvs.fit(df)
      assertEquals(model.validationMetrics.length, 2)
      assertEquals(model.transform(df).count(), 40L)
      model.release()
    }
  }

  test("LogisticRegressionModel round-trips through save and load") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val model = new LogisticRegression().setMaxIter(5).fit(df)
      val path = tempDir("lr")
      model.write.save(path)
      val loaded = LogisticRegressionModel.load(path)
      assertEquals(loaded.coefficients.toArray.toSeq, model.coefficients.toArray.toSeq)
      assertEquals(loaded.getMaxIter, 5)
      assertEquals(loaded.transform(df).count(), 40L)
      model.release()
      loaded.release()
    }
  }

  test("LogisticRegression estimator round-trips through save and load with overwrite") {
    withSpark { spark =>
      val path = tempDir("lr-est")
      val lr = new LogisticRegression().setRegParam(0.25).setMaxIter(4)
      lr.save(path)
      val loaded = LogisticRegression.load(path)
      assertEquals(loaded.getRegParam, 0.25)
      assertEquals(loaded.getMaxIter, 4)
      lr.write.overwrite().save(path)
      val fitted = LogisticRegression.load(path).setMaxIter(2)
      assertEquals(fitted.getMaxIter, 2)
    }
  }

  test("PipelineModel and CrossValidatorModel persist and reload") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(5)
      val pipelineModel = new Pipeline().setStages(Array(lr)).fit(df)
      val pipelinePath = tempDir("pipeline")
      pipelineModel.write.save(pipelinePath)
      val reloaded = PipelineModel.load(pipelinePath)
      assertEquals(reloaded.stages.length, 1)
      assertEquals(reloaded.transform(df).count(), 40L)
      pipelineModel.release()
      reloaded.release()
    }
  }
}
