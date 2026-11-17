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

import org.apache.spark.ml.{MlRemoteSuite, MlTestData, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StopWordsRemover, VectorAssembler}
import org.apache.spark.ml.util.{DefaultParams, Json}

/**
 * Checks the on-disk layout and metadata of every composite model against Spark's standard
 * persistence format (the layout PySpark 4.3.0 writes over Connect). The cross-client round trips
 * were run by hand against PySpark; these tests keep the layout from drifting.
 */
class PersistenceLayoutIntegrationSuite extends MlRemoteSuite {

  private def dir(name: String): String =
    Files.createTempDirectory("ml-layout-" + name).resolve("m").toString

  private def metadata(path: String): Json.Obj = DefaultParams.load(path).raw

  private def paramMap(path: String): Json.Obj = DefaultParams.load(path).paramMap

  test("Pipeline metadata lists stage uids; stages live under zero-padded stage directories") {
    withSpark { spark =>
      val df = MlTestData
        .binary(spark)
        .drop("features")
        .withColumn("x2", org.apache.spark.sql.functions.col("label") * 0 + 1)
      val lr = new LogisticRegression().setMaxIter(3)
      val assembler =
        new VectorAssembler().setInputCols(Array("x2", "label")).setOutputCol("features")
      val model = new Pipeline().setStages(Array(assembler, lr)).fit(df)
      val path = dir("pipeline")
      model.write.save(path)

      val meta = metadata(path)
      assertEquals(meta.getStr("class"), "org.apache.spark.ml.PipelineModel")
      assertEquals(meta.getStr("uid"), model.uid)
      assert(meta.get("timestamp").isDefined && meta.get("sparkVersion").isDefined)
      val stageUids = paramMap(path).get("stageUids") match {
        case Some(Json.Arr(vs)) => vs.collect { case Json.Str(s) => s }
        case other => fail(s"no stageUids: $other"); Nil
      }
      assertEquals(stageUids, Seq(assembler.uid, lr.uid))
      assertEquals(stageUids.length, 2)
      assert(new java.io.File(s"$path/stages/0_${stageUids(0)}").isDirectory)
      assert(new java.io.File(s"$path/stages/1_${stageUids(1)}").isDirectory)
      model.release()
    }
  }

  test(
    "CrossValidatorModel writes estimator, evaluator, bestModel and Spark's validator metadata"
  ) {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(3)
      val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.1)).build()
      val cv = new CrossValidator()
        .setEstimator(lr)
        .setEstimatorParamMaps(grid)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setNumFolds(2)
        .setSeed(5L)
      val model = cv.fit(df)
      val path = dir("cv")
      model.write.save(path)
      val meta = metadata(path)
      assertEquals(meta.getStr("class"), "org.apache.spark.ml.tuning.CrossValidatorModel")
      assert(meta.get("avgMetrics").isDefined)
      assert(meta.get("persistSubModels").isDefined)
      val epm = paramMap(path).get("estimatorParamMaps") match {
        case Some(Json.Arr(vs)) => vs
        case other => fail(s"no estimatorParamMaps: $other"); Nil
      }
      assertEquals(epm.length, 2)
      val first = epm.head match {
        case Json.Arr(pairs) => pairs.head.asInstanceOf[Json.Obj]; case _ => fail("shape")
      }
      assertEquals(first.getStr("name"), "regParam")
      assertEquals(first.getStr("parent"), lr.uid)
      assert(new java.io.File(s"$path/bestModel").isDirectory)
      assert(new java.io.File(s"$path/estimator").isDirectory)
      assert(new java.io.File(s"$path/evaluator").isDirectory)
      model.release()
    }
  }

  test("TrainValidationSplitModel writes validationMetrics and the same subdirectories") {
    withSpark { spark =>
      val df = MlTestData.binary(spark)
      val lr = new LogisticRegression().setMaxIter(3)
      val grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.2)).build()
      val model = new TrainValidationSplit()
        .setEstimator(lr)
        .setEstimatorParamMaps(grid)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setSeed(5L)
        .fit(df)
      val path = dir("tvs")
      model.write.save(path)
      val meta = metadata(path)
      assertEquals(meta.getStr("class"), "org.apache.spark.ml.tuning.TrainValidationSplitModel")
      assert(meta.get("validationMetrics").isDefined)
      assert(new java.io.File(s"$path/bestModel").isDirectory)
      model.release()
    }
  }

  test("OneVsRestModel writes numClasses, the classifier and one directory per class") {
    withSpark { spark =>
      val df = MlTestData.multiclass(spark)
      val model = new OneVsRest().setClassifier(new LogisticRegression().setMaxIter(3)).fit(df)
      val path = dir("ovr")
      model.write.save(path)
      assertEquals(
        metadata(path).getStr("class"),
        "org.apache.spark.ml.classification.OneVsRestModel"
      )
      assert(metadata(path).get("numClasses").isDefined)
      (0 until 3).foreach(i => assert(new java.io.File(s"$path/model_$i").isDirectory))
      assert(new java.io.File(s"$path/classifier").isDirectory)
      val loaded = OneVsRestModel.load(path)
      assertEquals(loaded.numClasses, 3)
      model.release()
      loaded.release()
    }
  }

  test("StopWordsRemover defaults stopWords to Spark's English list from the server") {
    withSpark { spark =>
      val remover = new StopWordsRemover()
      assert(remover.getStopWords.contains("the"))
      assert(remover.getStopWords.length > 100)
    }
  }
}
