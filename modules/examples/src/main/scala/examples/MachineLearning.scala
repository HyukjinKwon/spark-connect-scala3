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
package examples

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/**
 * Spark ML over Spark Connect: a pipeline (vector assembly and logistic regression) tuned with
 * cross validation, with the predictions scored by a binary evaluator. Everything runs on the
 * server; this program only builds the plans and reads the results.
 *
 * Needs a Spark Connect server 4.0 or newer.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.MachineLearning"
 * }}}
 */
object MachineLearning {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("machine-learning").getOrCreate()
    try {
      // Label 1 for the rows with a large id, so the two classes are separable.
      val data = spark.sql(
        "SELECT CAST(IF(id < 30, 0, 1) AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x, " +
          "CAST(id * id % 7 AS DOUBLE) AS y FROM range(60)"
      )

      val assembler = new VectorAssembler()
        .setInputCols(Array("x", "y"))
        .setOutputCol("features")
      val lr = new LogisticRegression().setMaxIter(20)
      val pipeline = new Pipeline().setStages(Array(assembler, lr))

      val grid = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.0, 0.1))
        .build()
      val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEstimatorParamMaps(grid)
        .setEvaluator(evaluator)
        .setNumFolds(3)
        .setSeed(42L)

      val model = cv.fit(data)
      println(s"Average AUC per parameter map: ${model.avgMetrics.mkString(", ")}")

      val predictions = model.transform(data)
      println(s"Test AUC on the training data: ${evaluator.evaluate(predictions)}")
      // Vector columns print as their struct form, so show the scalar columns only.
      predictions.select("label", "prediction").show(5)

      // Model attributes are read from the server on demand.
      val best = model.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      val logistic =
        best.stages.last.asInstanceOf[org.apache.spark.ml.classification.LogisticRegressionModel]
      println(s"Coefficients: ${logistic.coefficients}")
      println(s"Predict (0.0, 0.0): ${logistic.predict(Vectors.dense(0.0, 0.0))}")
      model.release()
    } finally spark.close()
  }
}
