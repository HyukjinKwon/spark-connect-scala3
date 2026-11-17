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

import org.apache.spark.ml.connect.ModelRef
import org.apache.spark.ml.util.Summary
import org.apache.spark.sql.DataFrame

/**
 * A summary of a classification model's predictions on a dataset, computed on the server. Mirrors
 * `org.apache.spark.ml.classification.ClassificationSummary`.
 */
class ClassificationSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends Summary(ref, path) {

  /** The DataFrame of predictions. */
  def predictions: DataFrame = frame("predictions")

  def predictionCol: String = attr("predictionCol")

  def labelCol: String = attr("labelCol")

  def weightCol: String = attr("weightCol")

  /** The distinct labels, in order. */
  def labels: Array[Double] = attr("labels")

  def accuracy: Double = attr("accuracy")

  def weightedPrecision: Double = attr("weightedPrecision")

  def weightedRecall: Double = attr("weightedRecall")

  def weightedFMeasure: Double = attr("weightedFMeasure")

  def weightedTruePositiveRate: Double = attr("weightedTruePositiveRate")

  def weightedFalsePositiveRate: Double = attr("weightedFalsePositiveRate")

  def truePositiveRateByLabel: Array[Double] = attr("truePositiveRateByLabel")

  def falsePositiveRateByLabel: Array[Double] = attr("falsePositiveRateByLabel")

  def precisionByLabel: Array[Double] = attr("precisionByLabel")

  def recallByLabel: Array[Double] = attr("recallByLabel")

  def fMeasureByLabel: Array[Double] = attr("fMeasureByLabel")
}
