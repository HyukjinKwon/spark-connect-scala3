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

import org.apache.spark.connect.proto
import org.apache.spark.ml.connect.{RemoteIO, RemoteOps}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
import org.apache.spark.sql.Dataset

/**
 * Evaluates multiclass classifiers (`f1` by default, plus accuracy, precision, recall and per-label
 * metrics), on the server. Mirrors
 * `org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator`.
 */
class MulticlassClassificationEvaluator(override val uid: String)
    extends Evaluator
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with HasProbabilityCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("mcEval"))

  /** The metric name, such as `f1`, `accuracy`, or `weightedPrecision`. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (f1|accuracy|weightedPrecision|weightedRecall|weightedFMeasure" +
      "|weightedTruePositiveRate|weightedFalsePositiveRate|truePositiveRateByLabel|" +
      "falsePositiveRateByLabel|precisionByLabel|recallByLabel|fMeasureByLabel|logLoss|" +
      "hammingLoss)"
  )

  /** The label whose metric is reported for the by-label metrics. */
  final val metricLabel: DoubleParam = new DoubleParam(
    this,
    "metricLabel",
    "The class whose metric will be computed in truePositiveRateByLabel|" +
      "falsePositiveRateByLabel|precisionByLabel|recallByLabel|fMeasureByLabel. Must be >= 0",
    ParamValidators.gtEq(0.0)
  )

  /** The beta used for F-measure. */
  final val beta: DoubleParam = new DoubleParam(
    this,
    "beta",
    "The beta value used in weightedFMeasure|fMeasureByLabel. Must be > 0. The default value is 1.",
    ParamValidators.gt(0.0)
  )

  /** The epsilon used to clip probabilities in logLoss. */
  final val eps: DoubleParam = new DoubleParam(
    this,
    "eps",
    "log-loss is undefined for p=0 or p=1, so probabilities are clipped to max(eps, min(1 - eps, p)).",
    ParamValidators.inRange(0.0, 0.5)
  )

  setDefault(metricName -> "f1", eps -> 1e-15, metricLabel -> 0.0, beta -> 1.0)

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getMetricLabel: Double = $(metricLabel)

  def setMetricLabel(value: Double): this.type = set(metricLabel, value)

  def getBeta: Double = $(beta)

  def setBeta(value: Double): this.type = set(beta, value)

  def getEps: Double = $(eps)

  def setEps(value: Double): this.type = set(eps, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, MulticlassClassificationEvaluator.OperatorName, dataset)

  override def isLargerBetter: Boolean =
    $(metricName) != "logLoss" && $(metricName) != "hammingLoss"

  override def copy(extra: ParamMap): MulticlassClassificationEvaluator =
    copyValues(new MulticlassClassificationEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object MulticlassClassificationEvaluator extends MLReadable[MulticlassClassificationEvaluator] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator"

  override def load(path: String): MulticlassClassificationEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new MulticlassClassificationEvaluator(_)
    )
}
