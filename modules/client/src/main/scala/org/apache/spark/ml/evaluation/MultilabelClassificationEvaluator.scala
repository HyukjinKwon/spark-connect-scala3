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
 * Evaluates multilabel predictions (`f1Measure` by default), on the server. Mirrors
 * `org.apache.spark.ml.evaluation.MultilabelClassificationEvaluator`.
 */
class MultilabelClassificationEvaluator(override val uid: String)
    extends Evaluator
    with HasLabelCol
    with HasPredictionCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("mlEval"))

  /** The metric name, such as `f1Measure`, `accuracy`, or `hammingLoss`. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (subsetAccuracy|accuracy|hammingLoss|precision|recall|f1Measure|" +
      "precisionByLabel|recallByLabel|f1MeasureByLabel|microPrecision|microRecall|microF1Measure)"
  )

  /** The label whose metric is reported for the by-label metrics. */
  final val metricLabel: DoubleParam = new DoubleParam(
    this,
    "metricLabel",
    "The class whose metric will be computed in precisionByLabel|recallByLabel|" +
      "f1MeasureByLabel. Must be >= 0",
    ParamValidators.gtEq(0.0)
  )

  setDefault(metricLabel -> 0.0, metricName -> "f1Measure")

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getMetricLabel: Double = $(metricLabel)

  def setMetricLabel(value: Double): this.type = set(metricLabel, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, MultilabelClassificationEvaluator.OperatorName, dataset)

  override def isLargerBetter: Boolean = $(metricName) != "hammingLoss"

  override def copy(extra: ParamMap): MultilabelClassificationEvaluator =
    copyValues(new MultilabelClassificationEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object MultilabelClassificationEvaluator extends MLReadable[MultilabelClassificationEvaluator] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.evaluation.MultilabelClassificationEvaluator"

  override def load(path: String): MultilabelClassificationEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new MultilabelClassificationEvaluator(_)
    )
}
