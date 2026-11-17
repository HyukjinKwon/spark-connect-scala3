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
 * Evaluates binary classifiers with `areaUnderROC` (default) or `areaUnderPR`, on the server.
 * Mirrors `org.apache.spark.ml.evaluation.BinaryClassificationEvaluator`.
 */
class BinaryClassificationEvaluator(override val uid: String)
    extends Evaluator
    with HasLabelCol
    with HasRawPredictionCol
    with HasWeightCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("binEval"))

  /** The metric: `areaUnderROC` or `areaUnderPR`. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (areaUnderROC|areaUnderPR)",
    ParamValidators.inArray(Array("areaUnderROC", "areaUnderPR"))
  )

  /** The number of bins used to compute the curves. */
  final val numBins: IntParam = new IntParam(
    this,
    "numBins",
    "Number of bins to down-sample the curves (ROC curve, PR curve) in area computation. If 0, " +
      "no down-sampling will be used.",
    ParamValidators.gtEq(0)
  )

  setDefault(metricName -> "areaUnderROC", numBins -> 1000)

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getNumBins: Int = $(numBins)

  def setNumBins(value: Int): this.type = set(numBins, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, BinaryClassificationEvaluator.OperatorName, dataset)

  override def copy(extra: ParamMap): BinaryClassificationEvaluator =
    copyValues(new BinaryClassificationEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object BinaryClassificationEvaluator extends MLReadable[BinaryClassificationEvaluator] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"

  override def load(path: String): BinaryClassificationEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new BinaryClassificationEvaluator(_)
    )
}
