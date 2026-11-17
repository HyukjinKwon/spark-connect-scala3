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
 * Evaluates regression predictions (`rmse` by default), on the Spark Connect server. Mirrors
 * `org.apache.spark.ml.evaluation.RegressionEvaluator`.
 */
class RegressionEvaluator(override val uid: String)
    extends Evaluator
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("regEval"))

  /** The metric: `mse`, `rmse`, `r2`, `mae`, or `var`. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (mse|rmse|r2|mae|var)",
    ParamValidators.inArray(Array("mse", "rmse", "r2", "mae", "var"))
  )

  /** Whether the regression goes through the origin (no intercept). */
  final val throughOrigin: BooleanParam =
    new BooleanParam(this, "throughOrigin", "whether the regression through the origin")

  setDefault(metricName -> "rmse", throughOrigin -> false)

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getThroughOrigin: Boolean = $(throughOrigin)

  def setThroughOrigin(value: Boolean): this.type = set(throughOrigin, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, RegressionEvaluator.OperatorName, dataset)

  override def isLargerBetter: Boolean = Set("r2", "var").contains($(metricName))

  override def copy(extra: ParamMap): RegressionEvaluator =
    copyValues(new RegressionEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RegressionEvaluator extends MLReadable[RegressionEvaluator] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.evaluation.RegressionEvaluator"

  override def load(path: String): RegressionEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new RegressionEvaluator(_)
    )
}
