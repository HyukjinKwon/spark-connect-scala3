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
 * Evaluates ranking predictions (`meanAveragePrecision` by default) at cutoff `k`, on the server.
 * Mirrors `org.apache.spark.ml.evaluation.RankingEvaluator`.
 */
class RankingEvaluator(override val uid: String)
    extends Evaluator
    with HasLabelCol
    with HasPredictionCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("rankEval"))

  /** The metric, such as `meanAveragePrecision`, `ndcgAtK`, or `precisionAtK`. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (meanAveragePrecision|meanAveragePrecisionAtK|precisionAtK|" +
      "ndcgAtK|recallAtK)"
  )

  /** The cutoff used by the `AtK` metrics. */
  final val k: IntParam = new IntParam(
    this,
    "k",
    "The ranking position value used in meanAveragePrecisionAtK|precisionAtK|ndcgAtK|recallAtK",
    ParamValidators.gt(0)
  )

  setDefault(k -> 10, metricName -> "meanAveragePrecision")

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getK: Int = $(k)

  def setK(value: Int): this.type = set(k, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, RankingEvaluator.OperatorName, dataset)

  override def copy(extra: ParamMap): RankingEvaluator =
    copyValues(new RankingEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RankingEvaluator extends MLReadable[RankingEvaluator] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.evaluation.RankingEvaluator"

  override def load(path: String): RankingEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new RankingEvaluator(_)
    )
}
