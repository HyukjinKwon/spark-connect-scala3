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
 * Evaluates clustering with the silhouette score (default) on the server. Mirrors
 * `org.apache.spark.ml.evaluation.ClusteringEvaluator`.
 */
class ClusteringEvaluator(override val uid: String)
    extends Evaluator
    with HasFeaturesCol
    with HasPredictionCol
    with HasWeightCol
    with MLWritable {

  def this() = this(Identifiable.randomUID("cluEval"))

  /** The metric. Only `silhouette` is supported. */
  final val metricName: Param[String] = new Param[String](
    this,
    "metricName",
    "metric name in evaluation (silhouette)",
    ParamValidators.inArray(Array("silhouette"))
  )

  /** The distance measure: `squaredEuclidean` or `cosine`. */
  final val distanceMeasure: Param[String] = new Param[String](
    this,
    "distanceMeasure",
    "The distance measure. Supported options: 'squaredEuclidean' and 'cosine'",
    ParamValidators.inArray(Array("squaredEuclidean", "cosine"))
  )

  setDefault(metricName -> "silhouette", distanceMeasure -> "squaredEuclidean")

  def getMetricName: String = $(metricName)

  def setMetricName(value: String): this.type = set(metricName, value)

  def getDistanceMeasure: String = $(distanceMeasure)

  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  override def evaluate(dataset: Dataset[_]): Double =
    RemoteOps.evaluate(this, ClusteringEvaluator.OperatorName, dataset)

  override def copy(extra: ParamMap): ClusteringEvaluator =
    copyValues(new ClusteringEvaluator(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object ClusteringEvaluator extends MLReadable[ClusteringEvaluator] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.evaluation.ClusteringEvaluator"

  override def load(path: String): ClusteringEvaluator =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR,
      path,
      new ClusteringEvaluator(_)
    )
}
