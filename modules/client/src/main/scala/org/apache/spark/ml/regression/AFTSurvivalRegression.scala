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
package org.apache.spark.ml.regression

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of AFTSurvivalRegression. */
trait AFTSurvivalRegressionParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasMaxIter
    with HasTol
    with HasFitIntercept
    with HasAggregationDepth
    with HasMaxBlockSizeInMB {

  final val censorCol: Param[String] = new Param[String](
    this,
    "censorCol",
    "censor column name. The value of this column could be 0 or 1. If the value is 1, it means the event has occurred i.e. uncensored; otherwise censored."
  )

  final val quantileProbabilities: DoubleArrayParam =
    new DoubleArrayParam(this, "quantileProbabilities", "quantile probabilities array")

  final val quantilesCol: Param[String] = new Param[String](
    this,
    "quantilesCol",
    "quantiles column name. This column will output quantiles of corresponding quantileProbabilities if it is set."
  )

  setDefault(
    censorCol -> "censor",
    quantileProbabilities -> Array(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
    maxIter -> 100,
    tol -> 1e-6,
    aggregationDepth -> 2,
    maxBlockSizeInMB -> 0.0
  )

  def setCensorCol(value: String): this.type = set(censorCol, value)
  def getCensorCol: String = $(censorCol)

  def setQuantileProbabilities(value: Array[Double]): this.type = set(quantileProbabilities, value)
  def getQuantileProbabilities: Array[Double] = $(quantileProbabilities)

  def setQuantilesCol(value: String): this.type = set(quantilesCol, value)
  def getQuantilesCol: String = $(quantilesCol)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

}

/**
 * Accelerated Failure Time (AFT) model survival regression. Mirrors
 * `org.apache.spark.ml.regression.AFTSurvivalRegression`.
 */
class AFTSurvivalRegression(override val uid: String)
    extends Estimator[AFTSurvivalRegressionModel]
    with AFTSurvivalRegressionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("aftSurvReg"))

  override def fit(dataset: Dataset[_]): AFTSurvivalRegressionModel =
    RemoteOps.fit(
      this,
      AFTSurvivalRegression.OperatorName,
      dataset,
      (id, ref) => new AFTSurvivalRegressionModel(id, ref)
    )

  override def copy(extra: ParamMap): AFTSurvivalRegression =
    copyValues(new AFTSurvivalRegression(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object AFTSurvivalRegression extends MLReadable[AFTSurvivalRegression] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.AFTSurvivalRegression"

  override def load(path: String): AFTSurvivalRegression =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new AFTSurvivalRegression(_)
    )
}

/**
 * The model fit by [[AFTSurvivalRegression]], held on the server. Mirrors
 * `org.apache.spark.ml.regression.AFTSurvivalRegressionModel`.
 */
class AFTSurvivalRegressionModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[AFTSurvivalRegressionModel]
    with AFTSurvivalRegressionParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `intercept`, read from the server. */
  def intercept: Double = attr[Double]("intercept")

  /** Model attribute `coefficients`, read from the server. */
  def coefficients: Vector = attr[Vector]("coefficients")

  /** Model attribute `scale`, read from the server. */
  def scale: Double = attr[Double]("scale")

  /** Quantiles of the survival time distribution for `features`. */

  def predictQuantiles(features: Vector): Vector = attrWith[Vector]("predictQuantiles", features)

  override def copy(extra: ParamMap): AFTSurvivalRegressionModel =
    copyValues(new AFTSurvivalRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object AFTSurvivalRegressionModel extends MLReadable[AFTSurvivalRegressionModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.AFTSurvivalRegressionModel"

  override def load(path: String): AFTSurvivalRegressionModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new AFTSurvivalRegressionModel(id, ref))
}
