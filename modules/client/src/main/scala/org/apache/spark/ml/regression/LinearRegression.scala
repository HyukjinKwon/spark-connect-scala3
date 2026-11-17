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

/** Params of LinearRegression. */
trait LinearRegressionParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with HasMaxIter
    with HasRegParam
    with HasTol
    with HasFitIntercept
    with HasStandardization
    with HasElasticNetParam
    with HasAggregationDepth
    with HasMaxBlockSizeInMB
    with HasSolver {

  final val loss: Param[String] = new Param[String](
    this,
    "loss",
    "The loss function to be optimized. Supported options: squaredError, huber.",
    ParamValidators.inArray(Array("squaredError", "huber"))
  )

  final val epsilon: DoubleParam = new DoubleParam(
    this,
    "epsilon",
    "The shape parameter to control the amount of robustness. Must be > 1.0. Only valid when loss is huber",
    ParamValidators.gt(1.0)
  )

  setDefault(
    loss -> "squaredError",
    epsilon -> 1.35,
    maxIter -> 100,
    regParam -> 0.0,
    tol -> 1e-6,
    elasticNetParam -> 0.0,
    solver -> "auto"
  )

  def setLoss(value: String): this.type = set(loss, value)
  def getLoss: String = $(loss)

  def setEpsilon(value: Double): this.type = set(epsilon, value)
  def getEpsilon: Double = $(epsilon)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setStandardization(value: Boolean): this.type = set(standardization, value)

  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)

  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  def setSolver(value: String): this.type = set(solver, value)

}

/** Linear regression. Mirrors `org.apache.spark.ml.regression.LinearRegression`. */
class LinearRegression(override val uid: String)
    extends Estimator[LinearRegressionModel]
    with LinearRegressionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("linReg"))

  override def fit(dataset: Dataset[_]): LinearRegressionModel =
    RemoteOps.fit(
      this,
      LinearRegression.OperatorName,
      dataset,
      (id, ref) => new LinearRegressionModel(id, ref)
    )

  override def copy(extra: ParamMap): LinearRegression =
    copyValues(new LinearRegression(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object LinearRegression extends MLReadable[LinearRegression] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.LinearRegression"

  override def load(path: String): LinearRegression =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new LinearRegression(_)
    )
}

/**
 * The model fit by [[LinearRegression]], held on the server. Mirrors
 * `org.apache.spark.ml.regression.LinearRegressionModel`.
 */
class LinearRegressionModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[LinearRegressionModel]
    with LinearRegressionParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `coefficients`, read from the server. */
  def coefficients: Vector = attr[Vector]("coefficients")

  /** Model attribute `intercept`, read from the server. */
  def intercept: Double = attr[Double]("intercept")

  /** Model attribute `scale`, read from the server. */
  def scale: Double = attr[Double]("scale")

  /** Whether the model was fit with a training summary available. */

  def hasSummary: Boolean = attr[Boolean]("hasSummary")

  /** The training summary of this model. */

  def summary: LinearRegressionTrainingSummary =
    new LinearRegressionTrainingSummary(ref, Seq("summary"))

  override def copy(extra: ParamMap): LinearRegressionModel =
    copyValues(new LinearRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object LinearRegressionModel extends MLReadable[LinearRegressionModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.LinearRegressionModel"

  override def load(path: String): LinearRegressionModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new LinearRegressionModel(id, ref))
}

/** Training summary of a [[LinearRegressionModel]]. */
class LinearRegressionTrainingSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends Summary(ref, path) {

  def r2: Double = attr[Double]("r2")

  def r2adj: Double = attr[Double]("r2adj")

  def rootMeanSquaredError: Double = attr[Double]("rootMeanSquaredError")

  def meanSquaredError: Double = attr[Double]("meanSquaredError")

  def meanAbsoluteError: Double = attr[Double]("meanAbsoluteError")

  def explainedVariance: Double = attr[Double]("explainedVariance")

  def numInstances: Long = attr[Long]("numInstances")

  def degreesOfFreedom: Long = attr[Long]("degreesOfFreedom")

  def objectiveHistory: Array[Double] = attr[Array[Double]]("objectiveHistory")

  def totalIterations: Int = attr[Int]("totalIterations")

  def predictions: DataFrame = frame("predictions")

  def residuals: DataFrame = frame("residuals")
}
