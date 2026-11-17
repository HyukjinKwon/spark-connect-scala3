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

/** Params of GeneralizedLinearRegression. */
trait GeneralizedLinearRegressionParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with HasMaxIter
    with HasTol
    with HasRegParam
    with HasFitIntercept {

  final val family: Param[String] = new Param[String](
    this,
    "family",
    "The name of family which is a description of the error distribution to be used in the model.",
    ParamValidators.inArray(Array("gaussian", "binomial", "poisson", "gamma", "tweedie"))
  )

  final val link: Param[String] = new Param[String](
    this,
    "link",
    "The name of link function which provides the relationship between the linear predictor and the mean of the distribution function.",
    ParamValidators.inArray(
      Array("identity", "log", "inverse", "logit", "probit", "cloglog", "sqrt")
    )
  )

  final val variancePower: DoubleParam = new DoubleParam(
    this,
    "variancePower",
    "The power in the variance function of the Tweedie distribution.",
    ParamValidators.gtEq(0.0)
  )

  final val linkPower: DoubleParam =
    new DoubleParam(this, "linkPower", "The index in the power link function.")

  final val linkPredictionCol: Param[String] =
    new Param[String](this, "linkPredictionCol", "link prediction (linear predictor) column name")

  final val offsetCol: Param[String] =
    new Param[String](this, "offsetCol", "The offset column name.")

  final val solver: Param[String] = new Param[String](
    this,
    "solver",
    "The solver algorithm for optimization.",
    ParamValidators.inArray(Array("irls"))
  )

  setDefault(
    family -> "gaussian",
    variancePower -> 0.0,
    solver -> "irls",
    maxIter -> 25,
    tol -> 1e-6,
    regParam -> 0.0
  )

  def setFamily(value: String): this.type = set(family, value)
  def getFamily: String = $(family)

  def setLink(value: String): this.type = set(link, value)
  def getLink: String = $(link)

  def setVariancePower(value: Double): this.type = set(variancePower, value)
  def getVariancePower: Double = $(variancePower)

  def setLinkPower(value: Double): this.type = set(linkPower, value)
  def getLinkPower: Double = $(linkPower)

  def setLinkPredictionCol(value: String): this.type = set(linkPredictionCol, value)
  def getLinkPredictionCol: String = $(linkPredictionCol)

  def setOffsetCol(value: String): this.type = set(offsetCol, value)
  def getOffsetCol: String = $(offsetCol)

  def setSolver(value: String): this.type = set(solver, value)
  def getSolver: String = $(solver)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

}

/**
 * Generalized linear regression. Mirrors
 * `org.apache.spark.ml.regression.GeneralizedLinearRegression`.
 */
class GeneralizedLinearRegression(override val uid: String)
    extends Estimator[GeneralizedLinearRegressionModel]
    with GeneralizedLinearRegressionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("glr"))

  override def fit(dataset: Dataset[_]): GeneralizedLinearRegressionModel =
    RemoteOps.fit(
      this,
      GeneralizedLinearRegression.OperatorName,
      dataset,
      (id, ref) => new GeneralizedLinearRegressionModel(id, ref)
    )

  override def copy(extra: ParamMap): GeneralizedLinearRegression =
    copyValues(new GeneralizedLinearRegression(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object GeneralizedLinearRegression extends MLReadable[GeneralizedLinearRegression] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.regression.GeneralizedLinearRegression"

  override def load(path: String): GeneralizedLinearRegression =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new GeneralizedLinearRegression(_)
    )
}

/**
 * The model fit by [[GeneralizedLinearRegression]], held on the server. Mirrors
 * `org.apache.spark.ml.regression.GeneralizedLinearRegressionModel`.
 */
class GeneralizedLinearRegressionModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[GeneralizedLinearRegressionModel]
    with GeneralizedLinearRegressionParams
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

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** The training summary of this model. */

  def summary: GeneralizedLinearRegressionTrainingSummary =
    new GeneralizedLinearRegressionTrainingSummary(ref, Seq("summary"))

  /** Whether a training summary is available. */

  def hasSummary: Boolean = attr[Boolean]("hasSummary")

  /** Evaluates the model on `dataset`, returning a summary of the predictions. */

  def evaluate(dataset: Dataset[_]): GeneralizedLinearRegressionTrainingSummary = {

    val summaryId = RemoteOps.fetch(ref, Seq("evaluate"), Seq(dataset)).asInstanceOf[String]

    new GeneralizedLinearRegressionTrainingSummary(new ModelRef(summaryId, ref.session, None), Nil)

  }

  override def copy(extra: ParamMap): GeneralizedLinearRegressionModel =
    copyValues(new GeneralizedLinearRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object GeneralizedLinearRegressionModel extends MLReadable[GeneralizedLinearRegressionModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.regression.GeneralizedLinearRegressionModel"

  override def load(path: String): GeneralizedLinearRegressionModel =
    RemoteIO.loadModel(
      OperatorName,
      path,
      (id, ref) => new GeneralizedLinearRegressionModel(id, ref)
    )
}

/** Training summary of a [[GeneralizedLinearRegressionModel]]. */
class GeneralizedLinearRegressionTrainingSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends Summary(ref, path) {

  def aic: Double = attr[Double]("aic")

  def degreesOfFreedom: Long = attr[Long]("degreesOfFreedom")

  def deviance: Double = attr[Double]("deviance")

  def dispersion: Double = attr[Double]("dispersion")

  def nullDeviance: Double = attr[Double]("nullDeviance")

  def numInstances: Long = attr[Long]("numInstances")

  def rank: Int = attr[Int]("rank")

  def residualDegreeOfFreedom: Long = attr[Long]("residualDegreeOfFreedom")

  def residualDegreeOfFreedomNull: Long = attr[Long]("residualDegreeOfFreedomNull")

  def numIterations: Int = attr[Int]("numIterations")

  def solver: String = attr[String]("solver")

  def tValues: Array[Double] = attr[Array[Double]]("tValues")

  def pValues: Array[Double] = attr[Array[Double]]("pValues")

  def coefficientStandardErrors: Array[Double] = attr[Array[Double]]("coefficientStandardErrors")

  def predictions: DataFrame = frame("predictions")

  def residuals: DataFrame = frame("residuals")

  def predictionCol: String = attr[String]("predictionCol")
}
