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

import java.util.Locale

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, ProbabilisticClassifierParams}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of [[LogisticRegression]]. */
trait LogisticRegressionParams
    extends ProbabilisticClassifierParams
    with HasRegParam
    with HasElasticNetParam
    with HasMaxIter
    with HasFitIntercept
    with HasTol
    with HasStandardization
    with HasWeightCol
    with HasThreshold
    with HasAggregationDepth
    with HasMaxBlockSizeInMB {

  /** The label distribution family: `auto`, `binomial`, or `multinomial`. */
  final val family: Param[String] = new Param[String](
    this,
    "family",
    "The name of family which is a description of the label distribution to be used in the " +
      "model. Supported options: auto, binomial, multinomial.",
    (value: String) =>
      Set("auto", "binomial", "multinomial").contains(value.toLowerCase(Locale.ROOT))
  )

  /** The lower bounds on coefficients if fitting under bound constrained optimization. */
  final val lowerBoundsOnCoefficients: Param[Matrix] = new Param[Matrix](
    this,
    "lowerBoundsOnCoefficients",
    "The lower bounds on coefficients if fitting under bound constrained optimization."
  )

  /** The upper bounds on coefficients if fitting under bound constrained optimization. */
  final val upperBoundsOnCoefficients: Param[Matrix] = new Param[Matrix](
    this,
    "upperBoundsOnCoefficients",
    "The upper bounds on coefficients if fitting under bound constrained optimization."
  )

  /** The lower bounds on intercepts if fitting under bound constrained optimization. */
  final val lowerBoundsOnIntercepts: Param[Vector] = new Param[Vector](
    this,
    "lowerBoundsOnIntercepts",
    "The lower bounds on intercepts if fitting under bound constrained optimization."
  )

  /** The upper bounds on intercepts if fitting under bound constrained optimization. */
  final val upperBoundsOnIntercepts: Param[Vector] = new Param[Vector](
    this,
    "upperBoundsOnIntercepts",
    "The upper bounds on intercepts if fitting under bound constrained optimization."
  )

  def getFamily: String = $(family)

  def getLowerBoundsOnCoefficients: Matrix = $(lowerBoundsOnCoefficients)

  def getUpperBoundsOnCoefficients: Matrix = $(upperBoundsOnCoefficients)

  def getLowerBoundsOnIntercepts: Vector = $(lowerBoundsOnIntercepts)

  def getUpperBoundsOnIntercepts: Vector = $(upperBoundsOnIntercepts)
}

/**
 * Logistic regression, fit on the Spark Connect server. Mirrors
 * `org.apache.spark.ml.classification.LogisticRegression`.
 */
class LogisticRegression(override val uid: String)
    extends Estimator[LogisticRegressionModel]
    with LogisticRegressionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("logreg"))

  setDefault(
    regParam -> 0.0,
    elasticNetParam -> 0.0,
    maxIter -> 100,
    tol -> 1e-6,
    fitIntercept -> true,
    family -> "auto",
    standardization -> true,
    threshold -> 0.5,
    aggregationDepth -> 2,
    maxBlockSizeInMB -> 0.0
  )

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setFamily(value: String): this.type = set(family, value)

  def setStandardization(value: Boolean): this.type = set(standardization, value)

  def setThreshold(value: Double): this.type = set(threshold, value)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setLowerBoundsOnCoefficients(value: Matrix): this.type =
    set(lowerBoundsOnCoefficients, value)

  def setUpperBoundsOnCoefficients(value: Matrix): this.type =
    set(upperBoundsOnCoefficients, value)

  def setLowerBoundsOnIntercepts(value: Vector): this.type = set(lowerBoundsOnIntercepts, value)

  def setUpperBoundsOnIntercepts(value: Vector): this.type = set(upperBoundsOnIntercepts, value)

  override def fit(dataset: Dataset[_]): LogisticRegressionModel =
    RemoteOps.fit(
      this,
      LogisticRegression.OperatorName,
      dataset,
      (id, ref) => new LogisticRegressionModel(id, ref)
    )

  override def copy(extra: org.apache.spark.ml.param.ParamMap): LogisticRegression =
    copyValues(new LogisticRegression(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object LogisticRegression extends MLReadable[LogisticRegression] {

  /** The server-side class name of this estimator. */
  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.LogisticRegression"

  override def load(path: String): LogisticRegression =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new LogisticRegression(_)
    )
}

/**
 * A logistic regression model fit on the server. Model attributes are read from the server on
 * demand. Mirrors `org.apache.spark.ml.classification.LogisticRegressionModel`.
 */
class LogisticRegressionModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[LogisticRegressionModel]
    with LogisticRegressionParams
    with RemoteModelLike
    with MLWritable {

  /** The coefficients of the model: a vector for binomial, one row per class for multinomial. */
  def coefficients: Vector = attr[Vector]("coefficients")

  /** The intercept of a binomial model. */
  def intercept: Double = attr[Double]("intercept")

  /** The coefficient matrix (one row per class). */
  def coefficientMatrix: Matrix = attr[Matrix]("coefficientMatrix")

  /** The intercept vector (one entry per class). */
  def interceptVector: Vector = attr[Vector]("interceptVector")

  /** The number of classes. */
  def numClasses: Int = attr[Int]("numClasses")

  /** The number of features. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Whether the model was trained with a training summary available. */
  def hasSummary: Boolean = attr[Boolean]("hasSummary")

  /** The training summary of this model. */
  def summary: LogisticRegressionTrainingSummary =
    new LogisticRegressionTrainingSummary(ref, Seq("summary"))

  /** The binary training summary of this model (only for binomial models). */
  def binarySummary: BinaryLogisticRegressionTrainingSummary =
    new BinaryLogisticRegressionTrainingSummary(ref, Seq("summary"))

  /** Evaluates the model on `dataset`, returning a summary of the results. */
  def evaluate(dataset: Dataset[_]): LogisticRegressionSummary = {
    val summaryId = RemoteOps.fetch(ref, Seq("evaluate"), Seq(dataset)).asInstanceOf[String]
    new LogisticRegressionSummary(new ModelRef(summaryId, ref.session, None), Nil)
  }

  /** Predicts the label of a feature vector. */
  def predict(features: Vector): Double = attrWith[Double]("predict", features)

  /** The raw prediction (margins) of a feature vector. */
  def predictRaw(features: Vector): Vector = attrWith[Vector]("predictRaw", features)

  /** The class probabilities of a feature vector. */
  def predictProbability(features: Vector): Vector =
    attrWith[Vector]("predictProbability", features)

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  override def copy(extra: org.apache.spark.ml.param.ParamMap): LogisticRegressionModel =
    copyValues(new LogisticRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object LogisticRegressionModel extends MLReadable[LogisticRegressionModel] {

  /** The server-side class name of this model. */
  private[ml] val OperatorName: String =
    "org.apache.spark.ml.classification.LogisticRegressionModel"

  override def load(path: String): LogisticRegressionModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new LogisticRegressionModel(id, ref))
}

/** A summary of a classification model's predictions. */
class LogisticRegressionSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends Summary(ref, path) {

  /** The DataFrame of predictions. */
  def predictions: DataFrame = frame("predictions")

  def predictionCol: String = attr("predictionCol")

  def labelCol: String = attr("labelCol")

  def featuresCol: String = attr("featuresCol")

  def probabilityCol: String = attr("probabilityCol")

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

/** The training summary of a [[LogisticRegressionModel]]. */
class LogisticRegressionTrainingSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends LogisticRegressionSummary(ref, path) {

  /** The objective function value after each iteration. */
  def objectiveHistory: Array[Double] = attr("objectiveHistory")

  /** The number of iterations the optimizer ran. */
  def totalIterations: Int = attr("totalIterations")
}

/** A summary of a binary classification model, with ROC and threshold curves. */
class BinaryLogisticRegressionSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends LogisticRegressionSummary(ref, path) {

  def scoreCol: String = attr("scoreCol")

  /** The ROC curve as a DataFrame of `(FPR, TPR)` points. */
  def roc: DataFrame = frame("roc")

  def areaUnderROC: Double = attr("areaUnderROC")

  /** The precision-recall curve as a DataFrame. */
  def pr: DataFrame = frame("pr")

  def fMeasureByThreshold: DataFrame = frame("fMeasureByThreshold")

  def precisionByThreshold: DataFrame = frame("precisionByThreshold")

  def recallByThreshold: DataFrame = frame("recallByThreshold")
}

/** The training summary of a binomial [[LogisticRegressionModel]]. */
class BinaryLogisticRegressionTrainingSummary private[ml] (ref: ModelRef, path: Seq[String])
    extends BinaryLogisticRegressionSummary(ref, path) {

  def objectiveHistory: Array[Double] = attr("objectiveHistory")

  def totalIterations: Int = attr("totalIterations")
}
