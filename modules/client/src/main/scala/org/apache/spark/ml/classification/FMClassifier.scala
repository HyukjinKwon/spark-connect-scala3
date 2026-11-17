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

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of FMClassifier. */
trait FMClassifierParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasMaxIter
    with HasTol
    with HasFitIntercept
    with HasRegParam
    with HasStepSize
    with HasSeed
    with HasThresholds
    with HasRawPredictionCol
    with HasProbabilityCol {

  final val factorSize: IntParam = new IntParam(
    this,
    "factorSize",
    "Dimensionality of the factor vectors, which are used in the pairwise interactions (> 0).",
    ParamValidators.gt(0)
  )

  final val fitLinear: BooleanParam =
    new BooleanParam(this, "fitLinear", "whether to fit linear term (aka 1-way term)")

  final val miniBatchFraction: DoubleParam = new DoubleParam(
    this,
    "miniBatchFraction",
    "fraction of the input data set that should be used for one iteration of gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false)
  )

  final val initStd: DoubleParam = new DoubleParam(
    this,
    "initStd",
    "standard deviation of initial coefficients",
    ParamValidators.gt(0.0)
  )

  final val solver: Param[String] = new Param[String](
    this,
    "solver",
    "The solver algorithm for optimization.",
    ParamValidators.inArray(Array("adamW", "gd"))
  )

  setDefault(
    factorSize -> 8,
    fitLinear -> true,
    miniBatchFraction -> 1.0,
    initStd -> 0.01,
    solver -> "adamW",
    maxIter -> 100,
    tol -> 1e-6,
    regParam -> 0.0,
    stepSize -> 1.0
  )

  def setFactorSize(value: Int): this.type = set(factorSize, value)
  def getFactorSize: Int = $(factorSize)

  def setFitLinear(value: Boolean): this.type = set(fitLinear, value)
  def getFitLinear: Boolean = $(fitLinear)

  def setMiniBatchFraction(value: Double): this.type = set(miniBatchFraction, value)
  def getMiniBatchFraction: Double = $(miniBatchFraction)

  def setInitStd(value: Double): this.type = set(initStd, value)
  def getInitStd: Double = $(initStd)

  def setSolver(value: String): this.type = set(solver, value)
  def getSolver: String = $(solver)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

}

/**
 * Factorization Machines learning algorithm for classification. Mirrors
 * `org.apache.spark.ml.classification.FMClassifier`.
 */
class FMClassifier(override val uid: String)
    extends Estimator[FMClassificationModel]
    with FMClassifierParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("fmc"))

  override def fit(dataset: Dataset[_]): FMClassificationModel =
    RemoteOps.fit(
      this,
      FMClassifier.OperatorName,
      dataset,
      (id, ref) => new FMClassificationModel(id, ref)
    )

  override def copy(extra: ParamMap): FMClassifier = copyValues(new FMClassifier(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object FMClassifier extends MLReadable[FMClassifier] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.FMClassifier"

  override def load(path: String): FMClassifier =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new FMClassifier(_)
    )
}

/**
 * The model fit by [[FMClassifier]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.FMClassificationModel`.
 */
class FMClassificationModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[FMClassificationModel]
    with FMClassifierParams
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

  /** Model attribute `linear`, read from the server. */
  def linear: Vector = attr[Vector]("linear")

  /** Model attribute `factors`, read from the server. */
  def factors: Matrix = attr[Matrix]("factors")

  /** Evaluates the model on `dataset`, returning a summary of the predictions. */

  def evaluate(dataset: Dataset[_]): ClassificationSummary = {

    val summaryId = RemoteOps.fetch(ref, Seq("evaluate"), Seq(dataset)).asInstanceOf[String]

    new ClassificationSummary(new ModelRef(summaryId, ref.session, None), Nil)

  }

  override def copy(extra: ParamMap): FMClassificationModel =
    copyValues(new FMClassificationModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object FMClassificationModel extends MLReadable[FMClassificationModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.FMClassificationModel"

  override def load(path: String): FMClassificationModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new FMClassificationModel(id, ref))
}
