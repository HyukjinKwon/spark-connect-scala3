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

/** Params of MultilayerPerceptronClassifier. */
trait MultilayerPerceptronClassifierParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasMaxIter
    with HasTol
    with HasSeed
    with HasThresholds
    with HasProbabilityCol
    with HasRawPredictionCol {

  final val layers: IntArrayParam = new IntArrayParam(
    this,
    "layers",
    "Sizes of layers from input layer to output layer. E.g., Array(780, 100, 10) means 780 inputs, one hidden layer with 100 neurons and output layer of 10 neurons."
  )

  final val blockSize: IntParam = new IntParam(
    this,
    "blockSize",
    "Block size for stacking input data in matrices.",
    ParamValidators.gt(0)
  )

  final val solver: Param[String] = new Param[String](
    this,
    "solver",
    "The solver algorithm for optimization.",
    ParamValidators.inArray(Array("l-bfgs", "gd"))
  )

  final val stepSize: DoubleParam = new DoubleParam(
    this,
    "stepSize",
    "Step size to be used for each iteration of optimization (> 0).",
    ParamValidators.gt(0.0)
  )

  final val initialWeights: Param[Vector] =
    new Param[Vector](this, "initialWeights", "The initial weights of the model.")

  setDefault(blockSize -> 128, solver -> "l-bfgs", stepSize -> 0.03, maxIter -> 100, tol -> 1e-6)

  def setLayers(value: Array[Int]): this.type = set(layers, value)
  def getLayers: Array[Int] = $(layers)

  def setBlockSize(value: Int): this.type = set(blockSize, value)
  def getBlockSize: Int = $(blockSize)

  def setSolver(value: String): this.type = set(solver, value)
  def getSolver: String = $(solver)

  def setStepSize(value: Double): this.type = set(stepSize, value)
  def getStepSize: Double = $(stepSize)

  def setInitialWeights(value: Vector): this.type = set(initialWeights, value)
  def getInitialWeights: Vector = $(initialWeights)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

}

/**
 * Classifier trainer based on the Multilayer Perceptron. Mirrors
 * `org.apache.spark.ml.classification.MultilayerPerceptronClassifier`.
 */
class MultilayerPerceptronClassifier(override val uid: String)
    extends Estimator[MultilayerPerceptronClassificationModel]
    with MultilayerPerceptronClassifierParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("mlpc"))

  override def fit(dataset: Dataset[_]): MultilayerPerceptronClassificationModel =
    RemoteOps.fit(
      this,
      MultilayerPerceptronClassifier.OperatorName,
      dataset,
      (id, ref) => new MultilayerPerceptronClassificationModel(id, ref)
    )

  override def copy(extra: ParamMap): MultilayerPerceptronClassifier =
    copyValues(new MultilayerPerceptronClassifier(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object MultilayerPerceptronClassifier extends MLReadable[MultilayerPerceptronClassifier] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"

  override def load(path: String): MultilayerPerceptronClassifier =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new MultilayerPerceptronClassifier(_)
    )
}

/**
 * The model fit by [[MultilayerPerceptronClassifier]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel`.
 */
class MultilayerPerceptronClassificationModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[MultilayerPerceptronClassificationModel]
    with MultilayerPerceptronClassifierParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `weights`, read from the server. */
  def weights: Vector = attr[Vector]("weights")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Model attribute `numClasses`, read from the server. */
  def numClasses: Int = attr[Int]("numClasses")

  /** Evaluates the model on `dataset`, returning a summary of the predictions. */

  def evaluate(dataset: Dataset[_]): ClassificationSummary = {

    val summaryId = RemoteOps.fetch(ref, Seq("evaluate"), Seq(dataset)).asInstanceOf[String]

    new ClassificationSummary(new ModelRef(summaryId, ref.session, None), Nil)

  }

  override def copy(extra: ParamMap): MultilayerPerceptronClassificationModel =
    copyValues(new MultilayerPerceptronClassificationModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object MultilayerPerceptronClassificationModel
    extends MLReadable[MultilayerPerceptronClassificationModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel"

  override def load(path: String): MultilayerPerceptronClassificationModel =
    RemoteIO.loadModel(
      OperatorName,
      path,
      (id, ref) => new MultilayerPerceptronClassificationModel(id, ref)
    )
}
