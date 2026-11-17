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

/** Params of NaiveBayes. */
trait NaiveBayesParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasRawPredictionCol
    with HasProbabilityCol
    with HasWeightCol {

  final val smoothing: DoubleParam = new DoubleParam(
    this,
    "smoothing",
    "The smoothing parameter, should be >= 0, default is 1.0",
    ParamValidators.gtEq(0.0)
  )

  final val modelType: Param[String] = new Param[String](
    this,
    "modelType",
    "The model type which is a string (case-sensitive). Supported options: multinomial (default), bernoulli and gaussian.",
    ParamValidators.inArray(Array("multinomial", "complement", "bernoulli", "gaussian"))
  )

  setDefault(smoothing -> 1.0, modelType -> "multinomial")

  def setSmoothing(value: Double): this.type = set(smoothing, value)
  def getSmoothing: Double = $(smoothing)

  def setModelType(value: String): this.type = set(modelType, value)
  def getModelType: String = $(modelType)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

}

/** Naive Bayes Classifiers. Mirrors `org.apache.spark.ml.classification.NaiveBayes`. */
class NaiveBayes(override val uid: String)
    extends Estimator[NaiveBayesModel]
    with NaiveBayesParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("nb"))

  override def fit(dataset: Dataset[_]): NaiveBayesModel =
    RemoteOps.fit(this, NaiveBayes.OperatorName, dataset, (id, ref) => new NaiveBayesModel(id, ref))

  override def copy(extra: ParamMap): NaiveBayes = copyValues(new NaiveBayes(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object NaiveBayes extends MLReadable[NaiveBayes] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.NaiveBayes"

  override def load(path: String): NaiveBayes =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new NaiveBayes(_)
    )
}

/**
 * The model fit by [[NaiveBayes]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.NaiveBayesModel`.
 */
class NaiveBayesModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[NaiveBayesModel]
    with NaiveBayesParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `pi`, read from the server. */
  def pi: Vector = attr[Vector]("pi")

  /** Model attribute `theta`, read from the server. */
  def theta: Matrix = attr[Matrix]("theta")

  /** Model attribute `sigma`, read from the server. */
  def sigma: Matrix = attr[Matrix]("sigma")

  override def copy(extra: ParamMap): NaiveBayesModel =
    copyValues(new NaiveBayesModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object NaiveBayesModel extends MLReadable[NaiveBayesModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.NaiveBayesModel"

  override def load(path: String): NaiveBayesModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new NaiveBayesModel(id, ref))
}
