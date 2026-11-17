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

/** Params of IsotonicRegression. */
trait IsotonicRegressionParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol {

  final val isotonic: BooleanParam = new BooleanParam(
    this,
    "isotonic",
    "whether the output sequence should be isotonic/increasing (true) or antitonic/decreasing (false)"
  )

  final val featureIndex: IntParam = new IntParam(
    this,
    "featureIndex",
    "The index of the feature if featuresCol is a vector column, no effect otherwise.",
    ParamValidators.gtEq(0)
  )

  setDefault(isotonic -> true, featureIndex -> 0)

  def setIsotonic(value: Boolean): this.type = set(isotonic, value)
  def getIsotonic: Boolean = $(isotonic)

  def setFeatureIndex(value: Int): this.type = set(featureIndex, value)
  def getFeatureIndex: Int = $(featureIndex)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

}

/** Isotonic regression. Mirrors `org.apache.spark.ml.regression.IsotonicRegression`. */
class IsotonicRegression(override val uid: String)
    extends Estimator[IsotonicRegressionModel]
    with IsotonicRegressionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("isoReg"))

  override def fit(dataset: Dataset[_]): IsotonicRegressionModel =
    RemoteOps.fit(
      this,
      IsotonicRegression.OperatorName,
      dataset,
      (id, ref) => new IsotonicRegressionModel(id, ref)
    )

  override def copy(extra: ParamMap): IsotonicRegression =
    copyValues(new IsotonicRegression(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object IsotonicRegression extends MLReadable[IsotonicRegression] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.IsotonicRegression"

  override def load(path: String): IsotonicRegression =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new IsotonicRegression(_)
    )
}

/**
 * The model fit by [[IsotonicRegression]], held on the server. Mirrors
 * `org.apache.spark.ml.regression.IsotonicRegressionModel`.
 */
class IsotonicRegressionModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[IsotonicRegressionModel]
    with IsotonicRegressionParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `boundaries`, read from the server. */
  def boundaries: Vector = attr[Vector]("boundaries")

  /** Model attribute `predictions`, read from the server. */
  def predictions: Vector = attr[Vector]("predictions")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Predicts the isotonic fit at `value`. */

  def predict(value: Double): Double = attrWith[Double]("predict", value)

  override def copy(extra: ParamMap): IsotonicRegressionModel =
    copyValues(new IsotonicRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object IsotonicRegressionModel extends MLReadable[IsotonicRegressionModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.IsotonicRegressionModel"

  override def load(path: String): IsotonicRegressionModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new IsotonicRegressionModel(id, ref))
}
