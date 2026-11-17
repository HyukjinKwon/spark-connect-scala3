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

/** Params of LinearSVC. */
trait LinearSVCParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasRawPredictionCol
    with HasMaxIter
    with HasRegParam
    with HasTol
    with HasFitIntercept
    with HasStandardization
    with HasAggregationDepth
    with HasMaxBlockSizeInMB
    with HasWeightCol {

  final val threshold: DoubleParam = new DoubleParam(
    this,
    "threshold",
    "The threshold in binary classification applied to the linear model prediction."
  )

  setDefault(threshold -> 0.0, maxIter -> 100, regParam -> 0.0, tol -> 1e-6)

  def setThreshold(value: Double): this.type = set(threshold, value)
  def getThreshold: Double = $(threshold)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  def setStandardization(value: Boolean): this.type = set(standardization, value)

  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

}

/**
 * This binary classifier optimizes the Hinge Loss using the OWLQN optimizer. Mirrors
 * `org.apache.spark.ml.classification.LinearSVC`.
 */
class LinearSVC(override val uid: String)
    extends Estimator[LinearSVCModel]
    with LinearSVCParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("linearsvc"))

  override def fit(dataset: Dataset[_]): LinearSVCModel =
    RemoteOps.fit(this, LinearSVC.OperatorName, dataset, (id, ref) => new LinearSVCModel(id, ref))

  override def copy(extra: ParamMap): LinearSVC = copyValues(new LinearSVC(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object LinearSVC extends MLReadable[LinearSVC] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.LinearSVC"

  override def load(path: String): LinearSVC =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new LinearSVC(_)
    )
}

/**
 * The model fit by [[LinearSVC]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.LinearSVCModel`.
 */
class LinearSVCModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[LinearSVCModel]
    with LinearSVCParams
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

  override def copy(extra: ParamMap): LinearSVCModel =
    copyValues(new LinearSVCModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object LinearSVCModel extends MLReadable[LinearSVCModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.LinearSVCModel"

  override def load(path: String): LinearSVCModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new LinearSVCModel(id, ref))
}
