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
package org.apache.spark.ml.feature

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of UnivariateFeatureSelector. */
trait UnivariateFeatureSelectorParams extends HasFeaturesCol with HasLabelCol with HasOutputCol {

  final val featureType: Param[String] = new Param[String](
    this,
    "featureType",
    "The feature type.",
    ParamValidators.inArray(Array("categorical", "continuous"))
  )

  final val labelType: Param[String] = new Param[String](
    this,
    "labelType",
    "The label type.",
    ParamValidators.inArray(Array("categorical", "continuous"))
  )

  final val selectionMode: Param[String] = new Param[String](
    this,
    "selectionMode",
    "The selection mode.",
    ParamValidators.inArray(Array("numTopFeatures", "percentile", "fpr", "fdr", "fwe"))
  )

  final val selectionThreshold: DoubleParam = new DoubleParam(
    this,
    "selectionThreshold",
    "The upper bound of the features that selector will select."
  )

  setDefault(selectionMode -> "numTopFeatures")

  def setFeatureType(value: String): this.type = set(featureType, value)
  def getFeatureType: String = $(featureType)

  def setLabelType(value: String): this.type = set(labelType, value)
  def getLabelType: String = $(labelType)

  def setSelectionMode(value: String): this.type = set(selectionMode, value)
  def getSelectionMode: String = $(selectionMode)

  def setSelectionThreshold(value: Double): this.type = set(selectionThreshold, value)
  def getSelectionThreshold: Double = $(selectionThreshold)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Feature selector based on univariate statistical tests. Mirrors
 * `org.apache.spark.ml.feature.UnivariateFeatureSelector`.
 */
class UnivariateFeatureSelector(override val uid: String)
    extends Estimator[UnivariateFeatureSelectorModel]
    with UnivariateFeatureSelectorParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("uniFeatSel"))

  override def fit(dataset: Dataset[_]): UnivariateFeatureSelectorModel =
    RemoteOps.fit(
      this,
      UnivariateFeatureSelector.OperatorName,
      dataset,
      (id, ref) => new UnivariateFeatureSelectorModel(id, ref)
    )

  override def copy(extra: ParamMap): UnivariateFeatureSelector =
    copyValues(new UnivariateFeatureSelector(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object UnivariateFeatureSelector extends MLReadable[UnivariateFeatureSelector] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.UnivariateFeatureSelector"

  override def load(path: String): UnivariateFeatureSelector =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new UnivariateFeatureSelector(_)
    )
}

/**
 * The model fit by [[UnivariateFeatureSelector]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.UnivariateFeatureSelectorModel`.
 */
class UnivariateFeatureSelectorModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[UnivariateFeatureSelectorModel]
    with UnivariateFeatureSelectorParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `selectedFeatures`, read from the server. */
  def selectedFeatures: Array[Int] = attr[Array[Int]]("selectedFeatures")

  override def copy(extra: ParamMap): UnivariateFeatureSelectorModel =
    copyValues(new UnivariateFeatureSelectorModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object UnivariateFeatureSelectorModel extends MLReadable[UnivariateFeatureSelectorModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.feature.UnivariateFeatureSelectorModel"

  override def load(path: String): UnivariateFeatureSelectorModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new UnivariateFeatureSelectorModel(id, ref))
}
