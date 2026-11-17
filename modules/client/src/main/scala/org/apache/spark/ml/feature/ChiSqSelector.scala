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

/** Params of ChiSqSelector. */
trait ChiSqSelectorParams extends HasFeaturesCol with HasLabelCol with HasOutputCol {

  final val selectorType: Param[String] = new Param[String](
    this,
    "selectorType",
    "The selector type. Supported options: numTopFeatures (default), percentile, fpr, fdr, fwe.",
    ParamValidators.inArray(Array("numTopFeatures", "percentile", "fpr", "fdr", "fwe"))
  )

  final val numTopFeatures: IntParam = new IntParam(
    this,
    "numTopFeatures",
    "Number of features that selector will select, ordered by ascending p-value.",
    ParamValidators.gtEq(1)
  )

  final val percentile: DoubleParam = new DoubleParam(
    this,
    "percentile",
    "Percentile of features that selector will select, ordered by ascending p-value.",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val fpr: DoubleParam = new DoubleParam(
    this,
    "fpr",
    "The highest p-value for features to be kept.",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val fdr: DoubleParam = new DoubleParam(
    this,
    "fdr",
    "The upper bound of the expected false discovery rate.",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val fwe: DoubleParam = new DoubleParam(
    this,
    "fwe",
    "The upper bound of the expected family-wise error rate.",
    ParamValidators.inRange(0.0, 1.0)
  )

  setDefault(
    selectorType -> "numTopFeatures",
    numTopFeatures -> 50,
    percentile -> 0.1,
    fpr -> 0.05,
    fdr -> 0.05,
    fwe -> 0.05
  )

  def setSelectorType(value: String): this.type = set(selectorType, value)
  def getSelectorType: String = $(selectorType)

  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)
  def getNumTopFeatures: Int = $(numTopFeatures)

  def setPercentile(value: Double): this.type = set(percentile, value)
  def getPercentile: Double = $(percentile)

  def setFpr(value: Double): this.type = set(fpr, value)
  def getFpr: Double = $(fpr)

  def setFdr(value: Double): this.type = set(fdr, value)
  def getFdr: Double = $(fdr)

  def setFwe(value: Double): this.type = set(fwe, value)
  def getFwe: Double = $(fwe)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Chi-Squared feature selection, which selects categorical features to use for predicting a
 * categorical label. Mirrors `org.apache.spark.ml.feature.ChiSqSelector`.
 */
class ChiSqSelector(override val uid: String)
    extends Estimator[ChiSqSelectorModel]
    with ChiSqSelectorParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("chiSqSelector"))

  override def fit(dataset: Dataset[_]): ChiSqSelectorModel =
    RemoteOps.fit(
      this,
      ChiSqSelector.OperatorName,
      dataset,
      (id, ref) => new ChiSqSelectorModel(id, ref)
    )

  override def copy(extra: ParamMap): ChiSqSelector = copyValues(new ChiSqSelector(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object ChiSqSelector extends MLReadable[ChiSqSelector] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.ChiSqSelector"

  override def load(path: String): ChiSqSelector =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new ChiSqSelector(_)
    )
}

/**
 * The model fit by [[ChiSqSelector]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.ChiSqSelectorModel`.
 */
class ChiSqSelectorModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[ChiSqSelectorModel]
    with ChiSqSelectorParams
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

  override def copy(extra: ParamMap): ChiSqSelectorModel =
    copyValues(new ChiSqSelectorModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object ChiSqSelectorModel extends MLReadable[ChiSqSelectorModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.ChiSqSelectorModel"

  override def load(path: String): ChiSqSelectorModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new ChiSqSelectorModel(id, ref))
}
