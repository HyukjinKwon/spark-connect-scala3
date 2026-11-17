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

/** Params of VarianceThresholdSelector. */
trait VarianceThresholdSelectorParams extends HasFeaturesCol with HasOutputCol {

  final val varianceThreshold: DoubleParam = new DoubleParam(
    this,
    "varianceThreshold",
    "Param for variance threshold. Features with a variance not greater than this threshold will be removed. The default value is 0.0.",
    ParamValidators.gtEq(0.0)
  )

  setDefault(varianceThreshold -> 0.0)

  def setVarianceThreshold(value: Double): this.type = set(varianceThreshold, value)
  def getVarianceThreshold: Double = $(varianceThreshold)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Feature selector that removes all low-variance features. Mirrors
 * `org.apache.spark.ml.feature.VarianceThresholdSelector`.
 */
class VarianceThresholdSelector(override val uid: String)
    extends Estimator[VarianceThresholdSelectorModel]
    with VarianceThresholdSelectorParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("varSelector"))

  override def fit(dataset: Dataset[_]): VarianceThresholdSelectorModel =
    RemoteOps.fit(
      this,
      VarianceThresholdSelector.OperatorName,
      dataset,
      (id, ref) => new VarianceThresholdSelectorModel(id, ref)
    )

  override def copy(extra: ParamMap): VarianceThresholdSelector =
    copyValues(new VarianceThresholdSelector(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object VarianceThresholdSelector extends MLReadable[VarianceThresholdSelector] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VarianceThresholdSelector"

  override def load(path: String): VarianceThresholdSelector =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new VarianceThresholdSelector(_)
    )
}

/**
 * The model fit by [[VarianceThresholdSelector]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.VarianceThresholdSelectorModel`.
 */
class VarianceThresholdSelectorModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[VarianceThresholdSelectorModel]
    with VarianceThresholdSelectorParams
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

  override def copy(extra: ParamMap): VarianceThresholdSelectorModel =
    copyValues(new VarianceThresholdSelectorModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object VarianceThresholdSelectorModel extends MLReadable[VarianceThresholdSelectorModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.feature.VarianceThresholdSelectorModel"

  override def load(path: String): VarianceThresholdSelectorModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new VarianceThresholdSelectorModel(id, ref))
}
