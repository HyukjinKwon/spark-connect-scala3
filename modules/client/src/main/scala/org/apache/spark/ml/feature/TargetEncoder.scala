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

/** Params of TargetEncoder. */
trait TargetEncoderParams extends HasInputCols with HasOutputCols with HasLabelCol {

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid data during transform().",
    ParamValidators.inArray(Array("error", "keep"))
  )

  final val targetType: Param[String] = new Param[String](
    this,
    "targetType",
    "Whether the label is binary or continuous.",
    ParamValidators.inArray(Array("binary", "continuous"))
  )

  final val smoothing: DoubleParam = new DoubleParam(
    this,
    "smoothing",
    "Smoothing value that controls how much the category mean is shrunk toward the global mean.",
    ParamValidators.gtEq(0.0)
  )

  setDefault(handleInvalid -> "error", targetType -> "binary", smoothing -> 0.0)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setTargetType(value: String): this.type = set(targetType, value)
  def getTargetType: String = $(targetType)

  def setSmoothing(value: Double): this.type = set(smoothing, value)
  def getSmoothing: Double = $(smoothing)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

}

/**
 * Target Encoding maps a column of categorical indices to a column of target statistics. Mirrors
 * `org.apache.spark.ml.feature.TargetEncoder`.
 */
class TargetEncoder(override val uid: String)
    extends Estimator[TargetEncoderModel]
    with TargetEncoderParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("targetEnc"))

  override def fit(dataset: Dataset[_]): TargetEncoderModel =
    RemoteOps.fit(
      this,
      TargetEncoder.OperatorName,
      dataset,
      (id, ref) => new TargetEncoderModel(id, ref)
    )

  override def copy(extra: ParamMap): TargetEncoder = copyValues(new TargetEncoder(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object TargetEncoder extends MLReadable[TargetEncoder] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.TargetEncoder"

  override def load(path: String): TargetEncoder =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new TargetEncoder(_)
    )
}

/**
 * The model fit by [[TargetEncoder]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.TargetEncoderModel`.
 */
class TargetEncoderModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[TargetEncoderModel]
    with TargetEncoderParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  override def copy(extra: ParamMap): TargetEncoderModel =
    copyValues(new TargetEncoderModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object TargetEncoderModel extends MLReadable[TargetEncoderModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.TargetEncoderModel"

  override def load(path: String): TargetEncoderModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new TargetEncoderModel(id, ref))
}
