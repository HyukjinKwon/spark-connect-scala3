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

/** Params of OneHotEncoder. */
trait OneHotEncoderParams extends HasInputCols with HasOutputCols {

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid data during transform(). Options are keep (invalid data presented as an extra categorical feature) or error (throw an error).",
    ParamValidators.inArray(Array("keep", "error"))
  )

  final val dropLast: BooleanParam =
    new BooleanParam(this, "dropLast", "whether to drop the last category")

  setDefault(handleInvalid -> "error", dropLast -> true)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setDropLast(value: Boolean): this.type = set(dropLast, value)
  def getDropLast: Boolean = $(dropLast)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

}

/**
 * A one-hot encoder that maps a column of category indices to a column of binary vectors. Mirrors
 * `org.apache.spark.ml.feature.OneHotEncoder`.
 */
class OneHotEncoder(override val uid: String)
    extends Estimator[OneHotEncoderModel]
    with OneHotEncoderParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("oneHot"))

  override def fit(dataset: Dataset[_]): OneHotEncoderModel =
    RemoteOps.fit(
      this,
      OneHotEncoder.OperatorName,
      dataset,
      (id, ref) => new OneHotEncoderModel(id, ref)
    )

  override def copy(extra: ParamMap): OneHotEncoder = copyValues(new OneHotEncoder(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object OneHotEncoder extends MLReadable[OneHotEncoder] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.OneHotEncoder"

  override def load(path: String): OneHotEncoder =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new OneHotEncoder(_)
    )
}

/**
 * The model fit by [[OneHotEncoder]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.OneHotEncoderModel`.
 */
class OneHotEncoderModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[OneHotEncoderModel]
    with OneHotEncoderParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `categorySizes`, read from the server. */
  def categorySizes: Array[Int] = attr[Array[Int]]("categorySizes")

  override def copy(extra: ParamMap): OneHotEncoderModel =
    copyValues(new OneHotEncoderModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object OneHotEncoderModel extends MLReadable[OneHotEncoderModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.OneHotEncoderModel"

  override def load(path: String): OneHotEncoderModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new OneHotEncoderModel(id, ref))
}
