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

/** Params of VectorIndexer. */
trait VectorIndexerParams extends HasInputCol with HasOutputCol {

  final val maxCategories: IntParam = new IntParam(
    this,
    "maxCategories",
    "Threshold for the number of values a categorical feature can take (>= 2).",
    ParamValidators.gtEq(2)
  )

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values).",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  setDefault(maxCategories -> 20, handleInvalid -> "error")

  def setMaxCategories(value: Int): this.type = set(maxCategories, value)
  def getMaxCategories: Int = $(maxCategories)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Class for indexing categorical feature columns in a dataset of Vector. Mirrors
 * `org.apache.spark.ml.feature.VectorIndexer`.
 */
class VectorIndexer(override val uid: String)
    extends Estimator[VectorIndexerModel]
    with VectorIndexerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("vecIdx"))

  override def fit(dataset: Dataset[_]): VectorIndexerModel =
    RemoteOps.fit(
      this,
      VectorIndexer.OperatorName,
      dataset,
      (id, ref) => new VectorIndexerModel(id, ref)
    )

  override def copy(extra: ParamMap): VectorIndexer = copyValues(new VectorIndexer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object VectorIndexer extends MLReadable[VectorIndexer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VectorIndexer"

  override def load(path: String): VectorIndexer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new VectorIndexer(_)
    )
}

/**
 * The model fit by [[VectorIndexer]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.VectorIndexerModel`.
 */
class VectorIndexerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[VectorIndexerModel]
    with VectorIndexerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Model attribute `categoryMapsDF`, a DataFrame on the server. */
  def categoryMapsDF: DataFrame = frame("categoryMapsDF")

  override def copy(extra: ParamMap): VectorIndexerModel =
    copyValues(new VectorIndexerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object VectorIndexerModel extends MLReadable[VectorIndexerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VectorIndexerModel"

  override def load(path: String): VectorIndexerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new VectorIndexerModel(id, ref))
}
