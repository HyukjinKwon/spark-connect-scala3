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

/** Params of StringIndexer. */
trait StringIndexerParams extends HasInputCol with HasOutputCol {

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries. Options are skip (which will filter out rows with bad values), error (which will throw an error), or keep (put invalid data in a special additional bucket)",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  final val stringOrderType: Param[String] = new Param[String](
    this,
    "stringOrderType",
    "How to order labels of string column. Supported options: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc.",
    ParamValidators.inArray(Array("frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc"))
  )

  setDefault(handleInvalid -> "error", stringOrderType -> "frequencyDesc")

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setStringOrderType(value: String): this.type = set(stringOrderType, value)
  def getStringOrderType: String = $(stringOrderType)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * A label indexer that maps a string column of labels to an ML column of label indices. Mirrors
 * `org.apache.spark.ml.feature.StringIndexer`.
 */
class StringIndexer(override val uid: String)
    extends Estimator[StringIndexerModel]
    with StringIndexerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("strIdx"))

  override def fit(dataset: Dataset[_]): StringIndexerModel =
    RemoteOps.fit(
      this,
      StringIndexer.OperatorName,
      dataset,
      (id, ref) => new StringIndexerModel(id, ref)
    )

  override def copy(extra: ParamMap): StringIndexer = copyValues(new StringIndexer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object StringIndexer extends MLReadable[StringIndexer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.StringIndexer"

  override def load(path: String): StringIndexer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new StringIndexer(_)
    )
}

/**
 * The model fit by [[StringIndexer]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.StringIndexerModel`.
 */
class StringIndexerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[StringIndexerModel]
    with StringIndexerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `labels`, read from the server. */
  def labels: Array[String] = attr[Array[String]]("labels")

  override def copy(extra: ParamMap): StringIndexerModel =
    copyValues(new StringIndexerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object StringIndexerModel extends MLReadable[StringIndexerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.StringIndexerModel"

  override def load(path: String): StringIndexerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new StringIndexerModel(id, ref))
}
