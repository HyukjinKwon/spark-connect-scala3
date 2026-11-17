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

/** Params of IDF. */
trait IDFParams extends HasInputCol with HasOutputCol {

  final val minDocFreq: IntParam = new IntParam(
    this,
    "minDocFreq",
    "minimum number of documents in which a term should appear for filtering (>= 0)",
    ParamValidators.gtEq(0)
  )

  setDefault(minDocFreq -> 0)

  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)
  def getMinDocFreq: Int = $(minDocFreq)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Compute the Inverse Document Frequency (IDF) given a collection of documents. Mirrors
 * `org.apache.spark.ml.feature.IDF`.
 */
class IDF(override val uid: String) extends Estimator[IDFModel] with IDFParams with MLWritable {

  def this() = this(Identifiable.randomUID("idf"))

  override def fit(dataset: Dataset[_]): IDFModel =
    RemoteOps.fit(this, IDF.OperatorName, dataset, (id, ref) => new IDFModel(id, ref))

  override def copy(extra: ParamMap): IDF = copyValues(new IDF(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object IDF extends MLReadable[IDF] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.IDF"

  override def load(path: String): IDF =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new IDF(_)
    )
}

/** The model fit by [[IDF]], held on the server. Mirrors `org.apache.spark.ml.feature.IDFModel`. */
class IDFModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[IDFModel]
    with IDFParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `idf`, read from the server. */
  def idf: Vector = attr[Vector]("idf")

  /** Model attribute `docFreq`, read from the server. */
  def docFreq: Array[Long] = attr[Array[Long]]("docFreq")

  /** Model attribute `numDocs`, read from the server. */
  def numDocs: Long = attr[Long]("numDocs")

  override def copy(extra: ParamMap): IDFModel =
    copyValues(new IDFModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object IDFModel extends MLReadable[IDFModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.IDFModel"

  override def load(path: String): IDFModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new IDFModel(id, ref))
}
