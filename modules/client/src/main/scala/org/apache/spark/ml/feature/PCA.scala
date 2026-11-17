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

/** Params of PCA. */
trait PCAParams extends HasInputCol with HasOutputCol {

  final val k: IntParam =
    new IntParam(this, "k", "the number of principal components (> 0)", ParamValidators.gt(0))

  def setK(value: Int): this.type = set(k, value)
  def getK: Int = $(k)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * PCA trains a model to project vectors to a lower dimensional space of the top k principal
 * components. Mirrors `org.apache.spark.ml.feature.PCA`.
 */
class PCA(override val uid: String) extends Estimator[PCAModel] with PCAParams with MLWritable {

  def this() = this(Identifiable.randomUID("pca"))

  override def fit(dataset: Dataset[_]): PCAModel =
    RemoteOps.fit(this, PCA.OperatorName, dataset, (id, ref) => new PCAModel(id, ref))

  override def copy(extra: ParamMap): PCA = copyValues(new PCA(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object PCA extends MLReadable[PCA] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.PCA"

  override def load(path: String): PCA =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new PCA(_)
    )
}

/** The model fit by [[PCA]], held on the server. Mirrors `org.apache.spark.ml.feature.PCAModel`. */
class PCAModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[PCAModel]
    with PCAParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `pc`, read from the server. */
  def pc: Matrix = attr[Matrix]("pc")

  /** Model attribute `explainedVariance`, read from the server. */
  def explainedVariance: Vector = attr[Vector]("explainedVariance")

  override def copy(extra: ParamMap): PCAModel =
    copyValues(new PCAModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object PCAModel extends MLReadable[PCAModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.PCAModel"

  override def load(path: String): PCAModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new PCAModel(id, ref))
}
