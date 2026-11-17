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

/** Params of StandardScaler. */
trait StandardScalerParams extends HasInputCol with HasOutputCol {

  final val withMean: BooleanParam =
    new BooleanParam(this, "withMean", "Whether to center the data with mean before scaling.")

  final val withStd: BooleanParam =
    new BooleanParam(this, "withStd", "Whether to scale the data to unit standard deviation.")

  setDefault(withMean -> false, withStd -> true)

  def setWithMean(value: Boolean): this.type = set(withMean, value)
  def getWithMean: Boolean = $(withMean)

  def setWithStd(value: Boolean): this.type = set(withStd, value)
  def getWithStd: Boolean = $(withStd)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Standardizes features by removing the mean and scaling to unit variance. Mirrors
 * `org.apache.spark.ml.feature.StandardScaler`.
 */
class StandardScaler(override val uid: String)
    extends Estimator[StandardScalerModel]
    with StandardScalerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("stdScal"))

  override def fit(dataset: Dataset[_]): StandardScalerModel =
    RemoteOps.fit(
      this,
      StandardScaler.OperatorName,
      dataset,
      (id, ref) => new StandardScalerModel(id, ref)
    )

  override def copy(extra: ParamMap): StandardScaler = copyValues(new StandardScaler(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object StandardScaler extends MLReadable[StandardScaler] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.StandardScaler"

  override def load(path: String): StandardScaler =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new StandardScaler(_)
    )
}

/**
 * The model fit by [[StandardScaler]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.StandardScalerModel`.
 */
class StandardScalerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[StandardScalerModel]
    with StandardScalerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `mean`, read from the server. */
  def mean: Vector = attr[Vector]("mean")

  /** Model attribute `std`, read from the server. */
  def std: Vector = attr[Vector]("std")

  override def copy(extra: ParamMap): StandardScalerModel =
    copyValues(new StandardScalerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object StandardScalerModel extends MLReadable[StandardScalerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.StandardScalerModel"

  override def load(path: String): StandardScalerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new StandardScalerModel(id, ref))
}
