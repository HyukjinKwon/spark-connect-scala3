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

/** Params of MinMaxScaler. */
trait MinMaxScalerParams extends HasInputCol with HasOutputCol {

  final val min: DoubleParam =
    new DoubleParam(this, "min", "Lower bound of the output feature range")

  final val max: DoubleParam =
    new DoubleParam(this, "max", "Upper bound of the output feature range")

  setDefault(min -> 0.0, max -> 1.0)

  def setMin(value: Double): this.type = set(min, value)
  def getMin: Double = $(min)

  def setMax(value: Double): this.type = set(max, value)
  def getMax: Double = $(max)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Rescale each feature individually to a common range [min, max] linearly using column summary
 * statistics. Mirrors `org.apache.spark.ml.feature.MinMaxScaler`.
 */
class MinMaxScaler(override val uid: String)
    extends Estimator[MinMaxScalerModel]
    with MinMaxScalerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("minMaxScal"))

  override def fit(dataset: Dataset[_]): MinMaxScalerModel =
    RemoteOps.fit(
      this,
      MinMaxScaler.OperatorName,
      dataset,
      (id, ref) => new MinMaxScalerModel(id, ref)
    )

  override def copy(extra: ParamMap): MinMaxScaler = copyValues(new MinMaxScaler(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object MinMaxScaler extends MLReadable[MinMaxScaler] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.MinMaxScaler"

  override def load(path: String): MinMaxScaler =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new MinMaxScaler(_)
    )
}

/**
 * The model fit by [[MinMaxScaler]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.MinMaxScalerModel`.
 */
class MinMaxScalerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[MinMaxScalerModel]
    with MinMaxScalerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `originalMax`, read from the server. */
  def originalMax: Vector = attr[Vector]("originalMax")

  /** Model attribute `originalMin`, read from the server. */
  def originalMin: Vector = attr[Vector]("originalMin")

  override def copy(extra: ParamMap): MinMaxScalerModel =
    copyValues(new MinMaxScalerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object MinMaxScalerModel extends MLReadable[MinMaxScalerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.MinMaxScalerModel"

  override def load(path: String): MinMaxScalerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new MinMaxScalerModel(id, ref))
}
