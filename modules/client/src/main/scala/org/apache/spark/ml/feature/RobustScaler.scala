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

/** Params of RobustScaler. */
trait RobustScalerParams extends HasInputCol with HasOutputCol with HasRelativeError {

  final val lower: DoubleParam = new DoubleParam(
    this,
    "lower",
    "Lower quantile to calculate quantile range",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = false)
  )

  final val upper: DoubleParam = new DoubleParam(
    this,
    "upper",
    "Upper quantile to calculate quantile range",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = false)
  )

  final val withCentering: BooleanParam =
    new BooleanParam(this, "withCentering", "Whether to center data with median")

  final val withScaling: BooleanParam =
    new BooleanParam(this, "withScaling", "Whether to scale the data to quantile range")

  setDefault(
    lower -> 0.25,
    upper -> 0.75,
    withCentering -> false,
    withScaling -> true,
    relativeError -> 0.001
  )

  def setLower(value: Double): this.type = set(lower, value)
  def getLower: Double = $(lower)

  def setUpper(value: Double): this.type = set(upper, value)
  def getUpper: Double = $(upper)

  def setWithCentering(value: Boolean): this.type = set(withCentering, value)
  def getWithCentering: Boolean = $(withCentering)

  def setWithScaling(value: Boolean): this.type = set(withScaling, value)
  def getWithScaling: Boolean = $(withScaling)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setRelativeError(value: Double): this.type = set(relativeError, value)

}

/**
 * Scale features using statistics that are robust to outliers. Mirrors
 * `org.apache.spark.ml.feature.RobustScaler`.
 */
class RobustScaler(override val uid: String)
    extends Estimator[RobustScalerModel]
    with RobustScalerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("robustScal"))

  override def fit(dataset: Dataset[_]): RobustScalerModel =
    RemoteOps.fit(
      this,
      RobustScaler.OperatorName,
      dataset,
      (id, ref) => new RobustScalerModel(id, ref)
    )

  override def copy(extra: ParamMap): RobustScaler = copyValues(new RobustScaler(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RobustScaler extends MLReadable[RobustScaler] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.RobustScaler"

  override def load(path: String): RobustScaler =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new RobustScaler(_)
    )
}

/**
 * The model fit by [[RobustScaler]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.RobustScalerModel`.
 */
class RobustScalerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[RobustScalerModel]
    with RobustScalerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `range`, read from the server. */
  def range: Vector = attr[Vector]("range")

  /** Model attribute `median`, read from the server. */
  def median: Vector = attr[Vector]("median")

  override def copy(extra: ParamMap): RobustScalerModel =
    copyValues(new RobustScalerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object RobustScalerModel extends MLReadable[RobustScalerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.RobustScalerModel"

  override def load(path: String): RobustScalerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new RobustScalerModel(id, ref))
}
