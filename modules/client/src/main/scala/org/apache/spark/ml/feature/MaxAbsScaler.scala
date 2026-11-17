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

/** Params of MaxAbsScaler. */
trait MaxAbsScalerParams extends HasInputCol with HasOutputCol {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
 * absolute value in each feature. Mirrors `org.apache.spark.ml.feature.MaxAbsScaler`.
 */
class MaxAbsScaler(override val uid: String)
    extends Estimator[MaxAbsScalerModel]
    with MaxAbsScalerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("maxAbsScal"))

  override def fit(dataset: Dataset[_]): MaxAbsScalerModel =
    RemoteOps.fit(
      this,
      MaxAbsScaler.OperatorName,
      dataset,
      (id, ref) => new MaxAbsScalerModel(id, ref)
    )

  override def copy(extra: ParamMap): MaxAbsScaler = copyValues(new MaxAbsScaler(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object MaxAbsScaler extends MLReadable[MaxAbsScaler] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.MaxAbsScaler"

  override def load(path: String): MaxAbsScaler =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new MaxAbsScaler(_)
    )
}

/**
 * The model fit by [[MaxAbsScaler]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.MaxAbsScalerModel`.
 */
class MaxAbsScalerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[MaxAbsScalerModel]
    with MaxAbsScalerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `maxAbs`, read from the server. */
  def maxAbs: Vector = attr[Vector]("maxAbs")

  override def copy(extra: ParamMap): MaxAbsScalerModel =
    copyValues(new MaxAbsScalerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object MaxAbsScalerModel extends MLReadable[MaxAbsScalerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.MaxAbsScalerModel"

  override def load(path: String): MaxAbsScalerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new MaxAbsScalerModel(id, ref))
}
