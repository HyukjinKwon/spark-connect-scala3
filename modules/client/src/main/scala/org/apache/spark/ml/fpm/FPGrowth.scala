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
package org.apache.spark.ml.fpm

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of FPGrowth. */
trait FPGrowthParams extends HasPredictionCol {

  final val minSupport: DoubleParam = new DoubleParam(
    this,
    "minSupport",
    "the minimal support level (default 0.3)",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val minConfidence: DoubleParam = new DoubleParam(
    this,
    "minConfidence",
    "minimal confidence for generating Association Rule (default 0.8)",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val itemsCol: Param[String] = new Param[String](this, "itemsCol", "items column name")

  setDefault(minSupport -> 0.3, minConfidence -> 0.8, itemsCol -> "items")

  def setMinSupport(value: Double): this.type = set(minSupport, value)
  def getMinSupport: Double = $(minSupport)

  def setMinConfidence(value: Double): this.type = set(minConfidence, value)
  def getMinConfidence: Double = $(minConfidence)

  def setItemsCol(value: String): this.type = set(itemsCol, value)
  def getItemsCol: String = $(itemsCol)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

}

/**
 * A parallel FP-growth algorithm to mine frequent itemsets. Mirrors
 * `org.apache.spark.ml.fpm.FPGrowth`.
 */
class FPGrowth(override val uid: String)
    extends Estimator[FPGrowthModel]
    with FPGrowthParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("fpgrowth"))

  override def fit(dataset: Dataset[_]): FPGrowthModel =
    RemoteOps.fit(this, FPGrowth.OperatorName, dataset, (id, ref) => new FPGrowthModel(id, ref))

  override def copy(extra: ParamMap): FPGrowth = copyValues(new FPGrowth(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object FPGrowth extends MLReadable[FPGrowth] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.fpm.FPGrowth"

  override def load(path: String): FPGrowth =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new FPGrowth(_)
    )
}

/**
 * The model fit by [[FPGrowth]], held on the server. Mirrors
 * `org.apache.spark.ml.fpm.FPGrowthModel`.
 */
class FPGrowthModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[FPGrowthModel]
    with FPGrowthParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `associationRules`, a DataFrame on the server. */
  def associationRules: DataFrame = frame("associationRules")

  /** Model attribute `freqItemsets`, a DataFrame on the server. */
  def freqItemsets: DataFrame = frame("freqItemsets")

  override def copy(extra: ParamMap): FPGrowthModel =
    copyValues(new FPGrowthModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object FPGrowthModel extends MLReadable[FPGrowthModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.fpm.FPGrowthModel"

  override def load(path: String): FPGrowthModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new FPGrowthModel(id, ref))
}
