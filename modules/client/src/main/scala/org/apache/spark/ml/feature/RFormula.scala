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

/** Params of RFormula. */
trait RFormulaParams extends HasFeaturesCol with HasLabelCol {

  final val formula: Param[String] = new Param[String](this, "formula", "R model formula")

  final val forceIndexLabel: BooleanParam = new BooleanParam(
    this,
    "forceIndexLabel",
    "Force to index label whether it is numeric or string"
  )

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries. Options are skip (which will filter out rows with bad values), or error (which will throw an error), or keep (which will keep invalid values)",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  final val stringIndexerOrderType: Param[String] = new Param[String](
    this,
    "stringIndexerOrderType",
    "How to order categories of a string feature column.",
    ParamValidators.inArray(Array("frequencyDesc", "frequencyAsc", "alphabetDesc", "alphabetAsc"))
  )

  setDefault(
    forceIndexLabel -> false,
    handleInvalid -> "error",
    stringIndexerOrderType -> "frequencyDesc"
  )

  def setFormula(value: String): this.type = set(formula, value)
  def getFormula: String = $(formula)

  def setForceIndexLabel(value: Boolean): this.type = set(forceIndexLabel, value)
  def getForceIndexLabel: Boolean = $(forceIndexLabel)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setStringIndexerOrderType(value: String): this.type = set(stringIndexerOrderType, value)
  def getStringIndexerOrderType: String = $(stringIndexerOrderType)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

}

/**
 * Implements the transforms required for fitting a dataset against an R model formula. Mirrors
 * `org.apache.spark.ml.feature.RFormula`.
 */
class RFormula(override val uid: String)
    extends Estimator[RFormulaModel]
    with RFormulaParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("rFormula"))

  override def fit(dataset: Dataset[_]): RFormulaModel =
    RemoteOps.fit(this, RFormula.OperatorName, dataset, (id, ref) => new RFormulaModel(id, ref))

  override def copy(extra: ParamMap): RFormula = copyValues(new RFormula(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RFormula extends MLReadable[RFormula] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.RFormula"

  override def load(path: String): RFormula =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new RFormula(_)
    )
}

/**
 * The model fit by [[RFormula]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.RFormulaModel`.
 */
class RFormulaModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[RFormulaModel]
    with RFormulaParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `resolvedFormulaString`, read from the server. */
  def resolvedFormulaString: String = attr[String]("resolvedFormulaString")

  override def copy(extra: ParamMap): RFormulaModel =
    copyValues(new RFormulaModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object RFormulaModel extends MLReadable[RFormulaModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.RFormulaModel"

  override def load(path: String): RFormulaModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new RFormulaModel(id, ref))
}
