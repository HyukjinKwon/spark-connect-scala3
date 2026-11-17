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

/** Params of VectorAssembler. */
trait VectorAssemblerParams extends HasInputCols with HasOutputCol {

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries containing NaN values or null. Options are error (throw an error), skip (filter out rows with bad values), or keep (keep invalid values in the output)",
    ParamValidators.inArray(Array("error", "skip", "keep"))
  )

  setDefault(handleInvalid -> "error")

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Combines a list of input columns into a single vector column. Mirrors
 * `org.apache.spark.ml.feature.VectorAssembler`.
 */
class VectorAssembler(override val uid: String)
    extends Transformer
    with VectorAssemblerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("va"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, VectorAssembler.OperatorName, dataset)

  override def copy(extra: ParamMap): VectorAssembler = copyValues(new VectorAssembler(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object VectorAssembler extends MLReadable[VectorAssembler] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VectorAssembler"

  override def load(path: String): VectorAssembler =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new VectorAssembler(_)
    )
}
