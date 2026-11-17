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

/** Params of VectorSizeHint. */
trait VectorSizeHintParams extends HasInputCol {

  final val size: IntParam =
    new IntParam(this, "size", "Size of vectors in column", ParamValidators.gtEq(0))

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid vectors in inputCol. Invalid vectors include nulls and vectors with the wrong size. The options are skip (filter out rows with invalid vectors), error (throw an error) and optimistic (do not check the vector size, and keep all rows)",
    ParamValidators.inArray(Array("skip", "error", "optimistic"))
  )

  setDefault(handleInvalid -> "error")

  def setSize(value: Int): this.type = set(size, value)
  def getSize: Int = $(size)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setInputCol(value: String): this.type = set(inputCol, value)

}

/**
 * A feature transformer that adds size information to the metadata of a vector column. Mirrors
 * `org.apache.spark.ml.feature.VectorSizeHint`.
 */
class VectorSizeHint(override val uid: String)
    extends Transformer
    with VectorSizeHintParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("vectorSizeHint"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, VectorSizeHint.OperatorName, dataset)

  override def copy(extra: ParamMap): VectorSizeHint = copyValues(new VectorSizeHint(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object VectorSizeHint extends MLReadable[VectorSizeHint] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VectorSizeHint"

  override def load(path: String): VectorSizeHint =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new VectorSizeHint(_)
    )
}
