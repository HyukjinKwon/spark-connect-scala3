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

/** Params of Bucketizer. */
trait BucketizerParams extends HasInputCol with HasOutputCol {

  final val splits: DoubleArrayParam = new DoubleArrayParam(
    this,
    "splits",
    "Split points for splitting a continuous feature into buckets. Must be monotonically increasing."
  )

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries containing NaN values. Options are skip (filter out rows), error (throw an error), or keep (keep invalid values in a special additional bucket)",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  setDefault(handleInvalid -> "error")

  def setSplits(value: Array[Double]): this.type = set(splits, value)
  def getSplits: Array[Double] = $(splits)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Maps a column of continuous features to a column of feature buckets. Mirrors
 * `org.apache.spark.ml.feature.Bucketizer`.
 */
class Bucketizer(override val uid: String)
    extends Transformer
    with BucketizerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("bucketizer"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, Bucketizer.OperatorName, dataset)

  override def copy(extra: ParamMap): Bucketizer = copyValues(new Bucketizer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object Bucketizer extends MLReadable[Bucketizer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Bucketizer"

  override def load(path: String): Bucketizer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new Bucketizer(_)
    )
}
