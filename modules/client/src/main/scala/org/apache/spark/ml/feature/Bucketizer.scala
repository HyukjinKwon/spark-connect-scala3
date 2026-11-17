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
import org.apache.spark.ml.Model
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of [[Bucketizer]]. */
trait BucketizerParams
    extends Params
    with HasInputCol
    with HasOutputCol
    with HasInputCols
    with HasOutputCols {

  /**
   * Split points for splitting a continuous feature into buckets. Must be monotonically increasing.
   */
  final val splits: DoubleArrayParam = new DoubleArrayParam(
    this,
    "splits",
    "Split points for splitting a continuous feature into buckets. With n+1 splits, there are n " +
      "buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last " +
      "bucket, which also includes y. The splits should be of length >= 3 and strictly increasing.",
    (splits: Array[Double]) => splits.length >= 3 && splits.sliding(2).forall(p => p(0) < p(1))
  )

  /** How to handle invalid entries: `skip`, `error`, or `keep`. */
  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries containing NaN values. Values outside the splits will always " +
      "be treated as errors. Options are skip (filter out rows with invalid values), error (throw " +
      "an error), or keep (keep invalid values in a special additional bucket).",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  setDefault(handleInvalid -> "error")

  def getSplits: Array[Double] = $(splits)

  def getHandleInvalid: String = $(handleInvalid)
}

/**
 * Maps continuous features to bucket indices. When produced by [[QuantileDiscretizer]] it is a
 * fitted model held on the server; otherwise it is a transformer that the server runs from its
 * params. Mirrors `org.apache.spark.ml.feature.Bucketizer`.
 */
class Bucketizer private[ml] (override val uid: String, private[ml] val remote: ModelRef)
    extends Model[Bucketizer]
    with BucketizerParams
    with RemoteModelLike
    with MLWritable {

  def this(uid: String) = this(uid, null)

  def this() = this(Identifiable.randomUID("bucketizer"), null)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setSplits(value: Array[Double]): this.type = set(splits, value)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  private[ml] def ref: ModelRef = remote

  override def transform(dataset: Dataset[_]): DataFrame =
    if (remote != null) RemoteOps.transformWithModel(this, remote, dataset)
    else RemoteOps.transformWithTransformer(this, Bucketizer.OperatorName, dataset)

  override def copy(extra: ParamMap): Bucketizer = {
    val copied =
      copyValues(new Bucketizer(uid, if (remote != null) remote.retain() else null), extra)
    copied
  }

  override def release(): Unit = if (remote != null) remote.release()

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
