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

/** Params of QuantileDiscretizer. */
trait QuantileDiscretizerParams extends HasInputCol with HasOutputCol with HasRelativeError {

  final val numBuckets: IntParam = new IntParam(
    this,
    "numBuckets",
    "Maximum number of buckets (quantiles, or categories) into which data points are grouped. Must be >= 2.",
    ParamValidators.gtEq(2)
  )

  final val numBucketsArray: IntArrayParam = new IntArrayParam(
    this,
    "numBucketsArray",
    "Array of number of buckets (quantiles, or categories) into which data points are grouped."
  )

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries. Options are skip (filter out rows with invalid data), error (throw an error), or keep (keep invalid values in a special additional bucket).",
    ParamValidators.inArray(Array("skip", "error", "keep"))
  )

  setDefault(numBuckets -> 2, handleInvalid -> "error", relativeError -> 0.001)

  def setNumBuckets(value: Int): this.type = set(numBuckets, value)
  def getNumBuckets: Int = $(numBuckets)

  def setNumBucketsArray(value: Array[Int]): this.type = set(numBucketsArray, value)
  def getNumBucketsArray: Array[Int] = $(numBucketsArray)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  def getHandleInvalid: String = $(handleInvalid)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setRelativeError(value: Double): this.type = set(relativeError, value)

}

/**
 * QuantileDiscretizer takes a column with continuous features and outputs a column with binned
 * categorical features. Mirrors `org.apache.spark.ml.feature.QuantileDiscretizer`.
 */
class QuantileDiscretizer(override val uid: String)
    extends Estimator[Bucketizer]
    with QuantileDiscretizerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("quantileDiscretizer"))

  override def fit(dataset: Dataset[_]): Bucketizer =
    RemoteOps.fit(
      this,
      QuantileDiscretizer.OperatorName,
      dataset,
      (id, ref) => new Bucketizer(id, ref)
    )

  override def copy(extra: ParamMap): QuantileDiscretizer =
    copyValues(new QuantileDiscretizer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object QuantileDiscretizer extends MLReadable[QuantileDiscretizer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.QuantileDiscretizer"

  override def load(path: String): QuantileDiscretizer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new QuantileDiscretizer(_)
    )
}
