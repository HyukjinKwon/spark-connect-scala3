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

/** Params of Binarizer. */
trait BinarizerParams extends HasInputCol with HasOutputCol {

  final val threshold: DoubleParam = new DoubleParam(
    this,
    "threshold",
    "The threshold used to binarize continuous features. The features greater than the threshold, will be binarized to 1.0. The features equal to or less than the threshold, will be binarized to 0.0"
  )

  final val thresholds: DoubleArrayParam = new DoubleArrayParam(
    this,
    "thresholds",
    "Array of threshold used to binarize continuous features. This is for multiple columns input. If transforming multiple columns and thresholds is not set, but threshold is set, then threshold will be applied to all columns"
  )

  setDefault(threshold -> 0.0)

  def setThreshold(value: Double): this.type = set(threshold, value)
  def getThreshold: Double = $(threshold)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)
  def getThresholds: Array[Double] = $(thresholds)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Binarize a continuous column to 0 or 1 given a threshold. Mirrors
 * `org.apache.spark.ml.feature.Binarizer`.
 */
class Binarizer(override val uid: String) extends Transformer with BinarizerParams with MLWritable {

  def this() = this(Identifiable.randomUID("binarizer"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, Binarizer.OperatorName, dataset)

  override def copy(extra: ParamMap): Binarizer = copyValues(new Binarizer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object Binarizer extends MLReadable[Binarizer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Binarizer"

  override def load(path: String): Binarizer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new Binarizer(_)
    )
}
