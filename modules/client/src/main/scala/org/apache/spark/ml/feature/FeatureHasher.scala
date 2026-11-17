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

/** Params of FeatureHasher. */
trait FeatureHasherParams extends HasInputCols with HasOutputCol with HasNumFeatures {

  final val categoricalCols: StringArrayParam = new StringArrayParam(
    this,
    "categoricalCols",
    "numeric columns to treat as categorical features"
  )

  def setCategoricalCols(value: Array[String]): this.type = set(categoricalCols, value)
  def getCategoricalCols: Array[String] = $(categoricalCols)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

}

/**
 * Feature hashing projects a set of categorical or numerical features into a feature vector of
 * specified dimension. Mirrors `org.apache.spark.ml.feature.FeatureHasher`.
 */
class FeatureHasher(override val uid: String)
    extends Transformer
    with FeatureHasherParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("featureHasher"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, FeatureHasher.OperatorName, dataset)

  override def copy(extra: ParamMap): FeatureHasher = copyValues(new FeatureHasher(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object FeatureHasher extends MLReadable[FeatureHasher] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.FeatureHasher"

  override def load(path: String): FeatureHasher =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new FeatureHasher(_)
    )
}
