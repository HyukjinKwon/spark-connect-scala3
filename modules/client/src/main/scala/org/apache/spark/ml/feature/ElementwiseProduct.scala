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

/** Params of ElementwiseProduct. */
trait ElementwiseProductParams extends HasInputCol with HasOutputCol {

  final val scalingVec: Param[Vector] =
    new Param[Vector](this, "scalingVec", "the vector to multiply with input vectors")

  def setScalingVec(value: Vector): this.type = set(scalingVec, value)
  def getScalingVec: Vector = $(scalingVec)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Outputs the Hadamard product (i.e., the element-wise product) of each input vector with a
 * provided weight vector. Mirrors `org.apache.spark.ml.feature.ElementwiseProduct`.
 */
class ElementwiseProduct(override val uid: String)
    extends Transformer
    with ElementwiseProductParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("elemProd"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, ElementwiseProduct.OperatorName, dataset)

  override def copy(extra: ParamMap): ElementwiseProduct =
    copyValues(new ElementwiseProduct(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object ElementwiseProduct extends MLReadable[ElementwiseProduct] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.ElementwiseProduct"

  override def load(path: String): ElementwiseProduct =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new ElementwiseProduct(_)
    )
}
