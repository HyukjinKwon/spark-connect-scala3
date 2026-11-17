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

/** Params of VectorSlicer. */
trait VectorSlicerParams extends HasInputCol with HasOutputCol {

  final val indices: IntArrayParam = new IntArrayParam(
    this,
    "indices",
    "An array of indices to select features from a vector column."
  )

  final val names: StringArrayParam = new StringArrayParam(
    this,
    "names",
    "An array of feature names to select features from a vector column."
  )

  setDefault(indices -> Array.empty[Int], names -> Array.empty[String])

  def setIndices(value: Array[Int]): this.type = set(indices, value)
  def getIndices: Array[Int] = $(indices)

  def setNames(value: Array[String]): this.type = set(names, value)
  def getNames: Array[String] = $(names)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Takes a feature vector and outputs a new feature vector with a sub-array of the original
 * features. Mirrors `org.apache.spark.ml.feature.VectorSlicer`.
 */
class VectorSlicer(override val uid: String)
    extends Transformer
    with VectorSlicerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("vectorSlicer"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, VectorSlicer.OperatorName, dataset)

  override def copy(extra: ParamMap): VectorSlicer = copyValues(new VectorSlicer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object VectorSlicer extends MLReadable[VectorSlicer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.VectorSlicer"

  override def load(path: String): VectorSlicer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new VectorSlicer(_)
    )
}
