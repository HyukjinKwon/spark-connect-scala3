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

/** Params of HashingTF. */
trait HashingTFParams extends HasInputCol with HasOutputCol with HasNumFeatures {

  final val binary: BooleanParam =
    new BooleanParam(this, "binary", "If true, all non zero counts are set to 1")

  setDefault(binary -> false)

  def setBinary(value: Boolean): this.type = set(binary, value)
  def getBinary: Boolean = $(binary)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

}

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick. Mirrors
 * `org.apache.spark.ml.feature.HashingTF`.
 */
class HashingTF(override val uid: String) extends Transformer with HashingTFParams with MLWritable {

  def this() = this(Identifiable.randomUID("hashingTF"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, HashingTF.OperatorName, dataset)

  override def copy(extra: ParamMap): HashingTF = copyValues(new HashingTF(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object HashingTF extends MLReadable[HashingTF] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.HashingTF"

  override def load(path: String): HashingTF =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new HashingTF(_)
    )
}
