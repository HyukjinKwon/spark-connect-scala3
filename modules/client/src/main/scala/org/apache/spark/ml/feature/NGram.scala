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

/** Params of NGram. */
trait NGramParams extends HasInputCol with HasOutputCol {

  final val n: IntParam =
    new IntParam(this, "n", "number of elements per n-gram (>=1)", ParamValidators.gtEq(1))

  setDefault(n -> 2)

  def setN(value: Int): this.type = set(n, value)
  def getN: Int = $(n)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * A feature transformer that converts the input array of strings into an array of n-grams. Mirrors
 * `org.apache.spark.ml.feature.NGram`.
 */
class NGram(override val uid: String) extends Transformer with NGramParams with MLWritable {

  def this() = this(Identifiable.randomUID("ngram"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, NGram.OperatorName, dataset)

  override def copy(extra: ParamMap): NGram = copyValues(new NGram(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object NGram extends MLReadable[NGram] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.NGram"

  override def load(path: String): NGram =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new NGram(_)
    )
}
