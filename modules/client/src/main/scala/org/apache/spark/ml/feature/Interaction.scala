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

/** Params of Interaction. */
trait InteractionParams extends HasInputCols with HasOutputCol {

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Implements the feature interaction transform of a set of input columns. Mirrors
 * `org.apache.spark.ml.feature.Interaction`.
 */
class Interaction(override val uid: String)
    extends Transformer
    with InteractionParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("interaction"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, Interaction.OperatorName, dataset)

  override def copy(extra: ParamMap): Interaction = copyValues(new Interaction(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object Interaction extends MLReadable[Interaction] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Interaction"

  override def load(path: String): Interaction =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new Interaction(_)
    )
}
