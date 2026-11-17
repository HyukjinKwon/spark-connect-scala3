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

/** Params of IndexToString. */
trait IndexToStringParams extends HasInputCol with HasOutputCol {

  final val labels: StringArrayParam = new StringArrayParam(
    this,
    "labels",
    "Optional array of labels specifying index-string mapping."
  )

  def setLabels(value: Array[String]): this.type = set(labels, value)
  def getLabels: Array[String] = $(labels)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * A Transformer that maps a column of indices back to a new column of corresponding string values.
 * Mirrors `org.apache.spark.ml.feature.IndexToString`.
 */
class IndexToString(override val uid: String)
    extends Transformer
    with IndexToStringParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("idxToStr"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, IndexToString.OperatorName, dataset)

  override def copy(extra: ParamMap): IndexToString = copyValues(new IndexToString(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object IndexToString extends MLReadable[IndexToString] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.IndexToString"

  override def load(path: String): IndexToString =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new IndexToString(_)
    )
}
