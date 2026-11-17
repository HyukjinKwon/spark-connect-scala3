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

/** Params of SQLTransformer. */
trait SQLTransformerParams extends Params {

  final val statement: Param[String] = new Param[String](
    this,
    "statement",
    "SQL statement parameter. The statement is a SELECT query that refers to the input dataset as __THIS__."
  )

  def setStatement(value: String): this.type = set(statement, value)
  def getStatement: String = $(statement)

}

/**
 * Implements the transformations which are defined by SQL statement. Mirrors
 * `org.apache.spark.ml.feature.SQLTransformer`.
 */
class SQLTransformer(override val uid: String)
    extends Transformer
    with SQLTransformerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("sql"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, SQLTransformer.OperatorName, dataset)

  override def copy(extra: ParamMap): SQLTransformer = copyValues(new SQLTransformer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object SQLTransformer extends MLReadable[SQLTransformer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.SQLTransformer"

  override def load(path: String): SQLTransformer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new SQLTransformer(_)
    )
}
