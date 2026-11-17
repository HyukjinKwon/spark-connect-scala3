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

/** Params of StopWordsRemover. */
trait StopWordsRemoverParams extends HasInputCol with HasOutputCol {

  final val stopWords: StringArrayParam =
    new StringArrayParam(this, "stopWords", "The words to be filtered out")

  final val caseSensitive: BooleanParam = new BooleanParam(
    this,
    "caseSensitive",
    "whether to do a case sensitive comparison over the stop words"
  )

  final val locale: Param[String] = new Param[String](
    this,
    "locale",
    "locale of the input for case insensitive matching. Ignored when caseSensitive is true."
  )

  setDefault(caseSensitive -> false, locale -> "en_US")

  def getStopWords: Array[String] = get(stopWords).getOrElse(StopWordsDefaults.english())
  def setStopWords(value: Array[String]): this.type = set(stopWords, value)

  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)
  def getCaseSensitive: Boolean = $(caseSensitive)

  def setLocale(value: String): this.type = set(locale, value)
  def getLocale: String = $(locale)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * A feature transformer that filters out stop words from input. Mirrors
 * `org.apache.spark.ml.feature.StopWordsRemover`.
 */
class StopWordsRemover(override val uid: String)
    extends Transformer
    with StopWordsRemoverParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("stopWords"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, StopWordsRemover.OperatorName, dataset)

  override def copy(extra: ParamMap): StopWordsRemover =
    copyValues(new StopWordsRemover(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object StopWordsRemover extends MLReadable[StopWordsRemover] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.StopWordsRemover"

  override def load(path: String): StopWordsRemover =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new StopWordsRemover(_)
    )
}
