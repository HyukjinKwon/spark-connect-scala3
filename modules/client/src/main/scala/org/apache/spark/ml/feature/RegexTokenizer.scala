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

/** Params of RegexTokenizer. */
trait RegexTokenizerParams extends HasInputCol with HasOutputCol {

  final val minTokenLength: IntParam =
    new IntParam(this, "minTokenLength", "minimum token length (>= 0)", ParamValidators.gtEq(0))

  final val gaps: BooleanParam =
    new BooleanParam(this, "gaps", "whether regex splits on gaps (true) or matches tokens (false)")

  final val pattern: Param[String] = new Param[String](
    this,
    "pattern",
    "regex pattern used to match delimiters if gaps is true or tokens if gaps is false"
  )

  final val toLowercase: BooleanParam = new BooleanParam(
    this,
    "toLowercase",
    "whether to convert all characters to lowercase before tokenizing"
  )

  setDefault(minTokenLength -> 1, gaps -> true, pattern -> "\\s+", toLowercase -> true)

  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)
  def getMinTokenLength: Int = $(minTokenLength)

  def setGaps(value: Boolean): this.type = set(gaps, value)
  def getGaps: Boolean = $(gaps)

  def setPattern(value: String): this.type = set(pattern, value)
  def getPattern: String = $(pattern)

  def setToLowercase(value: Boolean): this.type = set(toLowercase, value)
  def getToLowercase: Boolean = $(toLowercase)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * A regex based tokenizer that splits the input string using a regex pattern. Mirrors
 * `org.apache.spark.ml.feature.RegexTokenizer`.
 */
class RegexTokenizer(override val uid: String)
    extends Transformer
    with RegexTokenizerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("regexTok"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, RegexTokenizer.OperatorName, dataset)

  override def copy(extra: ParamMap): RegexTokenizer = copyValues(new RegexTokenizer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RegexTokenizer extends MLReadable[RegexTokenizer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.RegexTokenizer"

  override def load(path: String): RegexTokenizer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new RegexTokenizer(_)
    )
}
