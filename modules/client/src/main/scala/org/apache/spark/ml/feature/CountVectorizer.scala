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

/** Params of CountVectorizer. */
trait CountVectorizerParams extends HasInputCol with HasOutputCol {

  final val vocabSize: IntParam =
    new IntParam(this, "vocabSize", "max size of the vocabulary", ParamValidators.gt(0))

  final val minDF: DoubleParam = new DoubleParam(
    this,
    "minDF",
    "Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.",
    ParamValidators.gtEq(0.0)
  )

  final val maxDF: DoubleParam = new DoubleParam(
    this,
    "maxDF",
    "Specifies the maximum number of different documents a term could appear in to be included in the vocabulary.",
    ParamValidators.gtEq(0.0)
  )

  final val minTF: DoubleParam = new DoubleParam(
    this,
    "minTF",
    "Filter to ignore rare words in a document.",
    ParamValidators.gtEq(0.0)
  )

  final val binary: BooleanParam =
    new BooleanParam(this, "binary", "Binary toggle to control the output vector values.")

  setDefault(
    vocabSize -> (1 << 18),
    minDF -> 1.0,
    maxDF -> Long.MaxValue.toDouble,
    minTF -> 1.0,
    binary -> false
  )

  def setVocabSize(value: Int): this.type = set(vocabSize, value)
  def getVocabSize: Int = $(vocabSize)

  def setMinDF(value: Double): this.type = set(minDF, value)
  def getMinDF: Double = $(minDF)

  def setMaxDF(value: Double): this.type = set(maxDF, value)
  def getMaxDF: Double = $(maxDF)

  def setMinTF(value: Double): this.type = set(minTF, value)
  def getMinTF: Double = $(minTF)

  def setBinary(value: Boolean): this.type = set(binary, value)
  def getBinary: Boolean = $(binary)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

/**
 * Extracts a vocabulary from document collections and generates a CountVectorizerModel. Mirrors
 * `org.apache.spark.ml.feature.CountVectorizer`.
 */
class CountVectorizer(override val uid: String)
    extends Estimator[CountVectorizerModel]
    with CountVectorizerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("countVec"))

  override def fit(dataset: Dataset[_]): CountVectorizerModel =
    RemoteOps.fit(
      this,
      CountVectorizer.OperatorName,
      dataset,
      (id, ref) => new CountVectorizerModel(id, ref)
    )

  override def copy(extra: ParamMap): CountVectorizer = copyValues(new CountVectorizer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object CountVectorizer extends MLReadable[CountVectorizer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.CountVectorizer"

  override def load(path: String): CountVectorizer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new CountVectorizer(_)
    )
}

/**
 * The model fit by [[CountVectorizer]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.CountVectorizerModel`.
 */
class CountVectorizerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[CountVectorizerModel]
    with CountVectorizerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `vocabulary`, read from the server. */
  def vocabulary: Array[String] = attr[Array[String]]("vocabulary")

  override def copy(extra: ParamMap): CountVectorizerModel =
    copyValues(new CountVectorizerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object CountVectorizerModel extends MLReadable[CountVectorizerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.CountVectorizerModel"

  override def load(path: String): CountVectorizerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new CountVectorizerModel(id, ref))
}
