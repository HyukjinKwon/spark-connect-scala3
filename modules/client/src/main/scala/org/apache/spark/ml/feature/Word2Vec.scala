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

/** Params of Word2Vec. */
trait Word2VecParams
    extends HasInputCol
    with HasOutputCol
    with HasMaxIter
    with HasStepSize
    with HasSeed {

  final val vectorSize: IntParam = new IntParam(
    this,
    "vectorSize",
    "the dimension of codes after transforming from words (> 0)",
    ParamValidators.gt(0)
  )

  final val windowSize: IntParam = new IntParam(
    this,
    "windowSize",
    "the window size (context words from [-window, window]) (> 0)",
    ParamValidators.gt(0)
  )

  final val numPartitions: IntParam = new IntParam(
    this,
    "numPartitions",
    "number of partitions for sentences of words (> 0)",
    ParamValidators.gt(0)
  )

  final val minCount: IntParam = new IntParam(
    this,
    "minCount",
    "the minimum number of times a token must appear to be included in the word2vec model's vocabulary (>= 0)",
    ParamValidators.gtEq(0)
  )

  final val maxSentenceLength: IntParam = new IntParam(
    this,
    "maxSentenceLength",
    "Maximum length (in words) of each sentence in the input data (> 0)",
    ParamValidators.gt(0)
  )

  setDefault(
    vectorSize -> 100,
    windowSize -> 5,
    numPartitions -> 1,
    minCount -> 5,
    maxSentenceLength -> 1000,
    maxIter -> 1,
    stepSize -> 0.025
  )

  def setVectorSize(value: Int): this.type = set(vectorSize, value)
  def getVectorSize: Int = $(vectorSize)

  def setWindowSize(value: Int): this.type = set(windowSize, value)
  def getWindowSize: Int = $(windowSize)

  def setNumPartitions(value: Int): this.type = set(numPartitions, value)
  def getNumPartitions: Int = $(numPartitions)

  def setMinCount(value: Int): this.type = set(minCount, value)
  def getMinCount: Int = $(minCount)

  def setMaxSentenceLength(value: Int): this.type = set(maxSentenceLength, value)
  def getMaxSentenceLength: Int = $(maxSentenceLength)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setStepSize(value: Double): this.type = set(stepSize, value)

  def setSeed(value: Long): this.type = set(seed, value)

}

/**
 * Word2Vec creates a vector representation of words in a text corpus. Mirrors
 * `org.apache.spark.ml.feature.Word2Vec`.
 */
class Word2Vec(override val uid: String)
    extends Estimator[Word2VecModel]
    with Word2VecParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("w2v"))

  override def fit(dataset: Dataset[_]): Word2VecModel =
    RemoteOps.fit(this, Word2Vec.OperatorName, dataset, (id, ref) => new Word2VecModel(id, ref))

  override def copy(extra: ParamMap): Word2Vec = copyValues(new Word2Vec(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object Word2Vec extends MLReadable[Word2Vec] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Word2Vec"

  override def load(path: String): Word2Vec =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new Word2Vec(_)
    )
}

/**
 * The model fit by [[Word2Vec]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.Word2VecModel`.
 */
class Word2VecModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[Word2VecModel]
    with Word2VecParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `getVectors`, a DataFrame on the server. */
  def getVectors: DataFrame = frame("getVectors")

  /** Words most similar to `word`, with their cosine similarity. */

  def findSynonyms(word: String, num: Int): DataFrame = frameWith("findSynonyms", word, num)

  /** Words most similar to the vector `vector`, with their cosine similarity. */

  def findSynonyms(vector: Vector, num: Int): DataFrame = frameWith("findSynonyms", vector, num)

  override def copy(extra: ParamMap): Word2VecModel =
    copyValues(new Word2VecModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object Word2VecModel extends MLReadable[Word2VecModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Word2VecModel"

  override def load(path: String): Word2VecModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new Word2VecModel(id, ref))
}
