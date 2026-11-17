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
package org.apache.spark.ml.clustering

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of [[LDA]]. */
trait LDAParams
    extends Params
    with HasMaxIter
    with HasSeed
    with HasCheckpointInterval
    with HasFeaturesCol {

  /** The number of topics. */
  final val k: IntParam =
    new IntParam(this, "k", "The number of topics (clusters) to infer.", ParamValidators.gt(0))

  /** Concentration parameter (commonly named alpha) for the prior on documents' topic mixtures. */
  final val docConcentration: DoubleArrayParam = new DoubleArrayParam(
    this,
    "docConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents' " +
      "distributions over topics (\"theta\")."
  )

  /** Concentration parameter (commonly named beta or eta) for the prior on topics. */
  final val topicConcentration: DoubleParam = new DoubleParam(
    this,
    "topicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic' " +
      "distributions over terms.",
    ParamValidators.gt(0.0)
  )

  /** Optimizer: `online` or `em`. */
  final val optimizer: Param[String] = new Param[String](
    this,
    "optimizer",
    "Optimizer or inference algorithm used to estimate the LDA model. Supported: online " +
      "(default), em",
    ParamValidators.inArray(Array("online", "em"))
  )

  /** The column of topic distributions added by transform. */
  final val topicDistributionCol: Param[String] = new Param[String](
    this,
    "topicDistributionCol",
    "Output column with estimates of the topic mixture distribution for each document."
  )

  /** Learning rate offset for the online optimizer. */
  final val learningOffset: DoubleParam = new DoubleParam(
    this,
    "learningOffset",
    "(For online optimizer only) A (positive) learning parameter that downweights early " +
      "iterations.",
    ParamValidators.gt(0.0)
  )

  /** Learning rate decay for the online optimizer. */
  final val learningDecay: DoubleParam = new DoubleParam(
    this,
    "learningDecay",
    "(For online optimizer only) Learning rate, set as an exponential decay rate.",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false)
  )

  /** Fraction of documents sampled per online iteration. */
  final val subsamplingRate: DoubleParam = new DoubleParam(
    this,
    "subsamplingRate",
    "(For online optimizer only) Fraction of the corpus to be sampled and used in each iteration " +
      "of mini-batch gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false)
  )

  /** Whether to optimize the document concentration during training. */
  final val optimizeDocConcentration: BooleanParam = new BooleanParam(
    this,
    "optimizeDocConcentration",
    "(For online optimizer only) Indicates whether the docConcentration (Dirichlet parameter for " +
      "document topic distribution) will be optimized during training."
  )

  /** Whether to keep the last checkpoint. */
  final val keepLastCheckpoint: BooleanParam = new BooleanParam(
    this,
    "keepLastCheckpoint",
    "(For EM optimizer only) If using checkpointing, this indicates whether to keep the last " +
      "checkpoint. If false, then the checkpoint will be deleted."
  )

  setDefault(
    maxIter -> 20,
    k -> 10,
    optimizer -> "online",
    checkpointInterval -> 10,
    learningOffset -> 1024,
    learningDecay -> 0.51,
    subsamplingRate -> 0.05,
    optimizeDocConcentration -> true,
    keepLastCheckpoint -> true,
    topicDistributionCol -> "topicDistribution"
  )

  def getK: Int = $(k)

  def getOptimizer: String = $(optimizer)

  def getTopicDistributionCol: String = $(topicDistributionCol)
}

/**
 * Latent Dirichlet Allocation, fit on the server. The `online` optimizer fits a [[LocalLDAModel]];
 * `em` fits a [[DistributedLDAModel]]. Mirrors `org.apache.spark.ml.clustering.LDA`.
 */
class LDA(override val uid: String) extends Estimator[LDAModel] with LDAParams with MLWritable {

  def this() = this(Identifiable.randomUID("lda"))

  def setK(value: Int): this.type = set(k, value)

  def setOptimizer(value: String): this.type = set(optimizer, value)

  def setDocConcentration(value: Array[Double]): this.type = set(docConcentration, value)

  def setTopicConcentration(value: Double): this.type = set(topicConcentration, value)

  def setTopicDistributionCol(value: String): this.type = set(topicDistributionCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setLearningOffset(value: Double): this.type = set(learningOffset, value)

  def setLearningDecay(value: Double): this.type = set(learningDecay, value)

  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  def setOptimizeDocConcentration(value: Boolean): this.type = set(optimizeDocConcentration, value)

  def setKeepLastCheckpoint(value: Boolean): this.type = set(keepLastCheckpoint, value)

  override def fit(dataset: Dataset[_]): LDAModel =
    RemoteOps.fit(
      this,
      LDA.OperatorName,
      dataset,
      (id, ref) =>
        if ($(optimizer) == "em") new DistributedLDAModel(id, ref) else new LocalLDAModel(id, ref)
    )

  override def copy(extra: ParamMap): LDA = copyValues(new LDA(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object LDA extends MLReadable[LDA] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.LDA"

  override def load(path: String): LDA =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new LDA(_)
    )
}

/**
 * A fitted LDA model held on the server. Mirrors `org.apache.spark.ml.clustering.LDAModel`.
 */
abstract class LDAModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[LDAModel]
    with LDAParams
    with RemoteModelLike
    with MLWritable {

  /** The prior on document topic mixtures. */
  def estimatedDocConcentration: Vector = attr[Vector]("estimatedDocConcentration")

  /** The topics, as a vocabulary-by-topics matrix. */
  def topicsMatrix: Matrix = attr[Matrix]("topicsMatrix")

  /** Whether the model is a distributed (EM) model. */
  def isDistributed: Boolean = attr[Boolean]("isDistributed")

  /** The number of terms in the vocabulary. */
  def vocabSize: Int = attr[Int]("vocabSize")

  /** The log likelihood of `dataset` under the model. */
  def logLikelihood(dataset: Dataset[_]): Double = attrWith[Double]("logLikelihood", dataset)

  /** The log perplexity of `dataset` under the model. */
  def logPerplexity(dataset: Dataset[_]): Double = attrWith[Double]("logPerplexity", dataset)

  /** The top terms of each topic, as a DataFrame. */
  def describeTopics(maxTermsPerTopic: Int): DataFrame =
    frameWith("describeTopics", maxTermsPerTopic)

  protected def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  protected def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  protected def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

/** A local LDA model, fit with the online optimizer. */
class LocalLDAModel private[ml] (override val uid: String, override private[ml] val ref: ModelRef)
    extends LDAModel(uid, ref) {

  override def copy(extra: ParamMap): LDAModel =
    copyValues(new LocalLDAModel(uid, ref.retain()), extra)
}

object LocalLDAModel extends MLReadable[LocalLDAModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.LocalLDAModel"

  override def load(path: String): LocalLDAModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new LocalLDAModel(id, ref))
}

/** A distributed LDA model, fit with the EM optimizer. */
class DistributedLDAModel private[ml] (
    override val uid: String,
    override private[ml] val ref: ModelRef
) extends LDAModel(uid, ref) {

  /** The log likelihood of the training data. */
  def trainingLogLikelihood: Double = attr[Double]("trainingLogLikelihood")

  /** The log prior of the topic distributions. */
  def logPrior: Double = attr[Double]("logPrior")

  /** The model as a local model, on the server. */
  def toLocal: LocalLDAModel = {
    val id = RemoteOps.fetch(ref, Seq("toLocal")).asInstanceOf[String]
    new LocalLDAModel(Identifiable.randomUID("lda"), new ModelRef(id, ref.session, None))
  }

  override def copy(extra: ParamMap): LDAModel =
    copyValues(new DistributedLDAModel(uid, ref.retain()), extra)
}

object DistributedLDAModel extends MLReadable[DistributedLDAModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.DistributedLDAModel"

  override def load(path: String): DistributedLDAModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new DistributedLDAModel(id, ref))
}
