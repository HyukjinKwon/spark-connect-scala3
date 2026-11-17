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
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of GaussianMixture. */
trait GaussianMixtureParams
    extends HasFeaturesCol
    with HasPredictionCol
    with HasProbabilityCol
    with HasMaxIter
    with HasTol
    with HasSeed
    with HasAggregationDepth
    with HasMaxBlockSizeInMB {

  final val k: IntParam = new IntParam(
    this,
    "k",
    "Number of independent Gaussians in the mixture model. Must be > 0.",
    ParamValidators.gt(0)
  )

  setDefault(k -> 2, maxIter -> 100, tol -> 0.01)

  def setK(value: Int): this.type = set(k, value)
  def getK: Int = $(k)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

}

/** Gaussian Mixture clustering. Mirrors `org.apache.spark.ml.clustering.GaussianMixture`. */
class GaussianMixture(override val uid: String)
    extends Estimator[GaussianMixtureModel]
    with GaussianMixtureParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("gmm"))

  override def fit(dataset: Dataset[_]): GaussianMixtureModel =
    RemoteOps.fit(
      this,
      GaussianMixture.OperatorName,
      dataset,
      (id, ref) => new GaussianMixtureModel(id, ref)
    )

  override def copy(extra: ParamMap): GaussianMixture = copyValues(new GaussianMixture(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object GaussianMixture extends MLReadable[GaussianMixture] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.GaussianMixture"

  override def load(path: String): GaussianMixture =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new GaussianMixture(_)
    )
}

/**
 * The model fit by [[GaussianMixture]], held on the server. Mirrors
 * `org.apache.spark.ml.clustering.GaussianMixtureModel`.
 */
class GaussianMixtureModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[GaussianMixtureModel]
    with GaussianMixtureParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `weights`, read from the server. */
  def weights: Array[Double] = attr[Array[Double]]("weights")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Model attribute `gaussiansDF`, a DataFrame on the server. */
  def gaussiansDF: DataFrame = frame("gaussiansDF")

  /** Predicts the most likely component of a feature vector. */

  def predict(features: Vector): Int = attrWith[Int]("predict", features)

  /** The posterior probability of each component for a feature vector. */

  def predictProbability(features: Vector): Vector =
    attrWith[Vector]("predictProbability", features)

  override def copy(extra: ParamMap): GaussianMixtureModel =
    copyValues(new GaussianMixtureModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object GaussianMixtureModel extends MLReadable[GaussianMixtureModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.GaussianMixtureModel"

  override def load(path: String): GaussianMixtureModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new GaussianMixtureModel(id, ref))
}
