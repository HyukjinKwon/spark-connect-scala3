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

/** Params of KMeans. */
trait KMeansParams
    extends HasFeaturesCol
    with HasPredictionCol
    with HasMaxIter
    with HasTol
    with HasDistanceMeasure
    with HasSeed
    with HasMaxBlockSizeInMB
    with HasSolver {

  final val k: IntParam = new IntParam(
    this,
    "k",
    "The number of clusters to create (k). Must be > 1.",
    ParamValidators.gt(1)
  )

  final val initMode: Param[String] = new Param[String](
    this,
    "initMode",
    "The initialization algorithm. This can be either 'random' to choose random points as initial cluster centers, or 'k-means||' to use a parallel variant of k-means++",
    ParamValidators.inArray(Array("random", "k-means||"))
  )

  final val initSteps: IntParam = new IntParam(
    this,
    "initSteps",
    "The number of steps for the k-means|| initialization mode. Must be > 0.",
    ParamValidators.gt(0)
  )

  setDefault(
    k -> 2,
    initMode -> "k-means||",
    initSteps -> 2,
    maxIter -> 20,
    tol -> 1e-4,
    solver -> "auto"
  )

  def setK(value: Int): this.type = set(k, value)
  def getK: Int = $(k)

  def setInitMode(value: String): this.type = set(initMode, value)
  def getInitMode: String = $(initMode)

  def setInitSteps(value: Int): this.type = set(initSteps, value)
  def getInitSteps: Int = $(initSteps)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setTol(value: Double): this.type = set(tol, value)

  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  def setSolver(value: String): this.type = set(solver, value)

}

/**
 * K-means clustering with a k-means++ like initialization mode. Mirrors
 * `org.apache.spark.ml.clustering.KMeans`.
 */
class KMeans(override val uid: String)
    extends Estimator[KMeansModel]
    with KMeansParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("kmeans"))

  override def fit(dataset: Dataset[_]): KMeansModel =
    RemoteOps.fit(this, KMeans.OperatorName, dataset, (id, ref) => new KMeansModel(id, ref))

  override def copy(extra: ParamMap): KMeans = copyValues(new KMeans(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object KMeans extends MLReadable[KMeans] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.KMeans"

  override def load(path: String): KMeans =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new KMeans(_)
    )
}

/**
 * The model fit by [[KMeans]], held on the server. Mirrors
 * `org.apache.spark.ml.clustering.KMeansModel`.
 */
class KMeansModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[KMeansModel]
    with KMeansParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** Model attribute `clusterCenterMatrix`, read from the server. */
  def clusterCenterMatrix: Matrix = attr[Matrix]("clusterCenterMatrix")

  /** Predicts the cluster index of a feature vector. */

  def predict(features: Vector): Int = attrWith[Int]("predict", features)

  /** The cluster centers, one vector per cluster. */

  def clusterCenters: Array[Vector] = {

    val m = clusterCenterMatrix

    Array.tabulate(m.numRows)(i =>
      org.apache.spark.ml.linalg.Vectors.dense(Array.tabulate(m.numCols)(j => m(i, j)))
    )

  }

  def hasSummary: Boolean = attr[Boolean]("hasSummary")

  /** The training summary of this model. */

  def summary: KMeansSummary = new KMeansSummary(ref, Seq("summary"))

  override def copy(extra: ParamMap): KMeansModel =
    copyValues(new KMeansModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object KMeansModel extends MLReadable[KMeansModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.KMeansModel"

  override def load(path: String): KMeansModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new KMeansModel(id, ref))
}

/** Training summary of a [[KMeansModel]]. */
class KMeansSummary private[ml] (ref: ModelRef, path: Seq[String]) extends Summary(ref, path) {

  def trainingCost: Double = attr[Double]("trainingCost")

  def k: Int = attr[Int]("k")

  def numIter: Int = attr[Int]("numIter")

  def clusterSizes: Array[Long] = attr[Array[Long]]("clusterSizes")

  def predictionCol: String = attr[String]("predictionCol")

  def featuresCol: String = attr[String]("featuresCol")

  def predictions: DataFrame = frame("predictions")

  def cluster: DataFrame = frame("cluster")
}
