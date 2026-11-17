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

/** Params of BisectingKMeans. */
trait BisectingKMeansParams
    extends HasFeaturesCol
    with HasPredictionCol
    with HasMaxIter
    with HasDistanceMeasure
    with HasSeed {

  final val k: IntParam = new IntParam(
    this,
    "k",
    "The desired number of leaf clusters. Must be > 1.",
    ParamValidators.gt(1)
  )

  final val minDivisibleClusterSize: DoubleParam = new DoubleParam(
    this,
    "minDivisibleClusterSize",
    "The minimum number of points (if >= 1.0) or the minimum proportion of points (if < 1.0) of a divisible cluster.",
    ParamValidators.gt(0.0)
  )

  setDefault(k -> 4, minDivisibleClusterSize -> 1.0, maxIter -> 20)

  def setK(value: Int): this.type = set(k, value)
  def getK: Int = $(k)

  def setMinDivisibleClusterSize(value: Double): this.type = set(minDivisibleClusterSize, value)
  def getMinDivisibleClusterSize: Double = $(minDivisibleClusterSize)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  def setSeed(value: Long): this.type = set(seed, value)

}

/**
 * A bisecting k-means clustering algorithm. Mirrors
 * `org.apache.spark.ml.clustering.BisectingKMeans`.
 */
class BisectingKMeans(override val uid: String)
    extends Estimator[BisectingKMeansModel]
    with BisectingKMeansParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("bisectingKMeans"))

  override def fit(dataset: Dataset[_]): BisectingKMeansModel =
    RemoteOps.fit(
      this,
      BisectingKMeans.OperatorName,
      dataset,
      (id, ref) => new BisectingKMeansModel(id, ref)
    )

  override def copy(extra: ParamMap): BisectingKMeans = copyValues(new BisectingKMeans(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object BisectingKMeans extends MLReadable[BisectingKMeans] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.BisectingKMeans"

  override def load(path: String): BisectingKMeans =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new BisectingKMeans(_)
    )
}

/**
 * The model fit by [[BisectingKMeans]], held on the server. Mirrors
 * `org.apache.spark.ml.clustering.BisectingKMeansModel`.
 */
class BisectingKMeansModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[BisectingKMeansModel]
    with BisectingKMeansParams
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

  /** The sum of squared distances from each point to its cluster center, over `dataset`. */

  def computeCost(dataset: Dataset[_]): Double = attrWith[Double]("computeCost", dataset)

  /** The cluster centers, one vector per cluster. */

  def clusterCenters: Array[Vector] = {

    val m = clusterCenterMatrix

    Array.tabulate(m.numRows)(i =>
      org.apache.spark.ml.linalg.Vectors.dense(Array.tabulate(m.numCols)(j => m(i, j)))
    )

  }

  override def copy(extra: ParamMap): BisectingKMeansModel =
    copyValues(new BisectingKMeansModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object BisectingKMeansModel extends MLReadable[BisectingKMeansModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.clustering.BisectingKMeansModel"

  override def load(path: String): BisectingKMeansModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new BisectingKMeansModel(id, ref))
}
