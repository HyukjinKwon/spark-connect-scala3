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

/** Params of BucketedRandomProjectionLSH. */
trait BucketedRandomProjectionLSHParams extends HasInputCol with HasOutputCol with HasSeed {

  final val bucketLength: DoubleParam = new DoubleParam(
    this,
    "bucketLength",
    "the length of each hash bucket, a larger bucket lowers the false negative rate",
    ParamValidators.gt(0.0)
  )

  final val numHashTables: IntParam = new IntParam(
    this,
    "numHashTables",
    "number of hash tables, where increasing number of hash tables lowers the false negative rate, and decreasing it improves the running performance",
    ParamValidators.gt(0)
  )

  setDefault(numHashTables -> 1)

  def setBucketLength(value: Double): this.type = set(bucketLength, value)
  def getBucketLength: Double = $(bucketLength)

  def setNumHashTables(value: Int): this.type = set(numHashTables, value)
  def getNumHashTables: Int = $(numHashTables)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

}

/**
 * LSH class for Euclidean distance metrics. Mirrors
 * `org.apache.spark.ml.feature.BucketedRandomProjectionLSH`.
 */
class BucketedRandomProjectionLSH(override val uid: String)
    extends Estimator[BucketedRandomProjectionLSHModel]
    with BucketedRandomProjectionLSHParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("brpLSH"))

  override def fit(dataset: Dataset[_]): BucketedRandomProjectionLSHModel =
    RemoteOps.fit(
      this,
      BucketedRandomProjectionLSH.OperatorName,
      dataset,
      (id, ref) => new BucketedRandomProjectionLSHModel(id, ref)
    )

  override def copy(extra: ParamMap): BucketedRandomProjectionLSH =
    copyValues(new BucketedRandomProjectionLSH(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object BucketedRandomProjectionLSH extends MLReadable[BucketedRandomProjectionLSH] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.BucketedRandomProjectionLSH"

  override def load(path: String): BucketedRandomProjectionLSH =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new BucketedRandomProjectionLSH(_)
    )
}

/**
 * The model fit by [[BucketedRandomProjectionLSH]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel`.
 */
class BucketedRandomProjectionLSHModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[BucketedRandomProjectionLSHModel]
    with BucketedRandomProjectionLSHParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** The `numNearestNeighbors` rows of `dataset` nearest to `key`, with their distances. */

  def approxNearestNeighbors(
      dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int,
      distCol: String
  ): DataFrame =
    frameWith("approxNearestNeighbors", dataset, key, numNearestNeighbors, distCol)

  /** Pairs of rows from `datasetA` and `datasetB` whose distance is at most `threshold`. */

  def approxSimilarityJoin(
      datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double,
      distCol: String
  ): DataFrame =
    frameWith("approxSimilarityJoin", datasetA, datasetB, threshold, distCol)

  override def copy(extra: ParamMap): BucketedRandomProjectionLSHModel =
    copyValues(new BucketedRandomProjectionLSHModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object BucketedRandomProjectionLSHModel extends MLReadable[BucketedRandomProjectionLSHModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel"

  override def load(path: String): BucketedRandomProjectionLSHModel =
    RemoteIO.loadModel(
      OperatorName,
      path,
      (id, ref) => new BucketedRandomProjectionLSHModel(id, ref)
    )
}
