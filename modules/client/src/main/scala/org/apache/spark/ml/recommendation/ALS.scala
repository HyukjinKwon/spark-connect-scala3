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
package org.apache.spark.ml.recommendation

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of ALS. */
trait ALSParams
    extends HasMaxIter
    with HasRegParam
    with HasCheckpointInterval
    with HasSeed
    with HasPredictionCol {

  final val rank: IntParam =
    new IntParam(this, "rank", "rank of the factorization", ParamValidators.gt(0))

  final val numUserBlocks: IntParam =
    new IntParam(this, "numUserBlocks", "number of user blocks", ParamValidators.gt(0))

  final val numItemBlocks: IntParam =
    new IntParam(this, "numItemBlocks", "number of item blocks", ParamValidators.gt(0))

  final val implicitPrefs: BooleanParam =
    new BooleanParam(this, "implicitPrefs", "whether to use implicit preference")

  final val alpha: DoubleParam = new DoubleParam(this, "alpha", "alpha for implicit preference")

  final val userCol: Param[String] = new Param[String](
    this,
    "userCol",
    "column name for user ids. Ids must be within the integer value range."
  )

  final val itemCol: Param[String] = new Param[String](
    this,
    "itemCol",
    "column name for item ids. Ids must be within the integer value range."
  )

  final val ratingCol: Param[String] =
    new Param[String](this, "ratingCol", "column name for ratings")

  final val nonnegative: BooleanParam =
    new BooleanParam(this, "nonnegative", "whether to use nonnegative constraint for least squares")

  final val blockSize: IntParam = new IntParam(
    this,
    "blockSize",
    "block size for stacking input data in matrices.",
    ParamValidators.gt(0)
  )

  final val intermediateStorageLevel: Param[String] =
    new Param[String](this, "intermediateStorageLevel", "StorageLevel for intermediate datasets.")

  final val finalStorageLevel: Param[String] =
    new Param[String](this, "finalStorageLevel", "StorageLevel for ALS model factors.")

  final val coldStartStrategy: Param[String] = new Param[String](
    this,
    "coldStartStrategy",
    "strategy for dealing with unknown or new users/items at prediction time. Supported values: nan, drop.",
    ParamValidators.inArray(Array("nan", "drop"))
  )

  setDefault(
    rank -> 10,
    numUserBlocks -> 10,
    numItemBlocks -> 10,
    implicitPrefs -> false,
    alpha -> 1.0,
    userCol -> "user",
    itemCol -> "item",
    ratingCol -> "rating",
    nonnegative -> false,
    blockSize -> 4096,
    intermediateStorageLevel -> "MEMORY_AND_DISK",
    finalStorageLevel -> "MEMORY_AND_DISK",
    coldStartStrategy -> "nan",
    maxIter -> 10,
    regParam -> 0.1,
    checkpointInterval -> 10
  )

  def setRank(value: Int): this.type = set(rank, value)
  def getRank: Int = $(rank)

  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)
  def getNumUserBlocks: Int = $(numUserBlocks)

  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)
  def getNumItemBlocks: Int = $(numItemBlocks)

  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)
  def getImplicitPrefs: Boolean = $(implicitPrefs)

  def setAlpha(value: Double): this.type = set(alpha, value)
  def getAlpha: Double = $(alpha)

  def setUserCol(value: String): this.type = set(userCol, value)
  def getUserCol: String = $(userCol)

  def setItemCol(value: String): this.type = set(itemCol, value)
  def getItemCol: String = $(itemCol)

  def setRatingCol(value: String): this.type = set(ratingCol, value)
  def getRatingCol: String = $(ratingCol)

  def setNonnegative(value: Boolean): this.type = set(nonnegative, value)
  def getNonnegative: Boolean = $(nonnegative)

  def setBlockSize(value: Int): this.type = set(blockSize, value)
  def getBlockSize: Int = $(blockSize)

  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)

  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)
  def getFinalStorageLevel: String = $(finalStorageLevel)

  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)
  def getColdStartStrategy: String = $(coldStartStrategy)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setRegParam(value: Double): this.type = set(regParam, value)

  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

}

/** Params of ALSModel. */
trait ALSModelParams extends HasPredictionCol {

  final val userCol: Param[String] = new Param[String](
    this,
    "userCol",
    "column name for user ids. Ids must be within the integer value range."
  )

  final val itemCol: Param[String] = new Param[String](
    this,
    "itemCol",
    "column name for item ids. Ids must be within the integer value range."
  )

  final val coldStartStrategy: Param[String] = new Param[String](
    this,
    "coldStartStrategy",
    "strategy for dealing with unknown or new users/items at prediction time. Supported values: nan, drop.",
    ParamValidators.inArray(Array("nan", "drop"))
  )

  setDefault(userCol -> "user", itemCol -> "item", coldStartStrategy -> "nan")

  def setUserCol(value: String): this.type = set(userCol, value)
  def getUserCol: String = $(userCol)

  def setItemCol(value: String): this.type = set(itemCol, value)
  def getItemCol: String = $(itemCol)

  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)
  def getColdStartStrategy: String = $(coldStartStrategy)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

}

/**
 * Alternating Least Squares (ALS) matrix factorization. Mirrors
 * `org.apache.spark.ml.recommendation.ALS`.
 */
class ALS(override val uid: String) extends Estimator[ALSModel] with ALSParams with MLWritable {

  def this() = this(Identifiable.randomUID("als"))

  override def fit(dataset: Dataset[_]): ALSModel =
    RemoteOps.fit(this, ALS.OperatorName, dataset, (id, ref) => new ALSModel(id, ref))

  override def copy(extra: ParamMap): ALS = copyValues(new ALS(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object ALS extends MLReadable[ALS] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.recommendation.ALS"

  override def load(path: String): ALS =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new ALS(_)
    )
}

/**
 * The model fit by [[ALS]], held on the server. Mirrors
 * `org.apache.spark.ml.recommendation.ALSModel`.
 */
class ALSModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[ALSModel]
    with ALSModelParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `userFactors`, a DataFrame on the server. */
  def userFactors: DataFrame = frame("userFactors")

  /** Model attribute `itemFactors`, a DataFrame on the server. */
  def itemFactors: DataFrame = frame("itemFactors")

  /** Top `numItems` recommendations for every user. */

  def recommendForAllUsers(numItems: Int): DataFrame = frameWith("recommendForAllUsers", numItems)

  /** Top `numUsers` recommendations for every item. */

  def recommendForAllItems(numUsers: Int): DataFrame = frameWith("recommendForAllItems", numUsers)

  /** Top `numItems` recommendations for the users in `dataset`. */

  def recommendForUserSubset(dataset: Dataset[_], numItems: Int): DataFrame =
    frameWith("recommendForUserSubset", dataset, numItems)

  /** Top `numUsers` recommendations for the items in `dataset`. */

  def recommendForItemSubset(dataset: Dataset[_], numUsers: Int): DataFrame =
    frameWith("recommendForItemSubset", dataset, numUsers)

  override def copy(extra: ParamMap): ALSModel =
    copyValues(new ALSModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object ALSModel extends MLReadable[ALSModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.recommendation.ALSModel"

  override def load(path: String): ALSModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new ALSModel(id, ref))
}
