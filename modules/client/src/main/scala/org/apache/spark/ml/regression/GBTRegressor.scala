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
package org.apache.spark.ml.regression

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of GBTRegressor. */
trait GBTRegressorParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with HasCheckpointInterval
    with HasSeed
    with HasMaxIter
    with HasValidationIndicatorCol {

  final val maxDepth: IntParam = new IntParam(
    this,
    "maxDepth",
    "Maximum depth of the tree (>= 0). E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.",
    ParamValidators.gtEq(0)
  )

  final val maxBins: IntParam = new IntParam(
    this,
    "maxBins",
    "Max number of bins for discretizing continuous features. Must be >=2 and >= number of categories for any categorical feature.",
    ParamValidators.gtEq(2)
  )

  final val minInstancesPerNode: IntParam = new IntParam(
    this,
    "minInstancesPerNode",
    "Minimum number of instances each child must have after split.",
    ParamValidators.gtEq(1)
  )

  final val minWeightFractionPerNode: DoubleParam = new DoubleParam(
    this,
    "minWeightFractionPerNode",
    "Minimum fraction of the weighted sample count that each child must have after split.",
    ParamValidators.inRange(0.0, 0.5)
  )

  final val minInfoGain: DoubleParam = new DoubleParam(
    this,
    "minInfoGain",
    "Minimum information gain for a split to be considered at a tree node.",
    ParamValidators.gtEq(0.0)
  )

  final val maxMemoryInMB: IntParam = new IntParam(
    this,
    "maxMemoryInMB",
    "Maximum memory in MB allocated to histogram aggregation.",
    ParamValidators.gtEq(0)
  )

  final val cacheNodeIds: BooleanParam = new BooleanParam(
    this,
    "cacheNodeIds",
    "If false, the algorithm will pass trees to executors to match instances with nodes."
  )

  final val leafCol: Param[String] = new Param[String](
    this,
    "leafCol",
    "Leaf indices column name. Predicted leaf index of each instance in each tree by preorder."
  )

  final val stepSize: DoubleParam = new DoubleParam(
    this,
    "stepSize",
    "Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator.",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false)
  )

  final val subsamplingRate: DoubleParam = new DoubleParam(
    this,
    "subsamplingRate",
    "Fraction of the training data used for learning each decision tree, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false)
  )

  final val featureSubsetStrategy: Param[String] = new Param[String](
    this,
    "featureSubsetStrategy",
    "The number of features to consider for splits at each tree node.",
    ParamValidators.inArray(Array("auto", "all", "onethird", "sqrt", "log2"))
  )

  final val validationTol: DoubleParam = new DoubleParam(
    this,
    "validationTol",
    "Threshold for stopping early when fit with validation is used.",
    ParamValidators.gtEq(0.0)
  )

  final val lossType: Param[String] = new Param[String](
    this,
    "lossType",
    "The loss function to be optimized.",
    ParamValidators.inArray(Array("squared", "absolute"))
  )

  setDefault(
    maxDepth -> 5,
    maxBins -> 32,
    minInstancesPerNode -> 1,
    minWeightFractionPerNode -> 0.0,
    minInfoGain -> 0.0,
    maxMemoryInMB -> 256,
    cacheNodeIds -> false,
    leafCol -> "",
    stepSize -> 0.1,
    subsamplingRate -> 1.0,
    featureSubsetStrategy -> "all",
    validationTol -> 0.01,
    lossType -> "squared",
    maxIter -> 20,
    checkpointInterval -> 10
  )

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)
  def getMaxDepth: Int = $(maxDepth)

  def setMaxBins(value: Int): this.type = set(maxBins, value)
  def getMaxBins: Int = $(maxBins)

  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)
  def getMinInstancesPerNode: Int = $(minInstancesPerNode)

  def setMinWeightFractionPerNode(value: Double): this.type = set(minWeightFractionPerNode, value)
  def getMinWeightFractionPerNode: Double = $(minWeightFractionPerNode)

  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)
  def getMinInfoGain: Double = $(minInfoGain)

  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)
  def getMaxMemoryInMB: Int = $(maxMemoryInMB)

  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)
  def getCacheNodeIds: Boolean = $(cacheNodeIds)

  def setLeafCol(value: String): this.type = set(leafCol, value)
  def getLeafCol: String = $(leafCol)

  def setStepSize(value: Double): this.type = set(stepSize, value)
  def getStepSize: Double = $(stepSize)

  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)
  def getSubsamplingRate: Double = $(subsamplingRate)

  def setFeatureSubsetStrategy(value: String): this.type = set(featureSubsetStrategy, value)
  def getFeatureSubsetStrategy: String = $(featureSubsetStrategy)

  def setValidationTol(value: Double): this.type = set(validationTol, value)
  def getValidationTol: Double = $(validationTol)

  def setLossType(value: String): this.type = set(lossType, value)
  def getLossType: String = $(lossType)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def setValidationIndicatorCol(value: String): this.type = set(validationIndicatorCol, value)

}

/**
 * Gradient-Boosted Trees (GBTs) learning algorithm for regression. Mirrors
 * `org.apache.spark.ml.regression.GBTRegressor`.
 */
class GBTRegressor(override val uid: String)
    extends Estimator[GBTRegressionModel]
    with GBTRegressorParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("gbtr"))

  override def fit(dataset: Dataset[_]): GBTRegressionModel =
    RemoteOps.fit(
      this,
      GBTRegressor.OperatorName,
      dataset,
      (id, ref) => new GBTRegressionModel(id, ref)
    )

  override def copy(extra: ParamMap): GBTRegressor = copyValues(new GBTRegressor(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object GBTRegressor extends MLReadable[GBTRegressor] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.GBTRegressor"

  override def load(path: String): GBTRegressor =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new GBTRegressor(_)
    )
}

/**
 * The model fit by [[GBTRegressor]], held on the server. Mirrors
 * `org.apache.spark.ml.regression.GBTRegressionModel`.
 */
class GBTRegressionModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[GBTRegressionModel]
    with GBTRegressorParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `featureImportances`, read from the server. */
  def featureImportances: Vector = attr[Vector]("featureImportances")

  /** Model attribute `treeWeights`, read from the server. */
  def treeWeights: Array[Double] = attr[Array[Double]]("treeWeights")

  /** Model attribute `totalNumNodes`, read from the server. */
  def totalNumNodes: Int = attr[Int]("totalNumNodes")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** The trees of this ensemble, as models cached on the server. */

  def trees: Array[DecisionTreeRegressionModel] =
    RemoteOps
      .fetch(ref, Seq("trees"))
      .asInstanceOf[String]
      .split(",")
      .map(id =>
        new DecisionTreeRegressionModel(
          Identifiable.randomUID("dtr"),
          new ModelRef(id, ref.session, None)
        )
      )

  /** The debug string of the ensemble. */

  def toDebugString: String = attr[String]("toDebugString")

  /** The predicted leaf of each tree for `features`. */

  def predictLeaf(features: Vector): Vector = attrWith[Vector]("predictLeaf", features)

  /** The loss of the ensemble after each boosting iteration, on `dataset`. */

  def evaluateEachIteration(dataset: Dataset[_]): Array[Double] =
    attrWith[Array[Double]]("evaluateEachIteration", dataset, getLossType)

  override def copy(extra: ParamMap): GBTRegressionModel =
    copyValues(new GBTRegressionModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object GBTRegressionModel extends MLReadable[GBTRegressionModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.regression.GBTRegressionModel"

  override def load(path: String): GBTRegressionModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new GBTRegressionModel(id, ref))
}
