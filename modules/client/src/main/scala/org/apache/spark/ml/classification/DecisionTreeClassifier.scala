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
package org.apache.spark.ml.classification

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of DecisionTreeClassifier. */
trait DecisionTreeClassifierParams
    extends HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol
    with HasCheckpointInterval
    with HasSeed
    with HasProbabilityCol
    with HasRawPredictionCol
    with HasThresholds {

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

  final val impurity: Param[String] = new Param[String](
    this,
    "impurity",
    "Criterion used for information gain calculation.",
    ParamValidators.inArray(Array("entropy", "gini"))
  )

  final val leafCol: Param[String] = new Param[String](
    this,
    "leafCol",
    "Leaf indices column name. Predicted leaf index of each instance in each tree by preorder."
  )

  setDefault(
    maxDepth -> 5,
    maxBins -> 32,
    minInstancesPerNode -> 1,
    minWeightFractionPerNode -> 0.0,
    minInfoGain -> 0.0,
    maxMemoryInMB -> 256,
    cacheNodeIds -> false,
    impurity -> "gini",
    leafCol -> "",
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

  def setImpurity(value: String): this.type = set(impurity, value)
  def getImpurity: String = $(impurity)

  def setLeafCol(value: String): this.type = set(leafCol, value)
  def getLeafCol: String = $(leafCol)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

}

/**
 * Decision tree learning algorithm for classification. Mirrors
 * `org.apache.spark.ml.classification.DecisionTreeClassifier`.
 */
class DecisionTreeClassifier(override val uid: String)
    extends Estimator[DecisionTreeClassificationModel]
    with DecisionTreeClassifierParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("dtc"))

  override def fit(dataset: Dataset[_]): DecisionTreeClassificationModel =
    RemoteOps.fit(
      this,
      DecisionTreeClassifier.OperatorName,
      dataset,
      (id, ref) => new DecisionTreeClassificationModel(id, ref)
    )

  override def copy(extra: ParamMap): DecisionTreeClassifier =
    copyValues(new DecisionTreeClassifier(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object DecisionTreeClassifier extends MLReadable[DecisionTreeClassifier] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.DecisionTreeClassifier"

  override def load(path: String): DecisionTreeClassifier =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new DecisionTreeClassifier(_)
    )
}

/**
 * The model fit by [[DecisionTreeClassifier]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.DecisionTreeClassificationModel`.
 */
class DecisionTreeClassificationModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[DecisionTreeClassificationModel]
    with DecisionTreeClassifierParams
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

  /** Model attribute `numNodes`, read from the server. */
  def numNodes: Int = attr[Int]("numNodes")

  /** Model attribute `depth`, read from the server. */
  def depth: Int = attr[Int]("depth")

  /** Model attribute `numClasses`, read from the server. */
  def numClasses: Int = attr[Int]("numClasses")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** The debug string of the tree. */

  def toDebugString: String = attr[String]("toDebugString")

  /** The leaf reached by `features`. */

  def predictLeaf(features: Vector): Double = attrWith[Double]("predictLeaf", features)

  override def copy(extra: ParamMap): DecisionTreeClassificationModel =
    copyValues(new DecisionTreeClassificationModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object DecisionTreeClassificationModel extends MLReadable[DecisionTreeClassificationModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.classification.DecisionTreeClassificationModel"

  override def load(path: String): DecisionTreeClassificationModel =
    RemoteIO.loadModel(
      OperatorName,
      path,
      (id, ref) => new DecisionTreeClassificationModel(id, ref)
    )
}
