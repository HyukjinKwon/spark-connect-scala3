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

/** Params of RandomForestClassifier. */
trait RandomForestClassifierParams
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

  final val numTrees: IntParam =
    new IntParam(this, "numTrees", "Number of trees to train (>= 1).", ParamValidators.gt(0))

  final val subsamplingRate: DoubleParam = new DoubleParam(
    this,
    "subsamplingRate",
    "Fraction of the training data used for learning each decision tree, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0)
  )

  final val featureSubsetStrategy: Param[String] = new Param[String](
    this,
    "featureSubsetStrategy",
    "The number of features to consider for splits at each tree node.",
    ParamValidators.inArray(Array("auto", "all", "onethird", "sqrt", "log2"))
  )

  final val bootstrap: BooleanParam =
    new BooleanParam(this, "bootstrap", "Whether bootstrap samples are used when building trees.")

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
    numTrees -> 20,
    subsamplingRate -> 1.0,
    featureSubsetStrategy -> "auto",
    bootstrap -> true,
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

  def setNumTrees(value: Int): this.type = set(numTrees, value)
  def getNumTrees: Int = $(numTrees)

  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)
  def getSubsamplingRate: Double = $(subsamplingRate)

  def setFeatureSubsetStrategy(value: String): this.type = set(featureSubsetStrategy, value)
  def getFeatureSubsetStrategy: String = $(featureSubsetStrategy)

  def setBootstrap(value: Boolean): this.type = set(bootstrap, value)
  def getBootstrap: Boolean = $(bootstrap)

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
 * Random Forest learning algorithm for classification. Mirrors
 * `org.apache.spark.ml.classification.RandomForestClassifier`.
 */
class RandomForestClassifier(override val uid: String)
    extends Estimator[RandomForestClassificationModel]
    with RandomForestClassifierParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("rfc"))

  override def fit(dataset: Dataset[_]): RandomForestClassificationModel =
    RemoteOps.fit(
      this,
      RandomForestClassifier.OperatorName,
      dataset,
      (id, ref) => new RandomForestClassificationModel(id, ref)
    )

  override def copy(extra: ParamMap): RandomForestClassifier =
    copyValues(new RandomForestClassifier(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object RandomForestClassifier extends MLReadable[RandomForestClassifier] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.classification.RandomForestClassifier"

  override def load(path: String): RandomForestClassifier =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new RandomForestClassifier(_)
    )
}

/**
 * The model fit by [[RandomForestClassifier]], held on the server. Mirrors
 * `org.apache.spark.ml.classification.RandomForestClassificationModel`.
 */
class RandomForestClassificationModel private[ml] (
    override val uid: String,
    private[ml] val ref: ModelRef
) extends Model[RandomForestClassificationModel]
    with RandomForestClassifierParams
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

  /** Model attribute `numClasses`, read from the server. */
  def numClasses: Int = attr[Int]("numClasses")

  /** Model attribute `numFeatures`, read from the server. */
  def numFeatures: Int = attr[Int]("numFeatures")

  /** The trees of this ensemble, as models cached on the server. */

  def trees: Array[DecisionTreeClassificationModel] =
    RemoteOps
      .fetch(ref, Seq("trees"))
      .asInstanceOf[String]
      .split(",")
      .map(id =>
        new DecisionTreeClassificationModel(
          Identifiable.randomUID("dtc"),
          new ModelRef(id, ref.session, None)
        )
      )

  /** The debug string of the ensemble. */

  def toDebugString: String = attr[String]("toDebugString")

  /** The predicted leaf of each tree for `features`. */

  def predictLeaf(features: Vector): Vector = attrWith[Vector]("predictLeaf", features)

  /** Class probabilities of an ensemble prediction for `features`. */

  def predictProbability(features: Vector): Vector =
    attrWith[Vector]("predictProbability", features)

  /** Raw ensemble votes for `features`. */

  def predictRaw(features: Vector): Vector = attrWith[Vector]("predictRaw", features)

  /** Evaluates the model on `dataset`, returning a summary of the predictions. */

  def evaluate(dataset: Dataset[_]): ClassificationSummary = {

    val summaryId = RemoteOps.fetch(ref, Seq("evaluate"), Seq(dataset)).asInstanceOf[String]

    new ClassificationSummary(new ModelRef(summaryId, ref.session, None), Nil)

  }

  override def copy(extra: ParamMap): RandomForestClassificationModel =
    copyValues(new RandomForestClassificationModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  /** The estimated in-memory size of this model on the server, in bytes (server 4.1+). */
  def estimatedSize: Long = RemoteOps.modelSize(ref)

  override def write: MLWriter = new MLWriter(this)
}

object RandomForestClassificationModel extends MLReadable[RandomForestClassificationModel] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.classification.RandomForestClassificationModel"

  override def load(path: String): RandomForestClassificationModel =
    RemoteIO.loadModel(
      OperatorName,
      path,
      (id, ref) => new RandomForestClassificationModel(id, ref)
    )
}
