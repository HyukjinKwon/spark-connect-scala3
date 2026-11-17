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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.connect.{RemoteIO, RemoteOps}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of [[PowerIterationClustering]]. */
trait PowerIterationClusteringParams extends Params with HasMaxIter with HasWeightCol {

  /** The number of clusters. */
  final val k: IntParam =
    new IntParam(this, "k", "The number of clusters to create (k).", ParamValidators.gt(1))

  /** The initialization mode: `random` or `degree`. */
  final val initMode: Param[String] = new Param[String](
    this,
    "initMode",
    "The initialization algorithm. This can be either \"random\" to use a random vector as vertex " +
      "properties, or \"degree\" to use a normalized sum of similarities with other vertices.",
    ParamValidators.inArray(Array("random", "degree"))
  )

  /** The source vertex column. */
  final val srcCol: Param[String] =
    new Param[String](this, "srcCol", "Name of the input column for source vertex IDs.")

  /** The destination vertex column. */
  final val dstCol: Param[String] =
    new Param[String](this, "dstCol", "Name of the input column for destination vertex IDs.")

  setDefault(srcCol -> "src", dstCol -> "dst", k -> 2, maxIter -> 20, initMode -> "random")

  def getK: Int = $(k)

  def getInitMode: String = $(initMode)

  def getSrcCol: String = $(srcCol)

  def getDstCol: String = $(dstCol)
}

/**
 * Power Iteration Clustering over an edge DataFrame, run on the server. Mirrors
 * `org.apache.spark.ml.clustering.PowerIterationClustering`. It is not an estimator: calling
 * [[assignClusters]] runs the algorithm once.
 */
class PowerIterationClustering(override val uid: String)
    extends PowerIterationClusteringParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("PowerIterationClustering"))

  def setK(value: Int): this.type = set(k, value)

  def setInitMode(value: String): this.type = set(initMode, value)

  def setSrcCol(value: String): this.type = set(srcCol, value)

  def setDstCol(value: String): this.type = set(dstCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** Assigns each vertex of the edges in `dataset` to a cluster. */
  def assignClusters(dataset: Dataset[_]): DataFrame = {
    val weight = if (isDefined(weightCol)) $(weightCol) else ""
    RemoteOps.fetchRelation(
      RemoteOps.helperRef(dataset.sparkSession),
      Seq("powerIterationClusteringAssignClusters"),
      Seq(dataset, $(k), $(maxIter), $(initMode), $(srcCol), $(dstCol), weight)
    )
  }

  override def write: MLWriter = new PowerIterationClusteringWriter(this)

  private[clustering] def toWrapper: PowerIterationClusteringWrapper = {
    val wrapper = new PowerIterationClusteringWrapper(uid)
    copyValues(wrapper)
  }
}

object PowerIterationClustering extends MLReadable[PowerIterationClustering] {

  override def load(path: String): PowerIterationClustering = {
    val wrapper = PowerIterationClusteringWrapper.load(path)
    val pic = new PowerIterationClustering(wrapper.uid)
    wrapper.copyValues(pic)
    pic
  }
}

/** Writes a [[PowerIterationClustering]] as Spark does, through its wrapper transformer. */
private[clustering] class PowerIterationClusteringWriter(instance: PowerIterationClustering)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = instance.toWrapper.write.save(path)
}

/**
 * The transformer that Spark saves a [[PowerIterationClustering]] as. Its transform runs the
 * clustering on the server. Mirrors
 * `org.apache.spark.ml.clustering.PowerIterationClusteringWrapper`.
 */
class PowerIterationClusteringWrapper(override val uid: String)
    extends Transformer
    with PowerIterationClusteringParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("PowerIterationClusteringWrapper"))

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithTransformer(this, PowerIterationClusteringWrapper.OperatorName, dataset)

  override def copy(extra: ParamMap): PowerIterationClusteringWrapper =
    copyValues(new PowerIterationClusteringWrapper(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object PowerIterationClusteringWrapper extends MLReadable[PowerIterationClusteringWrapper] {

  private[ml] val OperatorName: String =
    "org.apache.spark.ml.clustering.PowerIterationClusteringWrapper"

  override def load(path: String): PowerIterationClusteringWrapper =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER,
      path,
      new PowerIterationClusteringWrapper(_)
    )
}
