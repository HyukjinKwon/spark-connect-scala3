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
package org.apache.spark.ml.tuning

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, rand}

/** Params of [[TrainValidationSplit]]. */
trait TrainValidationSplitParams extends Params with HasSeed with HasCollectSubModels {

  /** The estimator to tune. */
  final val estimator: Param[Estimator[_]] =
    new Param[Estimator[_]](this, "estimator", "estimator to be cross-validated")

  /** Parameter maps to evaluate. */
  final val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param[Array[ParamMap]](this, "estimatorParamMaps", "param maps for the estimator")

  /** The evaluator used to score the models on the validation split. */
  final val evaluator: Param[Evaluator] =
    new Param[Evaluator](this, "evaluator", "evaluator used to select hyper-parameters")

  /** The fraction of rows used for training, in (0, 1). */
  final val trainRatio: DoubleParam = new DoubleParam(
    this,
    "trainRatio",
    "Param for ratio between train and validation data. Must be between 0 and 1.",
    ParamValidators.inRange(0.0, 1.0)
  )

  /** The number of threads for parallel fits. Fits run one after another in this client. */
  final val parallelism: IntParam = new IntParam(
    this,
    "parallelism",
    "the number of threads to use when running parallel algorithms (>= 1)",
    ParamValidators.gtEq(1)
  )

  setDefault(trainRatio -> 0.75, parallelism -> 1)

  def getEstimator: Estimator[_] = $(estimator)

  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  def getEvaluator: Evaluator = $(evaluator)

  def getTrainRatio: Double = $(trainRatio)

  def getParallelism: Int = $(parallelism)
}

/**
 * Tunes parameters on one train/validation split. Each parameter map is fit on the training rows,
 * scored on the validation rows, and the best map is refit on the full dataset. Mirrors
 * `org.apache.spark.ml.tuning.TrainValidationSplit`.
 */
class TrainValidationSplit(override val uid: String)
    extends Estimator[TrainValidationSplitModel]
    with TrainValidationSplitParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("tvs"))

  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setParallelism(value: Int): this.type = set(parallelism, value)

  def setCollectSubModels(value: Boolean): this.type = set(collectSubModels, value)

  override def fit(dataset: Dataset[_]): TrainValidationSplitModel = {
    val est = $(estimator)
    val epm = $(estimatorParamMaps)
    val eva = $(evaluator)
    val numModels = epm.length
    val randCol = uid + "_rand"
    val withRand = dataset.toDF().select(col("*"), rand($(seed)).alias(randCol))
    val ratio = $(trainRatio)
    val train = withRand.filter(col(randCol) < lit(ratio)).drop(randCol)
    val validation = withRand.filter(col(randCol) >= lit(ratio)).drop(randCol)
    val metrics = new Array[Double](numModels)
    val subModels: Array[Model[_]] =
      if ($(collectSubModels)) new Array[Model[_]](numModels) else null
    epm.zipWithIndex.foreach { case (paramMap, j) =>
      val model = est.fit(train, paramMap).asInstanceOf[Model[_]]
      metrics(j) = eva.evaluate(model.transform(validation, paramMap))
      if (subModels != null) subModels(j) = model else model.release()
    }
    val bestIndex =
      if (eva.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)._2
      else metrics.zipWithIndex.minBy(_._1)._2
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TrainValidationSplitModel(uid, bestModel, metrics, subModels))
  }

  override def copy(extra: ParamMap): TrainValidationSplit =
    copyValues(new TrainValidationSplit(uid), extra)

  override def write: MLWriter = new TrainValidationSplitWriter(this)
}

object TrainValidationSplit extends MLReadable[TrainValidationSplit] {

  override def load(path: String): TrainValidationSplit = {
    val lines = org.apache.spark.ml.PipelineIO.readLines(path)
    val uid = lines.collectFirst { case Seq("uid", u) => u }.get
    val tvs = new TrainValidationSplit(uid)
    val est = TuningUtils.loadStage(
      lines.collectFirst { case Seq("estimatorClass", c) => c }.get,
      s"$path/estimator"
    )
    val eva = TuningUtils.loadStage(
      lines.collectFirst { case Seq("evaluatorClass", c) => c }.get,
      s"$path/evaluator"
    )
    tvs.set(tvs.estimator, est.asInstanceOf[Estimator[_]])
    tvs.set(tvs.evaluator, eva.asInstanceOf[Evaluator])
    tvs.set(
      tvs.estimatorParamMaps,
      TuningUtils.decodeParamMaps(
        est,
        lines.filter(_.headOption.contains("epm")).map(_.mkString("\t"))
      )
    )
    tvs.set(tvs.trainRatio, lines.collectFirst { case Seq("trainRatio", v) => v.toDouble }.get)
    tvs
  }
}

/**
 * The best model of a [[TrainValidationSplit]] run, with the metric of every parameter map. Mirrors
 * `org.apache.spark.ml.tuning.TrainValidationSplitModel`.
 */
class TrainValidationSplitModel private[ml] (
    override val uid: String,
    val bestModel: Model[_],
    val validationMetrics: Array[Double],
    val subModels: Array[Model[_]]
) extends Model[TrainValidationSplitModel]
    with TrainValidationSplitParams
    with MLWritable {

  override def transform(dataset: Dataset[_]): DataFrame = bestModel.transform(dataset)

  override def copy(extra: ParamMap): TrainValidationSplitModel =
    copyValues(
      new TrainValidationSplitModel(
        uid,
        bestModel.copy(extra).asInstanceOf[Model[_]],
        validationMetrics.clone(),
        subModels
      ),
      extra
    )

  override def release(): Unit = {
    bestModel.release()
    if (subModels != null) subModels.foreach(_.release())
  }

  override def write: MLWriter = new TrainValidationSplitModelWriter(this)
}

object TrainValidationSplitModel extends MLReadable[TrainValidationSplitModel] {

  override def load(path: String): TrainValidationSplitModel = {
    val lines = org.apache.spark.ml.PipelineIO.readLines(path)
    val uid = lines.collectFirst { case Seq("uid", u) => u }.get
    val best = TuningUtils.loadStage(
      lines.collectFirst { case Seq("bestModelClass", c) => c }.get,
      s"$path/bestModel"
    )
    val model = new TrainValidationSplitModel(
      uid,
      best.asInstanceOf[Model[_]],
      TuningUtils.splitDoubles(
        lines.collectFirst { case Seq("validationMetrics", v) => v }.getOrElse("")
      ),
      null
    )
    val est = TuningUtils.loadStage(
      lines.collectFirst { case Seq("estimatorClass", c) => c }.get,
      s"$path/estimator"
    )
    val eva = TuningUtils.loadStage(
      lines.collectFirst { case Seq("evaluatorClass", c) => c }.get,
      s"$path/evaluator"
    )
    model.set(model.estimator, est.asInstanceOf[Estimator[_]])
    model.set(model.evaluator, eva.asInstanceOf[Evaluator])
    model
  }
}

private[tuning] class TrainValidationSplitWriter(instance: TrainValidationSplit)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    val est = instance.getEstimator.asInstanceOf[org.apache.spark.ml.param.Params]
    val eva = instance.getEvaluator.asInstanceOf[org.apache.spark.ml.param.Params]
    TuningUtils.saveStage(est, s"$path/estimator", isOverwrite)
    TuningUtils.saveStage(eva, s"$path/evaluator", isOverwrite)
    org.apache.spark.ml.PipelineIO.writeLines(
      path,
      Seq(
        s"class\t${instance.getClass.getName}",
        s"uid\t${instance.uid}",
        s"estimatorClass\t${est.getClass.getName}",
        s"evaluatorClass\t${eva.getClass.getName}",
        s"trainRatio\t${instance.getTrainRatio}"
      ) ++ TuningUtils.encodeParamMaps(instance.getEstimatorParamMaps.toSeq),
      isOverwrite
    )
  }
}

private[tuning] class TrainValidationSplitModelWriter(instance: TrainValidationSplitModel)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    TuningUtils.saveStage(instance.bestModel, s"$path/bestModel", isOverwrite)
    val est = instance.getEstimator.asInstanceOf[org.apache.spark.ml.param.Params]
    val eva = instance.getEvaluator.asInstanceOf[org.apache.spark.ml.param.Params]
    TuningUtils.saveStage(est, s"$path/estimator", isOverwrite)
    TuningUtils.saveStage(eva, s"$path/evaluator", isOverwrite)
    org.apache.spark.ml.PipelineIO.writeLines(
      path,
      Seq(
        s"class\t${instance.getClass.getName}",
        s"uid\t${instance.uid}",
        s"bestModelClass\t${instance.bestModel.getClass.getName}",
        s"estimatorClass\t${est.getClass.getName}",
        s"evaluatorClass\t${eva.getClass.getName}",
        s"validationMetrics\t${TuningUtils.joinDoubles(instance.validationMetrics.toSeq)}"
      ),
      isOverwrite
    )
  }
}
