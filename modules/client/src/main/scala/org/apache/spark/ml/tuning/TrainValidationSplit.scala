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
import org.apache.spark.ml.util.{
  DefaultParams,
  Identifiable,
  Json,
  MLReadable,
  MLWritable,
  MLWriter
}
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

  /** The number of fits to run at once. */
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
    val randCol = uid + "_rand"
    val withRand = dataset.toDF().select(col("*"), rand($(seed)).alias(randCol))
    val ratio = $(trainRatio)
    val train = withRand.filter(col(randCol) < lit(ratio)).drop(randCol)
    val validation = withRand.filter(col(randCol) >= lit(ratio)).drop(randCol)
    val collect = $(collectSubModels)
    val tasks = epm.zipWithIndex.map { case (paramMap, j) =>
      () => {
        val model = est.fit(train, paramMap).asInstanceOf[Model[_]]
        val metric = eva.evaluate(model.transform(validation, paramMap))
        if (!collect) model.release()
        (j, metric, model)
      }
    }
    val results = TuningUtils.runParallel($(parallelism))(tasks)
    val metrics = new Array[Double](epm.length)
    val subModels: Array[Model[_]] = if (collect) new Array[Model[_]](epm.length) else null
    results.foreach { case (j, metric, model) =>
      metrics(j) = metric
      if (subModels != null) subModels(j) = model
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
    val loaded = TuningUtils.loadValidator(path)
    val tvs = new TrainValidationSplit(loaded.meta.uid)
    DefaultParams.getAndSetParams(tvs, loaded.meta, Set("estimatorParamMaps"))
    tvs.set(tvs.estimator, loaded.estimator)
    tvs.set(tvs.evaluator, loaded.evaluator)
    tvs.set(tvs.estimatorParamMaps, loaded.paramMaps)
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
    val loaded = TuningUtils.loadValidator(path)
    val meta = loaded.meta
    val bestModel = TuningUtils.loadStage(s"$path/bestModel").asInstanceOf[Model[_]]
    val subModels =
      if (TuningUtils.persistSubModels(meta)) {
        TuningUtils.loadSubModels(path, 1, loaded.paramMaps.length).head
      } else {
        null
      }
    val model = new TrainValidationSplitModel(
      meta.uid,
      bestModel,
      TuningUtils.parseDoubles(meta.raw.get("validationMetrics")),
      subModels
    )
    DefaultParams.getAndSetParams(model, meta, Set("estimatorParamMaps"))
    model.set(model.estimator, loaded.estimator)
    model.set(model.evaluator, loaded.evaluator)
    model.set(model.estimatorParamMaps, loaded.paramMaps)
    model
  }
}

private[tuning] class TrainValidationSplitWriter(instance: TrainValidationSplit)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit =
    TuningUtils.saveValidator(
      path,
      instance.uid,
      instance.getClass.getName,
      instance.extractParamMap(),
      instance.getEstimator.asInstanceOf[Params],
      instance.getEvaluator.asInstanceOf[Params],
      instance.getEstimatorParamMaps.toSeq,
      isOverwrite,
      Nil
    )
}

private[tuning] class TrainValidationSplitModelWriter(instance: TrainValidationSplitModel)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    val persist = optionValue("persistSubModels").map(_.toBoolean).getOrElse(false)
    if (persist && instance.subModels == null) {
      throw new IllegalArgumentException(
        "Persisting sub-models requires collectSubModels to be set when the tuning was run."
      )
    }
    TuningUtils.saveValidator(
      path,
      instance.uid,
      instance.getClass.getName,
      TuningUtils.modelParams(instance),
      instance.getEstimator.asInstanceOf[Params],
      instance.getEvaluator.asInstanceOf[Params],
      instance.getEstimatorParamMaps.toSeq,
      isOverwrite,
      Seq(
        "validationMetrics" -> TuningUtils.doubles(instance.validationMetrics.toSeq),
        "persistSubModels" -> Json.Bool(persist)
      )
    )
    TuningUtils.saveStage(instance.bestModel.asInstanceOf[Params], s"$path/bestModel", isOverwrite)
    if (persist) {
      instance.subModels.zipWithIndex.foreach { case (m, j) =>
        TuningUtils.saveStage(m.asInstanceOf[Params], s"$path/subModels/fold0/$j", isOverwrite)
      }
    }
  }
}
