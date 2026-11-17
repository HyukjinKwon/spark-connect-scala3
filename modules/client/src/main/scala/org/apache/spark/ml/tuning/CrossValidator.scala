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

/** Params of [[CrossValidator]]. */
trait CrossValidatorParams extends Params with HasSeed with HasCollectSubModels {

  /** The estimator to cross-validate. */
  final val estimator: Param[Estimator[_]] =
    new Param[Estimator[_]](this, "estimator", "estimator to be cross-validated")

  /** Parameter maps to evaluate, one model is fit per map and fold. */
  final val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param[Array[ParamMap]](this, "estimatorParamMaps", "param maps for the estimator")

  /** The evaluator used to score the models on the validation folds. */
  final val evaluator: Param[Evaluator] =
    new Param[Evaluator](this, "evaluator", "evaluator used to select hyper-parameters")

  /** The number of folds. */
  final val numFolds: IntParam =
    new IntParam(
      this,
      "numFolds",
      "number of folds for cross validation (>= 2)",
      ParamValidators.gtEq(2)
    )

  /** The column that assigns each row to a fold, or empty for a random split. */
  final val foldCol: Param[String] = new Param[String](
    this,
    "foldCol",
    "Param for the column name of user specified fold number. Once this is specified, " +
      "CrossValidator won't do random k-fold split. Note that this column should be integer type " +
      "with range [0, numFolds) and Spark will throw exception on out-of-range fold numbers."
  )

  /** The number of fits to run at once. */
  final val parallelism: IntParam = new IntParam(
    this,
    "parallelism",
    "the number of threads to use when running parallel algorithms (>= 1)",
    ParamValidators.gtEq(1)
  )

  setDefault(numFolds -> 3, foldCol -> "", parallelism -> 1)

  def getEstimator: Estimator[_] = $(estimator)

  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  def getEvaluator: Evaluator = $(evaluator)

  def getNumFolds: Int = $(numFolds)

  def getFoldCol: String = $(foldCol)

  def getParallelism: Int = $(parallelism)
}

/**
 * K-fold cross validation. Each fold fits every parameter map on the server, scores it with the
 * evaluator, and the best map is refit on the full dataset. Mirrors
 * `org.apache.spark.ml.tuning.CrossValidator`. The folds run in this client, as PySpark does over
 * Connect; `parallelism` sets how many fits run at once.
 */
class CrossValidator(override val uid: String)
    extends Estimator[CrossValidatorModel]
    with CrossValidatorParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("cv"))

  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  def setNumFolds(value: Int): this.type = set(numFolds, value)

  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setParallelism(value: Int): this.type = set(parallelism, value)

  def setCollectSubModels(value: Boolean): this.type = set(collectSubModels, value)

  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
    val est = $(estimator)
    val epm = $(estimatorParamMaps)
    val eva = $(evaluator)
    val nFolds = $(numFolds)
    val numModels = epm.length
    val folds = TuningUtils.kFold(dataset, nFolds, $(foldCol), $(seed), uid)
    val collect = $(collectSubModels)
    // One task per (fold, map): fit on the training rows and score on the validation rows.
    val tasks = for {
      (fold, i) <- folds.zipWithIndex
      (paramMap, j) <- epm.zipWithIndex
    } yield () => {
      val (train, validation) = fold
      val model = est.fit(train, paramMap).asInstanceOf[Model[_]]
      val metric = eva.evaluate(model.transform(validation, paramMap))
      if (!collect) model.release()
      (i, j, metric, model)
    }
    val results = TuningUtils.runParallel($(parallelism))(tasks)
    val metrics = Array.fill(nFolds, numModels)(0.0)
    val subModels: Array[Array[Model[_]]] =
      if (collect) Array.fill(nFolds, numModels)(null) else null
    results.foreach { case (i, j, metric, model) =>
      metrics(i)(j) = metric
      if (subModels != null) subModels(i)(j) = model
    }
    val avgMetrics = metrics.transpose.map(m => m.sum / nFolds)
    val stdMetrics = metrics.transpose.map { m =>
      val mean = m.sum / nFolds
      math.sqrt(m.map(x => (x - mean) * (x - mean)).sum / nFolds)
    }
    val bestIndex =
      if (eva.isLargerBetter) avgMetrics.zipWithIndex.maxBy(_._1)._2
      else avgMetrics.zipWithIndex.minBy(_._1)._2
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModel(uid, bestModel, avgMetrics, stdMetrics, subModels))
  }

  override def copy(extra: ParamMap): CrossValidator = copyValues(new CrossValidator(uid), extra)

  override def write: MLWriter = new CrossValidatorWriter(this)
}

object CrossValidator extends MLReadable[CrossValidator] {

  override def load(path: String): CrossValidator = {
    val loaded = TuningUtils.loadValidator(path)
    val cv = new CrossValidator(loaded.meta.uid)
    DefaultParams.getAndSetParams(cv, loaded.meta, Set("estimatorParamMaps"))
    cv.set(cv.estimator, loaded.estimator)
    cv.set(cv.evaluator, loaded.evaluator)
    cv.set(cv.estimatorParamMaps, loaded.paramMaps)
    cv
  }
}

/**
 * The best model of a [[CrossValidator]] run, with the metrics of every parameter map. Mirrors
 * `org.apache.spark.ml.tuning.CrossValidatorModel`.
 *
 * @param bestModel
 *   the model refit on the full dataset with the best parameter map
 * @param avgMetrics
 *   the average metric of each parameter map over the folds
 * @param stdMetrics
 *   the standard deviation of the metric of each parameter map over the folds
 * @param subModels
 *   the models of every fold and map, or null unless `collectSubModels` was set
 */
class CrossValidatorModel private[ml] (
    override val uid: String,
    val bestModel: Model[_],
    val avgMetrics: Array[Double],
    val stdMetrics: Array[Double],
    val subModels: Array[Array[Model[_]]]
) extends Model[CrossValidatorModel]
    with CrossValidatorParams
    with MLWritable {

  override def transform(dataset: Dataset[_]): DataFrame = bestModel.transform(dataset)

  override def copy(extra: ParamMap): CrossValidatorModel =
    copyValues(
      new CrossValidatorModel(
        uid,
        bestModel.copy(extra).asInstanceOf[Model[_]],
        avgMetrics.clone(),
        stdMetrics.clone(),
        subModels
      ),
      extra
    )

  override def release(): Unit = {
    bestModel.release()
    if (subModels != null) subModels.foreach(_.foreach(_.release()))
  }

  override def write: MLWriter = new CrossValidatorModelWriter(this)
}

object CrossValidatorModel extends MLReadable[CrossValidatorModel] {

  override def load(path: String): CrossValidatorModel = {
    val loaded = TuningUtils.loadValidator(path)
    val meta = loaded.meta
    val numFolds = meta.paramMap.get("numFolds") match {
      case Some(Json.Num(t)) => t.toDouble.toInt
      case _ => 3
    }
    val bestModel = TuningUtils.loadStage(s"$path/bestModel").asInstanceOf[Model[_]]
    val subModels =
      if (TuningUtils.persistSubModels(meta)) {
        TuningUtils.loadSubModels(path, numFolds, loaded.paramMaps.length)
      } else {
        null
      }
    val model = new CrossValidatorModel(
      meta.uid,
      bestModel,
      TuningUtils.parseDoubles(meta.raw.get("avgMetrics")),
      TuningUtils.parseDoubles(meta.raw.get("stdMetrics")),
      subModels
    )
    DefaultParams.getAndSetParams(model, meta, Set("estimatorParamMaps"))
    model.set(model.estimator, loaded.estimator)
    model.set(model.evaluator, loaded.evaluator)
    model.set(model.estimatorParamMaps, loaded.paramMaps)
    model
  }
}

private[tuning] class CrossValidatorWriter(instance: CrossValidator) extends MLWriter(instance) {
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

private[tuning] class CrossValidatorModelWriter(instance: CrossValidatorModel)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    val persist = optionValue("persistSubModels").map(_.toBoolean).getOrElse(false)
    if (persist && instance.subModels == null) {
      throw new IllegalArgumentException(
        "Persisting sub-models requires collectSubModels to be set when the tuning was run."
      )
    }
    val extra: Seq[(String, Json.Value)] =
      Seq(
        "avgMetrics" -> TuningUtils.doubles(instance.avgMetrics.toSeq),
        "persistSubModels" -> Json.Bool(persist)
      ) ++
        (if (instance.stdMetrics.nonEmpty)
           Seq("stdMetrics" -> TuningUtils.doubles(instance.stdMetrics.toSeq))
         else Nil)
    TuningUtils.saveValidator(
      path,
      instance.uid,
      instance.getClass.getName,
      TuningUtils.modelParams(instance),
      instance.getEstimator.asInstanceOf[Params],
      instance.getEvaluator.asInstanceOf[Params],
      instance.getEstimatorParamMaps.toSeq,
      isOverwrite,
      extra
    )
    TuningUtils.saveStage(instance.bestModel.asInstanceOf[Params], s"$path/bestModel", isOverwrite)
    if (persist) TuningUtils.saveSubModels(path, instance.subModels, isOverwrite)
  }
}
