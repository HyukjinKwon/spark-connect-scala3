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

import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.connect.MlSerializer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{DefaultParams, Json, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, rand}

/** Helpers shared by the tuning estimators: fold splitting, parallel runs and Spark's format. */
private[ml] object TuningUtils {

  /**
   * Splits `dataset` into `numFolds` (train, validation) pairs. Without `foldCol`, each row gets a
   * seeded uniform random value, and fold `i` validates on the rows in `[i/n, (i+1)/n)`. With
   * `foldCol`, the integer values of that column select the validation fold.
   */
  def kFold(
      dataset: Dataset[_],
      numFolds: Int,
      foldCol: String,
      seed: Long,
      uid: String
  ): Seq[(DataFrame, DataFrame)] = {
    val df = dataset.toDF()
    if (foldCol.isEmpty) {
      val h = 1.0 / numFolds
      val randCol = uid + "_rand"
      val withRand = df.select(col("*"), rand(seed).alias(randCol))
      (0 until numFolds).map { i =>
        val lb = i * h
        val ub = (i + 1) * h
        val condition = col(randCol) >= lb && col(randCol) < ub
        (withRand.filter(!condition), withRand.filter(condition))
      }
    } else {
      (0 until numFolds).map { i =>
        (df.filter(col(foldCol) =!= lit(i)), df.filter(col(foldCol) === lit(i)))
      }
    }
  }

  /** Runs `tasks` on a pool of `parallelism` threads and returns their results in order. */
  def runParallel[T](parallelism: Int)(tasks: Seq[() => T]): Seq[T] = {
    val pool: ExecutorService = Executors.newFixedThreadPool(math.max(1, parallelism))
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try
      Await.result(Future.sequence(tasks.map(t => Future(t()))), Duration.Inf)
    finally
      pool.shutdown()
  }

  /** The params of `root` and of every stage nested in it, keyed by uid. */
  def uidMap(root: Params): Map[String, Params] = {
    val nested: Seq[Params] = root match {
      case p: org.apache.spark.ml.Pipeline => p.getStages.toSeq
      case p: org.apache.spark.ml.PipelineModel => p.stages.toSeq
      case p: CrossValidator => Seq(p.getEstimator, p.getEvaluator).collect { case x: Params => x }
      case p: TrainValidationSplit =>
        Seq(p.getEstimator, p.getEvaluator).collect { case x: Params => x }
      case _ => Nil
    }
    (Seq(root) ++ nested.flatMap(n => uidMap(n).values)).map(p => p.uid -> p).toMap
  }

  /**
   * Writes a validator's metadata, its evaluator and estimator, and any param values that are
   * stages (saved as `epm_<name><n>` directories). Mirrors `ValidatorParams.saveImpl`.
   */
  def saveValidator(
      path: String,
      uid: String,
      className: String,
      paramMap: ParamMap,
      estimator: Params,
      evaluator: Params,
      epms: Seq[ParamMap],
      overwrite: Boolean,
      extra: Seq[(String, Json.Value)]
  ): Unit = {
    var numParamsNotJson = 0
    val epmJson = epms.map { map =>
      Json.Arr(map.toSeq.map { pair =>
        val name = pair.param.name
        val base = Seq("parent" -> Json.Str(pair.param.parent), "name" -> Json.Str(name))
        pair.value match {
          case s: Params with MLWritable if isStage(s) =>
            val relative = s"epm_$name$numParamsNotJson"
            numParamsNotJson += 1
            saveStage(s, s"$path/$relative", overwrite)
            Json.Obj(base ++ Seq("value" -> Json.Str(relative), "isJson" -> Json.Str("false")))
          case v =>
            val encoded = DefaultParams
              .encodeParam(v)
              .getOrElse(
                throw new IllegalArgumentException(s"Cannot save param ${pair.param} value $v")
              )
            Json.Obj(
              base ++ Seq("value" -> Json.Str(Json.render(encoded)), "isJson" -> Json.Str("true"))
            )
        }
      })
    }
    val skip = Set("estimator", "evaluator", "estimatorParamMaps")
    val paramMapJson = DefaultParams.paramMapJson(paramMap, skip) match {
      case Json.Obj(fs) => Json.Obj(fs ++ Seq("estimatorParamMaps" -> Json.Arr(epmJson)))
    }
    DefaultParams.save(uid, className, ParamMap.empty, path, overwrite, paramMapJson, extra)
    saveStage(evaluator, s"$path/evaluator", overwrite)
    saveStage(estimator, s"$path/estimator", overwrite)
  }

  /** Whether `v` is an estimator, transformer or evaluator (a stage, not a plain value). */
  private def isStage(v: Any): Boolean =
    v.isInstanceOf[Estimator[_]] || v.isInstanceOf[Transformer] || v.isInstanceOf[Evaluator]

  /** Saves `stage` to `path`, with the overwrite policy of the enclosing save. */
  def saveStage(stage: Params, path: String, overwrite: Boolean): Unit = stage match {
    case w: MLWritable => if (overwrite) w.write.overwrite().save(path) else w.write.save(path)
    case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not writable.")
  }

  /** Loads the metadata-recorded stage at `path`. */
  def loadStage(path: String): Params = DefaultParams.loadInstance(path).asInstanceOf[Params]

  /** The result of loading a validator: its metadata, estimator, evaluator and param maps. */
  final case class Loaded(
      meta: DefaultParams.Metadata,
      estimator: Estimator[_],
      evaluator: Evaluator,
      paramMaps: Array[ParamMap]
  )

  /** Loads a validator saved by [[saveValidator]] or by PySpark's `ValidatorSharedReadWrite`. */
  def loadValidator(path: String): Loaded = {
    val meta = DefaultParams.load(path)
    val evaluator = loadStage(s"$path/evaluator").asInstanceOf[Evaluator]
    val estimator = loadStage(s"$path/estimator").asInstanceOf[Estimator[_]]
    val uidToParams = uidMap(estimator) + (evaluator.uid -> evaluator)
    val epmJson = meta.paramMap.get("estimatorParamMaps") match {
      case Some(Json.Arr(vs)) => vs
      case _ => Nil
    }
    val maps = epmJson.map {
      case Json.Arr(pairs) =>
        val map = new ParamMap()
        pairs.foreach { case pair: Json.Obj =>
          val owner = uidToParams.getOrElse(
            pair.getStr("parent"),
            throw new IllegalArgumentException(s"Unknown parent ${pair.getStr("parent")}")
          )
          val param = owner.getParam(pair.getStr("name")).asInstanceOf[Param[Any]]
          val isJson = pair.get("isJson") match {
            case Some(Json.Bool(b)) => b
            case Some(Json.Str(s)) => s.toBoolean
            case _ => true
          }
          if (isJson) {
            val text = pair.getStr("value")
            map.put(param, DefaultParams.decodeParam(param, Json.parse(text)))
          } else {
            val rel = pair.getStr("value")
            map.put(param, loadStage(s"$path/$rel"))
          }
        }
        map
      case _ => new ParamMap()
    }.toArray
    Loaded(meta, estimator, evaluator, maps)
  }

  /** Writes `path/<name>` for each non-empty sub-model directory, as persistSubModels does. */
  def saveSubModels(path: String, subModels: Array[Array[Model[_]]], overwrite: Boolean): Unit =
    subModels.zipWithIndex.foreach { case (models, fold) =>
      models.zipWithIndex.foreach { case (m, index) =>
        saveStage(m, s"$path/subModels/fold$fold/$index", overwrite)
      }
    }

  /** Loads sub-models saved by [[saveSubModels]]. */
  def loadSubModels(path: String, folds: Int, maps: Int): Array[Array[Model[_]]] =
    Array.tabulate(folds, maps)((fold, index) =>
      loadStage(s"$path/subModels/fold$fold/$index").asInstanceOf[Model[_]]
    )

  /** The JSON array of doubles, for metric fields. */
  def doubles(values: Seq[Double]): Json.Value =
    Json.Arr(values.map(v => Json.Num(v.toString)))

  /** Parses a JSON array of doubles. */
  def parseDoubles(v: Option[Json.Value]): Array[Double] = v match {
    case Some(Json.Arr(vs)) => vs.collect { case Json.Num(t) => t.toDouble }.toArray
    case _ => Array.empty[Double]
  }

  /**
   * The params a fitted tuning model records: those its PySpark counterpart defines. Spark's Scala
   * model also records `parallelism` and `collectSubModels`, which the Python model does not
   * define, so they are left out to keep the metadata loadable by PySpark's model reader.
   */
  def modelParams(model: Params): ParamMap = {
    val keep = Set("numFolds", "foldCol", "seed", "trainRatio")
    ParamMap(model.extractParamMap().toSeq.filter(p => keep.contains(p.param.name))*)
  }

  /** Whether the metadata records the `persistSubModels` option. */
  def persistSubModels(meta: DefaultParams.Metadata): Boolean =
    meta.raw.get("persistSubModels").exists {
      case Json.Bool(b) => b
      case Json.Str(s) => s.toBoolean
      case _ => false
    }
}
