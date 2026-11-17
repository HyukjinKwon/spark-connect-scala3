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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.functions.{array_to_vector, vector_to_array}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.tuning.TuningUtils
import org.apache.spark.ml.util.{
  DefaultParams,
  Identifiable,
  Json,
  MLReadable,
  MLWritable,
  MLWriter
}
import org.apache.spark.sql.{functions => F, DataFrame, Dataset}

/** Params of [[OneVsRest]]. */
trait OneVsRestParams
    extends Params
    with HasFeaturesCol
    with HasLabelCol
    with HasPredictionCol
    with HasWeightCol {

  /** The base binary classifier, fit once per class. */
  final val classifier: Param[Estimator[_]] =
    new Param[Estimator[_]](this, "classifier", "base binary classifier")

  /** The number of classes fit at once. */
  final val parallelism: IntParam = new IntParam(
    this,
    "parallelism",
    "the number of threads to use when running parallel algorithms (>= 1)",
    ParamValidators.gtEq(1)
  )

  setDefault(parallelism -> 1)

  def getClassifier: Estimator[_] = $(classifier)

  def getParallelism: Int = $(parallelism)
}

/**
 * One-vs-rest multiclass reduction: a binary classifier is fit per class, with the label set to 1
 * for that class and 0 otherwise. The fits run on the server; the reduction runs in this client, as
 * it does in PySpark over Connect. Mirrors `org.apache.spark.ml.classification.OneVsRest`.
 */
class OneVsRest(override val uid: String)
    extends org.apache.spark.ml.Estimator[OneVsRestModel]
    with OneVsRestParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("oneVsRest"))

  def setClassifier(value: Estimator[_]): this.type = set(classifier, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setParallelism(value: Int): this.type = set(parallelism, value)

  override def fit(dataset: Dataset[_]): OneVsRestModel = {
    val df = dataset.toDF()
    val label = $(labelCol)
    val maxLabel = df.select(F.max(F.col(label))).head().get(0).asInstanceOf[Number].doubleValue()
    val numClasses = maxLabel.toInt + 1
    val base = $(classifier)
    val tasks = (0 until numClasses).map { idx => () =>
      {
        val binary =
          df.withColumn(label, F.when(F.col(label) === F.lit(idx.toDouble), 1.0).otherwise(0.0))
        val weight = if (isDefined(weightCol)) $(weightCol) else ""
        val est = OneVsRest.configure(base, $(featuresCol), label, $(predictionCol), weight)
        est.fit(binary).asInstanceOf[Model[_]]
      }
    }
    val models = TuningUtils.runParallel($(parallelism))(tasks)
    copyValues(new OneVsRestModel(uid, models.toArray, numClasses))
  }

  override def copy(extra: ParamMap): OneVsRest = copyValues(new OneVsRest(uid), extra)

  override def write: MLWriter = new OneVsRestWriter(this)
}

object OneVsRest extends MLReadable[OneVsRest] {

  /** A copy of `base` set to this reduction's columns, with weights when the base takes them. */
  private[classification] def configure(
      base: Estimator[_],
      featuresCol: String,
      labelCol: String,
      predictionCol: String,
      weightCol: String
  ): Estimator[_] = {
    val extra = ParamMap.empty
    base match {
      case p: Params =>
        def put(name: String, value: Any): Unit =
          if (p.hasParam(name)) extra.put(p.getParam(name).asInstanceOf[Param[Any]], value)
        put("featuresCol", featuresCol)
        put("labelCol", labelCol)
        put("predictionCol", predictionCol)
        if (weightCol.nonEmpty) put("weightCol", weightCol)
      case _ =>
    }
    base.copy(extra)
  }

  override def load(path: String): OneVsRest = {
    val meta = DefaultParams.load(path)
    val ovr = new OneVsRest(meta.uid)
    DefaultParams.getAndSetParams(ovr, meta, Set("classifier"))
    ovr.set(ovr.classifier, TuningUtils.loadStage(s"$path/classifier").asInstanceOf[Estimator[_]])
    ovr
  }
}

/** Writes a [[OneVsRest]]: its metadata and its base classifier under `classifier`. */
private[classification] class OneVsRestWriter(instance: OneVsRest) extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    DefaultParams.save(
      instance.uid,
      instance.getClass.getName,
      org.apache.spark.ml.param.ParamMap.empty,
      path,
      isOverwrite,
      DefaultParams.paramMapJson(instance.extractParamMap(), Set("classifier"))
    )
    TuningUtils.saveStage(
      instance.getClassifier.asInstanceOf[Params],
      s"$path/classifier",
      isOverwrite
    )
  }
}

/**
 * The fitted one-vs-rest model: one binary model per class. Prediction takes the class whose binary
 * model gives the largest raw score. Mirrors `org.apache.spark.ml.classification.OneVsRestModel`.
 *
 * @param models
 *   the binary models, indexed by class
 */
class OneVsRestModel private[ml] (
    override val uid: String,
    val models: Array[Model[_]],
    val numClasses: Int
) extends Model[OneVsRestModel]
    with OneVsRestParams
    with MLWritable {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Each binary model writes its raw score to its own column; the argmax over those columns is
    // the predicted class. The temporary columns are dropped at the end.
    var current = dataset.toDF()
    val rawCols = models.indices.map(i => s"__ovr_raw_$i")
    models.zipWithIndex.foreach { case (m, i) =>
      val params = m.asInstanceOf[Params]
      // Each binary model gets its own output columns, so the chained transforms do not clash.
      val outputs = Seq(
        "rawPredictionCol" -> rawCols(i),
        "predictionCol" -> s"__ovr_pred_$i",
        "probabilityCol" -> s"__ovr_prob_$i"
      ).filter { case (name, _) => params.hasParam(name) }
      val copied = m.copy(ParamMap(outputs.map { case (name, col) =>
        params.getParam(name).asInstanceOf[Param[Any]] -> col
      }*))
      current = copied.transform(current)
    }
    val scores = rawCols.map(c => vector_to_array(F.col(c)).getItem(1))
    val arr = F.array(scores*)
    val predicted = (F.array_position(arr, F.array_max(arr)) - F.lit(1)).cast("double")
    val withPrediction = current.withColumn($(predictionCol), predicted)
    withPrediction.drop(
      (rawCols ++ models.indices.flatMap(i => Seq(s"__ovr_pred_$i", s"__ovr_prob_$i")))*
    )
  }

  override def copy(extra: ParamMap): OneVsRestModel =
    copyValues(new OneVsRestModel(uid, models.map(_.copy(extra)), numClasses), extra)

  override def release(): Unit = models.foreach(_.release())

  override def write: MLWriter = new OneVsRestModelWriter(this)
}

object OneVsRestModel extends MLReadable[OneVsRestModel] {

  override def load(path: String): OneVsRestModel = {
    val meta = DefaultParams.load(path)
    val numClasses = meta.raw.get("numClasses") match {
      case Some(Json.Num(t)) => t.toDouble.toInt
      case _ => throw new IllegalArgumentException(s"No numClasses in $path")
    }
    val models = (0 until numClasses).map { i =>
      TuningUtils.loadStage(s"$path/model_$i").asInstanceOf[Model[_]]
    }
    val model = new OneVsRestModel(meta.uid, models.toArray, numClasses)
    DefaultParams.getAndSetParams(model, meta, Set("classifier", "parallelism"))
    model.set(
      model.classifier,
      TuningUtils.loadStage(s"$path/classifier").asInstanceOf[Estimator[_]]
    )
    model
  }
}

/**
 * Writes a [[OneVsRestModel]]: metadata with `numClasses`, the base classifier, and one model per
 * class.
 */
private[classification] class OneVsRestModelWriter(instance: OneVsRestModel)
    extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit = {
    DefaultParams.save(
      instance.uid,
      instance.getClass.getName,
      org.apache.spark.ml.param.ParamMap.empty,
      path,
      isOverwrite,
      DefaultParams.paramMapJson(instance.extractParamMap(), Set("classifier", "parallelism")),
      Seq("numClasses" -> Json.Num(instance.numClasses.toString))
    )
    TuningUtils.saveStage(
      instance.getClassifier.asInstanceOf[Params],
      s"$path/classifier",
      isOverwrite
    )
    instance.models.zipWithIndex.foreach { case (m, i) =>
      TuningUtils.saveStage(m.asInstanceOf[Params], s"$path/model_$i", isOverwrite)
    }
  }
}
