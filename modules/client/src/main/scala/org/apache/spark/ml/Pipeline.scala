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
package org.apache.spark.ml

import scala.collection.mutable

import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{
  DefaultParams,
  Identifiable,
  Json,
  MLReadable,
  MLWritable,
  MLWriter
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * A pipeline of stages. Fitting runs each estimator on the output of the stages before it, on the
 * server. Mirrors `org.apache.spark.ml.Pipeline`.
 */
class Pipeline(override val uid: String) extends Estimator[PipelineModel] with MLWritable {

  def this() = this(Identifiable.randomUID("pipeline"))

  /** Stages of the pipeline, in order. */
  final val stages: Param[Array[PipelineStage]] =
    new Param[Array[PipelineStage]](this, "stages", "stages of the pipeline")

  /** Sets the stages of the pipeline. */
  def setStages(value: Array[_ <: PipelineStage]): this.type =
    set(stages, value.asInstanceOf[Array[PipelineStage]])

  /** The stages of the pipeline. */
  def getStages: Array[PipelineStage] = $(stages)

  override def fit(dataset: Dataset[_]): PipelineModel = {
    val theStages = $(stages)
    // The stages after the last estimator are only transformers, so they are not fit.
    val lastEstimator = theStages.lastIndexWhere(_.isInstanceOf[Estimator[_]])
    var curDataset: Dataset[_] = dataset
    val transformers = mutable.ArrayBuffer.empty[Transformer]
    theStages.zipWithIndex.foreach { case (stage, index) =>
      if (index <= lastEstimator) {
        val transformer: Transformer = stage match {
          case estimator: Estimator[_] => estimator.fit(curDataset).asInstanceOf[Transformer]
          case t: Transformer => t
          case other =>
            throw new IllegalArgumentException(s"Unsupported pipeline stage: $other")
        }
        if (index < lastEstimator) {
          curDataset = transformer.transform(curDataset)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }
    new PipelineModel(uid, transformers.toArray)
  }

  override def copy(extra: ParamMap): Pipeline = {
    val copied = copyValues(new Pipeline(uid), extra)
    copied.setStages($(stages).map(_.copy(extra)))
  }

  override def write: MLWriter = new PipelineWriter(this)
}

object Pipeline extends MLReadable[Pipeline] {

  override def load(path: String): Pipeline = {
    val meta = DefaultParams.load(path)
    val pipeline = new Pipeline(meta.uid)
    pipeline.setStages(PipelineIO.loadStages(path, meta).toArray)
    DefaultParams.getAndSetParams(pipeline, meta, Set("stages", "stageUids"))
    pipeline
  }
}

/**
 * A fitted pipeline: the transformers of each stage, in order. Mirrors
 * `org.apache.spark.ml.PipelineModel`.
 */
class PipelineModel(override val uid: String, val stages: Array[Transformer])
    extends Model[PipelineModel]
    with MLWritable {

  override def transform(dataset: Dataset[_]): DataFrame =
    stages.foldLeft(dataset.toDF())((cur, stage) => stage.transform(cur))

  override def copy(extra: ParamMap): PipelineModel =
    new PipelineModel(uid, stages.map(_.copy(extra).asInstanceOf[Transformer]))

  /** Releases the server-side state of every model stage. */
  override def release(): Unit = stages.foreach {
    case m: Model[_] => m.release()
    case _ =>
  }

  override def write: MLWriter = new PipelineModelWriter(this)
}

object PipelineModel extends MLReadable[PipelineModel] {

  override def load(path: String): PipelineModel = {
    val meta = DefaultParams.load(path)
    val stages = PipelineIO.loadStages(path, meta).map(_.asInstanceOf[Transformer])
    new PipelineModel(meta.uid, stages.toArray)
  }
}

/** Writes a [[Pipeline]] in Spark's format: metadata, then one directory per stage. */
private[ml] class PipelineWriter(instance: Pipeline) extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit =
    PipelineIO.save(path, instance.uid, instance.getClass.getName, instance.getStages.toSeq, this)
}

/** Writes a [[PipelineModel]] in Spark's format: metadata, then one directory per stage. */
private[ml] class PipelineModelWriter(instance: PipelineModel) extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit =
    PipelineIO.save(path, instance.uid, instance.getClass.getName, instance.stages.toSeq, this)
}

/**
 * Persistence for pipelines, in Spark's `PipelineSharedReadWrite` layout. The metadata's `paramMap`
 * holds `stageUids`, and each stage is saved under `stages/<index>_<uid>`, with the index
 * zero-padded to the width of the stage count.
 */
private[ml] object PipelineIO {

  /** The directory of stage `index` of `numStages` under `root`, as Spark names it. */
  def stagePath(root: String, uid: String, index: Int, numStages: Int): String = {
    val digits = index.toString
    val width = numStages.toString.length
    s"$root/stages/${"0" * (width - digits.length) + digits}_$uid"
  }

  /** Saves the stages and metadata of a pipeline-like object to `root`. */
  def save(
      root: String,
      uid: String,
      className: String,
      stages: Seq[PipelineStage],
      writer: MLWriter
  ): Unit = {
    stages.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case w: MLWritable =>
          val stageWriter = if (writer.isOverwrite) w.write.overwrite() else w.write
          stageWriter.save(stagePath(root, stage.uid, index, stages.length))
        case other =>
          throw new IllegalArgumentException(
            s"Pipeline stage ${other.getClass.getName} is not writable."
          )
      }
    }
    val stageUids = Json.Arr(stages.map(s => Json.Str(s.uid)))
    DefaultParams.save(
      uid,
      className,
      org.apache.spark.ml.param.ParamMap.empty,
      root,
      writer.isOverwrite,
      Json.Obj(Seq("stageUids" -> stageUids))
    )
  }

  /** Loads the stages recorded in `meta`, each from its own directory. */
  def loadStages(root: String, meta: DefaultParams.Metadata): Seq[PipelineStage] = {
    val uids = meta.paramMap.get("stageUids") match {
      case Some(Json.Arr(vs)) => vs.collect { case Json.Str(s) => s }
      case _ => throw new IllegalArgumentException(s"No stageUids in $root")
    }
    uids.zipWithIndex.map { case (uid, index) =>
      DefaultParams
        .loadInstance(stagePath(root, uid, index, uids.length))
        .asInstanceOf[PipelineStage]
    }
  }
}
