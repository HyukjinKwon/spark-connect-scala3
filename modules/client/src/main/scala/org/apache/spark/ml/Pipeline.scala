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
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter}
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
    val meta = PipelineIO.readMetadata(path)
    val pipeline = new Pipeline(meta.uid)
    pipeline.setStages(
      meta.stages.map(s => PipelineIO.loadStage(s.className, s.path(path))).toArray
    )
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
    val meta = PipelineIO.readMetadata(path)
    new PipelineModel(
      meta.uid,
      meta.stages
        .map(s => PipelineIO.loadStage(s.className, s.path(path)).asInstanceOf[Transformer])
        .toArray
    )
  }
}

/** Writes a [[Pipeline]] as metadata plus one directory per stage. */
private[ml] class PipelineWriter(instance: Pipeline) extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit =
    PipelineIO.save(path, instance.uid, instance.getClass.getName, instance.getStages.toSeq, this)
}

/** Writes a [[PipelineModel]] as metadata plus one directory per stage. */
private[ml] class PipelineModelWriter(instance: PipelineModel) extends MLWriter(instance) {
  override protected def saveImpl(path: String): Unit =
    PipelineIO.save(path, instance.uid, instance.getClass.getName, instance.stages.toSeq, this)
}

/**
 * Persistence for pipelines. A saved pipeline is a metadata file (`metadata`, one `key<TAB>value`
 * line per entry) and a `stages/<index>_<uid>` directory for each stage, which is written with the
 * stage's own writer.
 */
private[ml] object PipelineIO {

  final case class StageRef(index: Int, className: String, uid: String) {
    def path(root: String): String = s"$root/stages/${index}_$uid"
  }

  final case class Metadata(className: String, uid: String, stages: Seq[StageRef])

  private def metadataPath(root: String): String = s"$root/metadata"

  /** Saves the stages and metadata of a pipeline-like object to `root`. */
  def save(
      root: String,
      uid: String,
      className: String,
      stages: Seq[PipelineStage],
      writer: MLWriter
  ): Unit = {
    val refs = stages.zipWithIndex.map { case (stage, index) =>
      stage match {
        case w: MLWritable =>
          val stageWriter = if (writer.isOverwrite) w.write.overwrite() else w.write
          stageWriter.save(s"$root/stages/${index}_${stage.uid}")
        case other =>
          throw new IllegalArgumentException(
            s"Pipeline stage ${other.getClass.getName} is not writable."
          )
      }
      StageRef(index, stage.getClass.getName, stage.uid)
    }
    val lines = Seq(s"class\t$className", s"uid\t$uid") ++
      refs.map(r => s"stage\t${r.index}\t${r.className}\t${r.uid}")
    writeLines(root, lines, writer.isOverwrite)
  }

  /** Reads the metadata of a saved pipeline-like object at `root`. */
  def readMetadata(root: String): Metadata = {
    val entries = readLines(root)
    val className = entries
      .collectFirst { case Seq("class", c) => c }
      .getOrElse(throw new IllegalArgumentException(s"No class in pipeline metadata at $root"))
    val uid = entries
      .collectFirst { case Seq("uid", u) => u }
      .getOrElse(throw new IllegalArgumentException(s"No uid in pipeline metadata at $root"))
    val stages = entries
      .collect { case Seq("stage", i, c, u) => StageRef(i.toInt, c, u) }
      .sortBy(_.index)
    Metadata(className, uid, stages)
  }

  /** Writes `lines` to the metadata file under `root`, replacing it when `overwrite` is set. */
  def writeLines(root: String, lines: Seq[String], overwrite: Boolean): Unit = {
    val text = SparkSession.active.createDataFrame(lines.map(Tuple1(_)))
    val mode = if (overwrite) "overwrite" else "errorifexists"
    text.coalesce(1).write.mode(mode).text(metadataPath(root))
  }

  /** Reads the lines of the metadata file under `root`. */
  def readLines(root: String): Seq[Seq[String]] =
    SparkSession.active.read
      .text(metadataPath(root))
      .collect()
      .toSeq
      .map(_.getString(0).split("\t", -1).toSeq)

  /** Loads a stage of class `className` by calling its companion's `load(path)`. */
  def loadStage(className: String, path: String): PipelineStage = {
    val companion = Class.forName(className + "$").getField("MODULE$").get(null)
    companion.getClass
      .getMethod("load", classOf[String])
      .invoke(companion, path)
      .asInstanceOf[PipelineStage]
  }
}
