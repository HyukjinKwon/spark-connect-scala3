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

import java.util.Base64

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, PipelineIO}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.connect.MlSerializer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, rand}

/** Helpers shared by the tuning estimators: fold splitting and parameter-map persistence. */
private[tuning] object TuningUtils {

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

  /** Encodes each parameter map as a base64 `MlParams` proto, one line per map. */
  def encodeParamMaps(maps: Seq[ParamMap]): Seq[String] =
    maps.map { m =>
      val bytes = MlSerializer.encodeParamMap(m).toByteArray
      "epm\t" + Base64.getEncoder.encodeToString(bytes)
    }

  /** Decodes parameter maps written by [[encodeParamMaps]], resolving names against `owner`. */
  def decodeParamMaps(owner: Params, lines: Seq[String]): Array[ParamMap] =
    lines.map { line =>
      val encoded = line.split("\t", -1)(1)
      val params = proto.MlParams.parseFrom(Base64.getDecoder.decode(encoded))
      val map = new ParamMap()
      MlSerializer.decodeParams(params).foreach { case (name, value) =>
        val p = owner.getParam(name).asInstanceOf[Param[Any]]
        map.put(p, value)
      }
      map
    }.toArray

  /** Writes a tuning object's sub-stage (`estimator`, `evaluator`, or a model) under `root`. */
  def saveStage(stage: Params, path: String, overwrite: Boolean): Unit = stage match {
    case w: org.apache.spark.ml.util.MLWritable =>
      if (overwrite) w.write.overwrite().save(path) else w.save(path)
    case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not writable.")
  }

  /** Loads a stage by class name, from the directory `path`. */
  def loadStage(className: String, path: String): Params =
    PipelineIO.loadStage(className, path).asInstanceOf[Params]

  /** Sum of (metric) values as a string of space-separated doubles, for metadata. */
  def joinDoubles(values: Seq[Double]): String = values.mkString(" ")

  /** Parses [[joinDoubles]] output. */
  def splitDoubles(text: String): Array[Double] =
    if (text.trim.isEmpty) Array.empty[Double] else text.trim.split(" ").map(_.toDouble)
}
