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
package org.apache.spark.ml.util

import org.apache.spark.ml.param._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.ml.MlClient

/**
 * Spark's standard ML metadata: a JSON document at `<path>/metadata` with `class`, `timestamp`,
 * `sparkVersion`, `uid`, `paramMap` and `defaultParamMap`, plus extra top-level fields. The layout
 * and encodings match `DefaultParamsWriter` and `DefaultParamsReader` in Spark and PySpark, so the
 * composite models written here load in either, and theirs load here.
 */
private[ml] object DefaultParams {

  /** The parsed metadata of a saved object. */
  final case class Metadata(
      className: String,
      uid: String,
      timestamp: Long,
      sparkVersion: String,
      paramMap: Json.Obj,
      raw: Json.Obj
  )

  /** Encodes a parameter value as JSON, or `None` for values that have no simple encoding. */
  def encodeParam(value: Any): Option[Json.Value] = value match {
    case v: Int => Some(Json.Num(v.toString))
    case v: Long => Some(Json.Num(v.toString))
    case v: Double if !v.isNaN && !v.isInfinite => Some(Json.Num(v.toString))
    case v: Float if !v.isNaN && !v.isInfinite => Some(Json.Num(v.toString))
    case v: Boolean => Some(Json.Bool(v))
    case v: String => Some(Json.Str(v))
    case v: Array[Int] => Some(Json.Arr(v.toSeq.map(x => Json.Num(x.toString))))
    case v: Array[Double] => Some(Json.Arr(v.toSeq.map(x => Json.Num(x.toString))))
    case v: Array[String] => Some(Json.Arr(v.toSeq.map(Json.Str(_))))
    case _ => None
  }

  /** Decodes a JSON value for `param`, using the parameter's type to pick the Scala type. */
  def decodeParam(param: Param[_], json: Json.Value): Any = (param, json) match {
    case (_: IntParam, Json.Num(t)) => t.toDouble.toInt
    case (_: LongParam, Json.Num(t)) => t.toDouble.toLong
    case (_: FloatParam, Json.Num(t)) => t.toFloat
    case (_: DoubleParam, Json.Num(t)) => t.toDouble
    case (_: BooleanParam, Json.Bool(b)) => b
    case (_: IntArrayParam, Json.Arr(vs)) => vs.map(num(_).toInt).toArray
    case (_: DoubleArrayParam, Json.Arr(vs)) => vs.map(num(_)).toArray
    case (_: StringArrayParam, Json.Arr(vs)) => vs.map(str).toArray
    case (_, Json.Str(s)) => s
    case (_, Json.Bool(b)) => b
    case (_, Json.Num(t)) => t.toDouble
    case (_, other) => throw new IllegalArgumentException(s"Cannot decode $other for ${param.name}")
  }

  private def num(v: Json.Value): Double = v match {
    case Json.Num(t) => t.toDouble
    case other => throw new IllegalArgumentException(s"Expected number, got $other")
  }

  private def str(v: Json.Value): String = v match {
    case Json.Str(s) => s
    case other => throw new IllegalArgumentException(s"Expected string, got $other")
  }

  /** The JSON `paramMap` of the explicitly set parameters of `instance`, minus `skip`. */
  def paramMapJson(map: ParamMap, skip: Set[String]): Json.Obj =
    Json.Obj(map.toSeq.filterNot(p => skip.contains(p.param.name)).flatMap { p =>
      encodeParam(p.value).map(p.param.name -> _)
    })

  /**
   * Writes `<path>/metadata` for `instance`. `paramMap` overrides the set parameters when given,
   * and `extra` fields are added at the top level, as Spark does.
   */
  def save(
      uid: String,
      className: String,
      defaults: ParamMap,
      path: String,
      overwrite: Boolean,
      paramMap: Json.Obj,
      extra: Seq[(String, Json.Value)] = Nil
  ): Unit = {
    val spark = MlClient.currentSession()
    val fields = Seq(
      "class" -> Json.Str(className),
      "timestamp" -> Json.Num(System.currentTimeMillis().toString),
      "sparkVersion" -> Json.Str(spark.version),
      "uid" -> Json.Str(uid),
      "paramMap" -> paramMap,
      "defaultParamMap" -> paramMapJson(defaults, Set.empty)
    ) ++ extra
    val json = Json.render(Json.Obj(fields))
    val mode = if (overwrite) "overwrite" else "errorifexists"
    spark.createDataFrame(Seq(Tuple1(json))).coalesce(1).write.mode(mode).text(s"$path/metadata")
  }

  /** Reads and parses `<path>/metadata`. */
  def load(path: String): Metadata = {
    val lines = MlClient.currentSession().read.text(s"$path/metadata").collect().toSeq
    val json = Json.parse(lines.map(_.getString(0)).mkString("\n"))
    val obj = json match {
      case o: Json.Obj => o
      case other => throw new IllegalArgumentException(s"Bad metadata at $path: $other")
    }
    val paramMap = obj.get("paramMap") match {
      case Some(o: Json.Obj) => o
      case _ => Json.Obj(Nil)
    }
    Metadata(
      className = obj.getStr("class"),
      uid = obj.getStr("uid"),
      timestamp = obj.get("timestamp").collect { case Json.Num(t) => t.toLong }.getOrElse(0L),
      sparkVersion = obj.get("sparkVersion").collect { case Json.Str(s) => s }.getOrElse(""),
      paramMap = paramMap,
      raw = obj
    )
  }

  /** Sets the parameters recorded in `metadata` on `instance`, skipping `skip`. */
  def getAndSetParams(instance: Params, metadata: Metadata, skip: Set[String] = Set.empty): Unit =
    metadata.paramMap.fields.foreach { case (name, json) =>
      if (!skip.contains(name) && instance.hasParam(name)) {
        val p = instance.getParam(name)
        instance.setInternalParam(name, decodeParam(p, json))
      }
    }

  /**
   * Maps a class name from metadata to the Scala class. PySpark records its own module names
   * (`pyspark.ml.pipeline.PipelineModel`), which map to the Scala ones.
   */
  def scalaClassName(recorded: String): String =
    if (recorded.startsWith("pyspark.ml.pipeline.")) {
      "org.apache.spark.ml." + recorded.stripPrefix("pyspark.ml.pipeline.")
    } else if (recorded.startsWith("pyspark.ml.")) {
      "org.apache.spark.ml." + recorded.stripPrefix("pyspark.ml.")
    } else {
      recorded
    }

  /** Loads the object saved at `path` by reading its metadata and calling its companion `load`. */
  def loadInstance(path: String): Any = {
    val meta = load(path)
    val className = scalaClassName(meta.className)
    val companion = Class.forName(className + "$").getField("MODULE$").get(null)
    companion.getClass.getMethod("load", classOf[String]).invoke(companion, path)
  }
}
