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
package org.apache.spark.sql.streaming

import scala.collection.mutable

import org.apache.spark.connect.proto
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a streaming [[org.apache.spark.sql.Dataset]] from external storage systems
 * (e.g. file systems, key-value stores, etc). Use `SparkSession.readStream` to access this. Mirrors
 * `org.apache.spark.sql.streaming.DataStreamReader`.
 *
 * {{{
 *   spark.readStream.format("rate").option("rowsPerSecond", "5").load()
 * }}}
 */
final class DataStreamReader private[sql] (sparkSession: SparkSession) {

  private var source: Option[String] = None
  private var userSpecifiedSchema: Option[String] = None
  private val extraOptions = mutable.LinkedHashMap.empty[String, String]

  /** Specifies the input data source format. */
  def format(source: String): this.type = { this.source = Some(source); this }

  /** Specifies the input schema. */
  def schema(schema: StructType): this.type = {
    this.userSpecifiedSchema = Some(schema.json)
    this
  }

  /** Specifies the input schema as a DDL-formatted string, e.g. `"a INT, b STRING"`. */
  def schema(schemaString: String): this.type = {
    this.userSpecifiedSchema = Some(schemaString)
    this
  }

  /** Adds an input option. */
  def option(key: String, value: String): this.type = { extraOptions(key) = value; this }
  def option(key: String, value: Boolean): this.type = option(key, value.toString)
  def option(key: String, value: Long): this.type = option(key, value.toString)
  def option(key: String, value: Double): this.type = option(key, value.toString)

  /** Adds input options. */
  def options(options: scala.collection.Map[String, String]): this.type = {
    extraOptions ++= options
    this
  }

  def options(options: java.util.Map[String, String]): this.type = {
    import scala.jdk.CollectionConverters._
    extraOptions ++= options.asScala
    this
  }

  /** Loads input data stream in as a [[DataFrame]], for data streams that don't require a path. */
  def load(): DataFrame = loadInternal(None)

  /** Loads input in as a [[DataFrame]], for data streams that read from some path. */
  def load(path: String): DataFrame = loadInternal(Some(path))

  private def loadInternal(path: Option[String]): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = userSpecifiedSchema,
      options = extraOptions.toMap,
      paths = path.toSeq
    )
    sparkSession.newDataFrame(
      proto
        .Relation()
        .withRead(
          proto.Read(
            isStreaming = true,
            readType = proto.Read.ReadType.DataSource(dataSource)
          )
        )
    )
  }

  /** Define a streaming [[DataFrame]] on a table. */
  def table(tableName: String): DataFrame = {
    val namedTable =
      proto.Read.NamedTable(unparsedIdentifier = tableName, options = extraOptions.toMap)
    sparkSession.newDataFrame(
      proto
        .Relation()
        .withRead(
          proto.Read(
            isStreaming = true,
            readType = proto.Read.ReadType.NamedTable(namedTable)
          )
        )
    )
  }

  // --- format shortcuts ---

  def json(path: String): DataFrame = format("json").load(path)
  def csv(path: String): DataFrame = format("csv").load(path)
  def parquet(path: String): DataFrame = format("parquet").load(path)
  def orc(path: String): DataFrame = format("orc").load(path)
  def text(path: String): DataFrame = format("text").load(path)
}
