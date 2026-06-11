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
 * Loads a streaming [[DataFrame]] from a streaming source. Use `SparkSession.readStream` to access
 * this.
 *
 * Mirrors the public surface of `org.apache.spark.sql.streaming.DataStreamReader` over the Spark
 * Connect protocol. A streaming read is expressed as a `Read` relation whose `is_streaming` flag is
 * set, so the resulting [[DataFrame]] is unbounded and is meant to be consumed through a
 * [[DataStreamWriter]].
 *
 * @example
 *   {{{
 *   val df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
 *   }}}
 */
class DataStreamReader private[sql] (spark: SparkSession) {

  private var source: Option[String] = None
  private var userSchema: Option[String] = None
  private val extraOptions = mutable.Map.empty[String, String]

  /**
   * Specifies the input data source format (e.g. `"rate"`, `"kafka"`).
   *
   * @return
   *   this reader, for chaining.
   */
  def format(source: String): DataStreamReader = {
    this.source = Option(source)
    this
  }

  /**
   * Specifies the input schema using a DDL-formatted string (e.g. `"a INT, b STRING"`).
   *
   * @return
   *   this reader, for chaining.
   */
  def schema(schemaString: String): DataStreamReader = {
    this.userSchema = Option(schemaString)
    this
  }

  /**
   * Specifies the input schema using a [[StructType]].
   *
   * @return
   *   this reader, for chaining.
   */
  def schema(schema: StructType): DataStreamReader = {
    this.userSchema = Option(schema.simpleString)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @return
   *   this reader, for chaining.
   */
  def option(key: String, value: String): DataStreamReader = {
    extraOptions += (key -> value)
    this
  }

  /** Adds a boolean input option. @return this reader, for chaining. */
  def option(key: String, value: Boolean): DataStreamReader = option(key, value.toString)

  /** Adds a long input option. @return this reader, for chaining. */
  def option(key: String, value: Long): DataStreamReader = option(key, value.toString)

  /** Adds a double input option. @return this reader, for chaining. */
  def option(key: String, value: Double): DataStreamReader = option(key, value.toString)

  /**
   * Adds multiple input options.
   *
   * @return
   *   this reader, for chaining.
   */
  def options(options: Map[String, String]): DataStreamReader = {
    extraOptions ++= options
    this
  }

  /**
   * Loads input as a streaming [[DataFrame]], for data sources that do not require a path (e.g.
   * `"rate"` or `"kafka"`).
   */
  def load(): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = userSchema,
      options = extraOptions.toMap,
      paths = Seq.empty
    )
    streamRelation(proto.Read.ReadType.DataSource(dataSource))
  }

  /** Loads input from the given path as a streaming [[DataFrame]]. */
  def load(path: String): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = userSchema,
      options = extraOptions.toMap,
      paths = Seq(path)
    )
    streamRelation(proto.Read.ReadType.DataSource(dataSource))
  }

  /** Loads a streaming [[DataFrame]] from a registered table. */
  def table(tableName: String): DataFrame = {
    val namedTable =
      proto.Read.NamedTable(unparsedIdentifier = tableName, options = extraOptions.toMap)
    streamRelation(proto.Read.ReadType.NamedTable(namedTable))
  }

  /** Loads CSV file(s) and returns the result as a streaming [[DataFrame]]. */
  def csv(path: String): DataFrame = format("csv").load(path)

  /** Loads JSON file(s) and returns the result as a streaming [[DataFrame]]. */
  def json(path: String): DataFrame = format("json").load(path)

  /** Loads Parquet file(s) and returns the result as a streaming [[DataFrame]]. */
  def parquet(path: String): DataFrame = format("parquet").load(path)

  /** Loads ORC file(s) and returns the result as a streaming [[DataFrame]]. */
  def orc(path: String): DataFrame = format("orc").load(path)

  /**
   * Loads text file(s) and returns the result as a streaming [[DataFrame]] with a single `value`
   * column.
   */
  def text(path: String): DataFrame = format("text").load(path)

  private def streamRelation(readType: proto.Read.ReadType): DataFrame =
    spark.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(isStreaming = true, readType = readType))
    )
}
