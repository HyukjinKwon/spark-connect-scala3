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
package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a [[DataFrame]] from external storage systems (file systems, key-value
 * stores, tables, ...). Use [[SparkSession.read]] to access this.
 *
 * {{{
 *   spark.read.format("parquet").load("/path/to/table")
 *   spark.read.option("header", "true").csv("/path/to/file.csv")
 *   spark.read.table("my_table")
 * }}}
 */
class DataFrameReader private[sql] (sparkSession: SparkSession) {

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

  /** Loads input in as a [[DataFrame]], for data sources that don't require a path. */
  def load(): DataFrame = loadPaths(Nil)

  /** Loads input in as a [[DataFrame]], for data sources that require a single path. */
  def load(path: String): DataFrame = loadPaths(Seq(path))

  /** Loads input in as a [[DataFrame]], for data sources that support multiple paths. */
  def load(paths: String*): DataFrame = loadPaths(paths.toSeq)

  private def loadPaths(paths: Seq[String]): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = userSpecifiedSchema,
      options = extraOptions.toMap,
      paths = paths
    )
    sparkSession.newDataFrame(
      proto.Relation().withRead(proto.Read(readType = proto.Read.ReadType.DataSource(dataSource)))
    )
  }

  /** Returns the specified table/view as a [[DataFrame]]. */
  def table(tableName: String): DataFrame = {
    val namedTable =
      proto.Read.NamedTable(unparsedIdentifier = tableName, options = extraOptions.toMap)
    sparkSession.newDataFrame(
      proto.Relation().withRead(proto.Read(readType = proto.Read.ReadType.NamedTable(namedTable)))
    )
  }

  // --- format shortcuts ---

  def json(paths: String*): DataFrame = format("json").load(paths: _*)
  def csv(paths: String*): DataFrame = format("csv").load(paths: _*)
  def parquet(paths: String*): DataFrame = format("parquet").load(paths: _*)
  def orc(paths: String*): DataFrame = format("orc").load(paths: _*)
  def text(paths: String*): DataFrame = format("text").load(paths: _*)

  /** Construct a [[DataFrame]] representing a JDBC table. */
  def jdbc(url: String, table: String, properties: java.util.Properties): DataFrame = {
    import scala.jdk.CollectionConverters._
    properties.asScala.foreach { case (k, v) => extraOptions(k) = v }
    format("jdbc").option("url", url).option("dbtable", table).load()
  }
}
