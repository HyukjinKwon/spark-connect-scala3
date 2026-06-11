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
 * Loads data from external storage systems (e.g. file systems, key-value stores, JDBC) into a
 * [[DataFrame]]. Use `SparkSession.read` to access this.
 *
 * Mirrors the public surface of `org.apache.spark.sql.DataFrameReader` over the Spark Connect
 * protocol.
 *
 * @example
 *   {{{
 *   spark.read.format("csv").option("header", true).load("data.csv")
 *   spark.read.json("events.json")
 *   spark.read.table("my_table")
 *   }}}
 */
class DataFrameReader private[sql] (sparkSession: SparkSession) {

  private var source: Option[String] = None
  private var userSchema: Option[String] = None
  private val extraOptions = mutable.Map.empty[String, String]

  /**
   * Specifies the input data source format (e.g. `"csv"`, `"json"`, `"parquet"`, `"orc"`).
   *
   * @return
   *   this reader, for chaining.
   */
  def format(source: String): DataFrameReader = {
    this.source = Option(source)
    this
  }

  /**
   * Specifies the input schema using a DDL-formatted string (e.g. `"a INT, b STRING"`).
   *
   * @return
   *   this reader, for chaining.
   */
  def schema(schemaString: String): DataFrameReader = {
    this.userSchema = Option(schemaString)
    this
  }

  /**
   * Specifies the input schema using a [[StructType]].
   *
   * @return
   *   this reader, for chaining.
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSchema = Option(schema.simpleString)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @return
   *   this reader, for chaining.
   */
  def option(key: String, value: String): DataFrameReader = {
    extraOptions += (key -> value)
    this
  }

  /** Adds a boolean input option. @return this reader, for chaining. */
  def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)

  /** Adds a long input option. @return this reader, for chaining. */
  def option(key: String, value: Long): DataFrameReader = option(key, value.toString)

  /** Adds a double input option. @return this reader, for chaining. */
  def option(key: String, value: Double): DataFrameReader = option(key, value.toString)

  /**
   * Adds multiple input options.
   *
   * @return
   *   this reader, for chaining.
   */
  def options(options: Map[String, String]): DataFrameReader = {
    extraOptions ++= options
    this
  }

  /**
   * Loads input as a [[DataFrame]], for data sources that do not require a path (e.g. external
   * key-value stores).
   */
  def load(): DataFrame = load(Seq.empty: _*)

  /** Loads input from the given path as a [[DataFrame]]. */
  def load(path: String): DataFrame = load(Seq(path): _*)

  /**
   * Loads input from the given paths as a [[DataFrame]], for data sources that support reading
   * multiple paths.
   */
  def load(paths: String*): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = userSchema,
      options = extraOptions.toMap,
      paths = paths
    )
    sparkSession.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(readType = proto.Read.ReadType.DataSource(dataSource)))
    )
  }

  /** Returns the specified table/view as a [[DataFrame]]. */
  def table(tableName: String): DataFrame = {
    val namedTable =
      proto.Read.NamedTable(unparsedIdentifier = tableName, options = extraOptions.toMap)
    sparkSession.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(readType = proto.Read.ReadType.NamedTable(namedTable)))
    )
  }

  /** Loads CSV file(s) and returns the result as a [[DataFrame]]. */
  def csv(path: String): DataFrame = csv(Seq(path): _*)

  /** Loads CSV file(s) and returns the result as a [[DataFrame]]. */
  def csv(paths: String*): DataFrame = format("csv").load(paths: _*)

  /** Loads JSON file(s) and returns the result as a [[DataFrame]]. */
  def json(path: String): DataFrame = json(Seq(path): _*)

  /** Loads JSON file(s) and returns the result as a [[DataFrame]]. */
  def json(paths: String*): DataFrame = format("json").load(paths: _*)

  /** Loads Parquet file(s) and returns the result as a [[DataFrame]]. */
  def parquet(paths: String*): DataFrame = format("parquet").load(paths: _*)

  /** Loads ORC file(s) and returns the result as a [[DataFrame]]. */
  def orc(paths: String*): DataFrame = format("orc").load(paths: _*)

  /** Loads text file(s) and returns the result as a [[DataFrame]] with a single `value` column. */
  def text(paths: String*): DataFrame = format("text").load(paths: _*)

  /**
   * Loads text file(s), returning each line as a row in a single-column (`value`) [[DataFrame]].
   *
   * Apache Spark's `textFile` returns a `Dataset[String]`; this client is untyped (closures and
   * custom encoders are not transported over Spark Connect), so the equivalent single-string-column
   * [[DataFrame]] is returned instead.
   */
  def textFile(paths: String*): DataFrame = text(paths: _*)

  /** Loads XML file(s) and returns the result as a [[DataFrame]]. */
  def xml(paths: String*): DataFrame = format("xml").load(paths: _*)

  /**
   * Constructs a [[DataFrame]] representing the database table accessible via JDBC.
   *
   * @param url
   *   the JDBC URL of the form `jdbc:subprotocol:subname`.
   * @param table
   *   the name of the table in the external database (or a subquery).
   * @param properties
   *   connection properties (e.g. `"user"`, `"password"`); these are merged into the read options.
   */
  def jdbc(url: String, table: String, properties: Map[String, String]): DataFrame = {
    val jdbcOptions = Map("url" -> url, "dbtable" -> table) ++ properties
    format("jdbc").options(jdbcOptions).load()
  }
}
