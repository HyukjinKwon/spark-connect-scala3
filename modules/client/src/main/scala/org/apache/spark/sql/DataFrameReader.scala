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
import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
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
  // A DDL string (as given) or a StructType; sent to the server as DDL.
  private var userSchema: Option[Either[String, StructType]] = None
  private val extraOptions = mutable.LinkedHashMap.empty[String, String]

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
    this.userSchema = Option(schemaString).map(Left(_))
    this
  }

  /**
   * Specifies the input schema using a [[StructType]].
   *
   * @return
   *   this reader, for chaining.
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSchema = Option(schema).map(Right(_))
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
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    extraOptions ++= options
    this
  }

  /** (Java-specific) Adds multiple input options. @return this reader, for chaining. */
  def options(options: java.util.Map[String, String]): DataFrameReader =
    this.options(options.asScala)

  private def schemaDDL: Option[String] = userSchema.map {
    case Left(ddl) => ddl
    case Right(struct) => struct.toDDL
  }

  private def assertNoSpecifiedSchema(operation: String): Unit =
    if (userSchema.isDefined) {
      throw AnalysisException.withErrorClass(
        "_LEGACY_ERROR_TEMP_1189",
        Map("operation" -> operation)
      )
    }

  /**
   * Loads input as a [[DataFrame]], for data sources that do not require a path (e.g. external
   * key-value stores).
   */
  def load(): DataFrame = load(Seq.empty*)

  /** Loads input from the given path as a [[DataFrame]]. */
  def load(path: String): DataFrame = load(Seq(path)*)

  /**
   * Loads input from the given paths as a [[DataFrame]], for data sources that support reading
   * multiple paths.
   */
  def load(paths: String*): DataFrame = {
    val dataSource = proto.Read.DataSource(
      format = source,
      schema = schemaDDL,
      options = extraOptions.toMap,
      paths = paths
    )
    sparkSession.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(readType = proto.Read.ReadType.DataSource(dataSource)))
    )
  }

  /** Returns the specified table/view as a [[DataFrame]]. */
  def table(tableName: String): DataFrame = {
    assertNoSpecifiedSchema("table")
    val namedTable =
      proto.Read.NamedTable(unparsedIdentifier = tableName, options = extraOptions.toMap)
    sparkSession.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(readType = proto.Read.ReadType.NamedTable(namedTable)))
    )
  }

  /**
   * Returns the row-level changes (Change Data Capture) of the table as a [[DataFrame]] (Spark
   * 4.2+). Only Data Source V2 tables whose catalog supports change logs implement this. Use
   * options such as `startingVersion`, `endingVersion`, `startingTimestamp`, `endingTimestamp`,
   * `deduplicationMode`, and `computeUpdates` to choose the range and post-processing:
   * {{{
   *   spark.read.option("startingVersion", "10").changes("my_table")
   * }}}
   */
  def changes(tableName: String): DataFrame = {
    require(tableName != null, "The table name can't be null")
    assertNoSpecifiedSchema("changes")
    sparkSession.newDataFrame(
      proto.Relation.RelType.RelationChanges(
        proto.RelationChanges(unparsedIdentifier = tableName, options = extraOptions.toMap)
      )
    )
  }

  /** Loads CSV file(s) and returns the result as a [[DataFrame]]. */
  def csv(path: String): DataFrame = csv(Seq(path)*)

  /** Loads CSV file(s) and returns the result as a [[DataFrame]]. */
  def csv(paths: String*): DataFrame = format("csv").load(paths*)

  /** Parses each row of a `Dataset[String]` as a CSV record, returning a [[DataFrame]]. */
  def csv(csvDataset: Dataset[String]): DataFrame =
    parse(csvDataset, proto.Parse.ParseFormat.PARSE_FORMAT_CSV)

  /** Loads JSON file(s) and returns the result as a [[DataFrame]]. */
  def json(path: String): DataFrame = json(Seq(path)*)

  /** Loads JSON file(s) and returns the result as a [[DataFrame]]. */
  def json(paths: String*): DataFrame = format("json").load(paths*)

  /** Parses each row of a `Dataset[String]` as a JSON object, returning a [[DataFrame]]. */
  def json(jsonDataset: Dataset[String]): DataFrame =
    parse(jsonDataset, proto.Parse.ParseFormat.PARSE_FORMAT_JSON)

  /** Loads XML file(s) and returns the result as a [[DataFrame]]. */
  def xml(path: String): DataFrame = xml(Seq(path)*)

  /** Loads XML file(s) and returns the result as a [[DataFrame]]. */
  def xml(paths: String*): DataFrame = format("xml").load(paths*)

  /** Parses each row of a `Dataset[String]` as an XML record (Spark 4.2+ servers). */
  def xml(xmlDataset: Dataset[String]): DataFrame =
    parse(xmlDataset, proto.Parse.ParseFormat.PARSE_FORMAT_XML)

  private def parse(ds: Dataset[String], format: proto.Parse.ParseFormat): DataFrame = {
    val schema = userSchema.map {
      case Right(struct) => DataTypeProtoConverter.toConnectProtoType(struct)
      // `Parse` only accepts a resolved schema (the server rejects UNPARSED types), so a DDL
      // string is resolved through the DDLParse analysis RPC, like upstream's `fromDDL`.
      case Left(ddl) => sparkSession.client.ddlParse(ddl)
    }
    sparkSession.newDataFrame(
      proto.Relation.RelType.Parse(
        proto.Parse(
          input = Some(ds.relation),
          format = format,
          schema = schema,
          options = extraOptions.toMap
        )
      )
    )
  }

  /** Loads Parquet file(s) and returns the result as a [[DataFrame]]. */
  def parquet(path: String): DataFrame = parquet(Seq(path)*)

  /** Loads Parquet file(s) and returns the result as a [[DataFrame]]. */
  def parquet(paths: String*): DataFrame = format("parquet").load(paths*)

  /** Loads ORC file(s) and returns the result as a [[DataFrame]]. */
  def orc(path: String): DataFrame = orc(Seq(path)*)

  /** Loads ORC file(s) and returns the result as a [[DataFrame]]. */
  def orc(paths: String*): DataFrame = format("orc").load(paths*)

  /** Loads text file(s) and returns the result as a [[DataFrame]] with a single `value` column. */
  def text(path: String): DataFrame = text(Seq(path)*)

  /** Loads text file(s) and returns the result as a [[DataFrame]] with a single `value` column. */
  def text(paths: String*): DataFrame = format("text").load(paths*)

  /** Loads text file(s) and returns a `Dataset[String]` of their lines. */
  def textFile(path: String): Dataset[String] = textFile(Seq(path)*)

  /** Loads text file(s) and returns a `Dataset[String]` of their lines. */
  def textFile(paths: String*): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(paths*).select("value").as[String]
  }

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
    assertNoSpecifiedSchema("jdbc")
    // Properties override existing options; the explicit url and table override everything.
    format("jdbc").options(properties).options(Map("url" -> url, "dbtable" -> table)).load()
  }

  /** Constructs a [[DataFrame]] over a JDBC table, with connection `properties`. */
  def jdbc(url: String, table: String, properties: java.util.Properties): DataFrame =
    jdbc(url, table, properties.asScala.toMap)

  /**
   * Constructs a [[DataFrame]] over a JDBC table, partitioned on `columnName` into `numPartitions`
   * ranges between `lowerBound` and `upperBound`.
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: java.util.Properties
  ): DataFrame = {
    options(
      Map(
        "partitionColumn" -> columnName,
        "lowerBound" -> lowerBound.toString,
        "upperBound" -> upperBound.toString,
        "numPartitions" -> numPartitions.toString
      )
    )
    jdbc(url, table, connectionProperties)
  }

  /**
   * Constructs a [[DataFrame]] over a JDBC table with one partition per `WHERE`-clause predicate in
   * `predicates`.
   */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: java.util.Properties
  ): DataFrame = {
    val params = extraOptions.toMap ++ Map("url" -> url, "dbtable" -> table) ++
      connectionProperties.asScala
    val dataSource = proto.Read.DataSource(
      format = Some("jdbc"),
      options = params,
      predicates = predicates.toSeq
    )
    sparkSession.newDataFrame(
      proto.Relation.RelType.Read(proto.Read(readType = proto.Read.ReadType.DataSource(dataSource)))
    )
  }
}
