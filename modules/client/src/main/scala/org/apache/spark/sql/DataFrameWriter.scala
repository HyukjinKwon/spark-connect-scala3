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

/**
 * Saves the contents of a [[DataFrame]] to external storage systems (e.g. file systems, key-value
 * stores, tables). Use `Dataset.write` to access this.
 *
 * Mirrors the public surface of `org.apache.spark.sql.DataFrameWriter` over the Spark Connect
 * protocol.
 *
 * @example
 *   {{{
 *   df.write.format("parquet").mode("overwrite").save("out.parquet")
 *   df.write.mode("append").saveAsTable("my_table")
 *   }}}
 */
class DataFrameWriter private[sql] (ds: Dataset[?]) {

  private var source: Option[String] = None
  private var saveMode: proto.WriteOperation.SaveMode =
    proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED
  private val extraOptions = mutable.Map.empty[String, String]
  private var partitioningColumns: Seq[String] = Seq.empty
  private var sortColumnNames: Seq[String] = Seq.empty
  private var numBuckets: Option[Int] = None
  private var bucketColumnNames: Seq[String] = Seq.empty
  private var clusteringColumns: Seq[String] = Seq.empty
  private var withSchemaEvolutionEnabled: Boolean = false

  /**
   * Specifies the behavior when data or table already exists. One of `"append"`, `"overwrite"`,
   * `"ignore"`, `"error"` / `"errorifexists"` (the default), or `"default"`.
   *
   * @return
   *   this writer, for chaining.
   */
  /** Specifies the behavior when data or table already exists, using a [[SaveMode]]. */
  def mode(saveMode: SaveMode): DataFrameWriter = mode(saveMode match {
    case SaveMode.Append => "append"
    case SaveMode.Overwrite => "overwrite"
    case SaveMode.ErrorIfExists => "errorifexists"
    case SaveMode.Ignore => "ignore"
  })

  def mode(saveMode: String): DataFrameWriter = {
    this.saveMode = saveMode.toLowerCase match {
      case "append" => proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
      case "overwrite" => proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case "ignore" => proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case "error" | "errorifexists" => proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
      case "default" => proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED
      case other =>
        throw new IllegalArgumentException(
          s"Unknown save mode: $other. Accepted save modes are 'overwrite', 'append', " +
            "'ignore', 'error', 'errorifexists', 'default'."
        )
    }
    this
  }

  /**
   * Specifies the output data source format (e.g. `"csv"`, `"json"`, `"parquet"`, `"orc"`).
   *
   * @return
   *   this writer, for chaining.
   */
  def format(source: String): DataFrameWriter = {
    this.source = Option(source)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @return
   *   this writer, for chaining.
   */
  def option(key: String, value: String): DataFrameWriter = {
    extraOptions += (key -> value)
    this
  }

  /** Adds a boolean output option. @return this writer, for chaining. */
  def option(key: String, value: Boolean): DataFrameWriter = option(key, value.toString)

  /** Adds a long output option. @return this writer, for chaining. */
  def option(key: String, value: Long): DataFrameWriter = option(key, value.toString)

  /** Adds a double output option. @return this writer, for chaining. */
  def option(key: String, value: Double): DataFrameWriter = option(key, value.toString)

  /**
   * Adds multiple output options.
   *
   * @return
   *   this writer, for chaining.
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter = {
    extraOptions ++= options
    this
  }

  /** (Java-specific) Adds multiple output options. @return this writer, for chaining. */
  def options(options: java.util.Map[String, String]): DataFrameWriter =
    this.options(options.asScala)

  /**
   * Partitions the output by the given columns on the file system.
   *
   * @return
   *   this writer, for chaining.
   */
  def partitionBy(colNames: String*): DataFrameWriter = {
    this.partitioningColumns = colNames
    validatePartitioning()
    this
  }

  /**
   * Buckets the output by the given columns into `numBuckets` buckets. If specified, the output is
   * laid out on the file system similar to Hive's bucketing scheme.
   *
   * @return
   *   this writer, for chaining.
   */
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter = {
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = colName +: colNames
    validatePartitioning()
    this
  }

  /**
   * Sorts the output in each bucket by the given columns.
   *
   * @return
   *   this writer, for chaining.
   */
  def sortBy(colName: String, colNames: String*): DataFrameWriter = {
    this.sortColumnNames = colName +: colNames
    this
  }

  /**
   * Clusters the output by the given columns, for table formats that support clustering (e.g.
   * liquid clustering). Cannot be combined with `partitionBy` or `bucketBy`.
   *
   * @return
   *   this writer, for chaining.
   */
  def clusterBy(colName: String, colNames: String*): DataFrameWriter = {
    this.clusteringColumns = colName +: colNames
    validatePartitioning()
    this
  }

  /**
   * Enables automatic schema evolution for this write (Spark 4.2+). The target table must declare
   * the `AUTOMATIC_SCHEMA_EVOLUTION` capability.
   *
   * @return
   *   this writer, for chaining.
   */
  def withSchemaEvolution(): DataFrameWriter = {
    this.withSchemaEvolutionEnabled = true
    this
  }

  private def isBucketed: Boolean = {
    if (sortColumnNames.nonEmpty && numBuckets.isEmpty) {
      throw AnalysisException.withErrorClass("SORT_BY_WITHOUT_BUCKETING", Map.empty)
    }
    numBuckets.isDefined
  }

  private def validatePartitioning(): Unit =
    if (clusteringColumns.nonEmpty) {
      if (partitioningColumns.nonEmpty) {
        throw AnalysisException.withErrorClass(
          "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
          Map.empty
        )
      }
      if (isBucketed) {
        throw AnalysisException.withErrorClass(
          "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
          Map.empty
        )
      }
    }

  /**
   * Saves the [[DataFrame]] as output, for data sources that do not require a path (e.g. external
   * key-value stores).
   */
  def save(): Unit = executeWriteOperation(builder => builder)

  /** Saves the [[DataFrame]] at the given path. */
  def save(path: String): Unit =
    executeWriteOperation(_.copy(saveType = proto.WriteOperation.SaveType.Path(path)))

  /** Saves the [[DataFrame]] as the given managed/registered table. */
  def saveAsTable(tableName: String): Unit = {
    val table = proto.WriteOperation.SaveTable(
      tableName = tableName,
      saveMethod = proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE
    )
    executeWriteOperation(_.copy(saveType = proto.WriteOperation.SaveType.Table(table)))
  }

  /** Inserts the [[DataFrame]] into the given existing table (by position). */
  def insertInto(tableName: String): Unit = {
    val table = proto.WriteOperation.SaveTable(
      tableName = tableName,
      saveMethod = proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
    )
    executeWriteOperation(_.copy(saveType = proto.WriteOperation.SaveType.Table(table)))
  }

  /** Saves the content as CSV at the given path. */
  def csv(path: String): Unit = format("csv").save(path)

  /** Saves the content as JSON at the given path. */
  def json(path: String): Unit = format("json").save(path)

  /** Saves the content as Parquet at the given path. */
  def parquet(path: String): Unit = format("parquet").save(path)

  /** Saves the content as ORC at the given path. */
  def orc(path: String): Unit = format("orc").save(path)

  /** Saves the content as text at the given path. */
  def text(path: String): Unit = format("text").save(path)

  /** Saves the content as XML at the given path. */
  def xml(path: String): Unit = format("xml").save(path)

  /**
   * Saves the content to the external database table `table` over JDBC, using `url` and the
   * connection properties. Partitioning, bucketing, and clustering are not supported here.
   */
  def jdbc(url: String, table: String, connectionProperties: java.util.Properties): Unit = {
    if (partitioningColumns.nonEmpty) {
      throw AnalysisException.withErrorClass("_LEGACY_ERROR_TEMP_1197", Map("operation" -> "jdbc"))
    }
    if (isBucketed) {
      val errorClass =
        if (sortColumnNames.isEmpty) "_LEGACY_ERROR_TEMP_1312" else "_LEGACY_ERROR_TEMP_1313"
      throw AnalysisException.withErrorClass(errorClass, Map("operation" -> "jdbc"))
    }
    if (clusteringColumns.nonEmpty) {
      throw AnalysisException.withErrorClass("CLUSTERING_NOT_SUPPORTED", Map("operation" -> "jdbc"))
    }
    // Connection properties override existing options; the url and table override everything.
    extraOptions ++= connectionProperties.asScala
    extraOptions ++= Seq("url" -> url, "dbtable" -> table)
    format("jdbc").save()
  }

  private def executeWriteOperation(f: proto.WriteOperation => proto.WriteOperation): Unit = {
    val writeOp = buildWriteOperation(f)
    ds.sparkSession.executeCommand(
      proto.Command(commandType = proto.Command.CommandType.WriteOperation(writeOp))
    )
  }

  /** The `WriteOperation` this writer sends (exposed for offline plan tests). */
  private[sql] def buildWriteOperation(
      f: proto.WriteOperation => proto.WriteOperation = identity
  ): proto.WriteOperation = {
    val bucketBy = numBuckets.map { n =>
      proto.WriteOperation.BucketBy(bucketColumnNames = bucketColumnNames, numBuckets = n)
    }
    val base = proto.WriteOperation(
      input = Option(ds.relation),
      source = source,
      mode = saveMode,
      sortColumnNames = sortColumnNames,
      partitioningColumns = partitioningColumns,
      bucketBy = bucketBy,
      options = extraOptions.toMap,
      clusteringColumns = clusteringColumns,
      withSchemaEvolution = withSchemaEvolutionEnabled
    )
    f(base)
  }
}
