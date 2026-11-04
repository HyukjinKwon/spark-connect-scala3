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
 * Interface used to write a [[Dataset]] to external storage using the v2 (catalog) API. Use
 * [[Dataset.writeTo]] to access this.
 *
 * {{{
 *   df.writeTo("catalog.db.table").using("parquet").create()
 *   df.writeTo("catalog.db.table").append()
 * }}}
 */
final class DataFrameWriterV2 private[sql] (table: String, ds: Dataset[?]) {

  private var provider: Option[String] = None
  private val options = mutable.LinkedHashMap.empty[String, String]
  private val properties = mutable.LinkedHashMap.empty[String, String]
  private var partitioning: Seq[proto.Expression] = Nil
  private var clustering: Seq[String] = Nil
  private var withSchemaEvolutionEnabled: Boolean = false

  /** Specifies a provider for the underlying output data source. */
  def using(provider: String): this.type = { this.provider = Some(provider); this }

  def option(key: String, value: String): this.type = { options(key) = value; this }
  def option(key: String, value: Boolean): this.type = option(key, value.toString)
  def option(key: String, value: Long): this.type = option(key, value.toString)
  def option(key: String, value: Double): this.type = option(key, value.toString)
  def options(opts: scala.collection.Map[String, String]): this.type = { options ++= opts; this }

  /** (Java-specific) Adds write options from a Java map. */
  def options(opts: java.util.Map[String, String]): this.type = options(opts.asScala)

  /**
   * Enables automatic schema evolution for this write (Spark 4.2+). The target table must declare
   * the `AUTOMATIC_SCHEMA_EVOLUTION` capability.
   */
  def withSchemaEvolution(): this.type = { withSchemaEvolutionEnabled = true; this }

  def tableProperty(property: String, value: String): this.type = {
    properties(property) = value; this
  }

  /** Partitions the output by the given columns. */
  def partitionedBy(column: Column, columns: Column*): this.type = {
    partitioning = (column +: columns.toSeq).map(_.expr)
    this
  }

  /** Clusters the output by the given columns. */
  def clusterBy(colName: String, colNames: String*): this.type = {
    clustering = colName +: colNames.toSeq
    this
  }

  /** Create a new table from the contents of the DataFrame. */
  def create(): Unit = run(proto.WriteOperationV2.Mode.MODE_CREATE)

  /** Replace an existing table with the contents of the DataFrame. */
  def replace(): Unit = run(proto.WriteOperationV2.Mode.MODE_REPLACE)

  /** Create a new table, or replace an existing one, with the contents of the DataFrame. */
  def createOrReplace(): Unit = run(proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)

  /** Append the contents of the DataFrame to the output table. */
  def append(): Unit = run(proto.WriteOperationV2.Mode.MODE_APPEND)

  /** Overwrite rows matching the given filter. */
  def overwrite(condition: Column): Unit =
    run(proto.WriteOperationV2.Mode.MODE_OVERWRITE, Some(condition.expr))

  /** Overwrite all partitions touched by the contents of the DataFrame. */
  def overwritePartitions(): Unit =
    run(proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)

  /**
   * Builds the `WriteOperationV2` command for the given mode. Extracted from [[run]] so the command
   * wiring can be unit-tested without a server.
   */
  private[sql] def buildWriteOperation(
      mode: proto.WriteOperationV2.Mode,
      overwriteCondition: Option[proto.Expression] = None
  ): proto.WriteOperationV2 =
    proto.WriteOperationV2(
      input = Some(ds.relation),
      tableName = table,
      provider = provider,
      partitioningColumns = partitioning,
      options = options.toMap,
      tableProperties = properties.toMap,
      clusteringColumns = clustering,
      mode = mode,
      overwriteCondition = overwriteCondition,
      withSchemaEvolution = withSchemaEvolutionEnabled
    )

  private def run(
      mode: proto.WriteOperationV2.Mode,
      overwriteCondition: Option[proto.Expression] = None
  ): Unit =
    ds.sparkSession.executeCommand(
      proto.Command(commandType =
        proto.Command.CommandType.WriteOperationV2(buildWriteOperation(mode, overwriteCondition))
      )
    )
}
