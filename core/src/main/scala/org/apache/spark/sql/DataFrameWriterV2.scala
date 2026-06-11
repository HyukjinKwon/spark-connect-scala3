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

/**
 * Interface used to write a [[Dataset]] to external storage using the v2 API. Use
 * [[Dataset.writeTo]] to access this.
 *
 * {{{
 *   df.writeTo("catalog.db.table").using("parquet").create()
 *   df.writeTo("catalog.db.table").append()
 * }}}
 */
final class DataFrameWriterV2[T] private[sql] (table: String, ds: Dataset[T]) {

  private var provider: Option[String] = None
  private val options = mutable.LinkedHashMap.empty[String, String]
  private val properties = mutable.LinkedHashMap.empty[String, String]
  private var partitioning: Seq[proto.Expression] = Nil
  private var clustering: Seq[String] = Nil

  /** Specifies a provider for the underlying output data source. */
  def using(provider: String): this.type = { this.provider = Some(provider); this }

  def option(key: String, value: String): this.type = { options(key) = value; this }
  def options(opts: scala.collection.Map[String, String]): this.type = { options ++= opts; this }

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
  def create(): Unit = executeWrite(proto.WriteOperationV2.Mode.MODE_CREATE)

  /** Replace an existing table with the contents of the DataFrame. */
  def replace(): Unit = executeWrite(proto.WriteOperationV2.Mode.MODE_REPLACE)

  /** Create a new table or replace an existing one with the contents of the DataFrame. */
  def createOrReplace(): Unit = executeWrite(proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)

  /** Append the contents of the DataFrame to the output table. */
  def append(): Unit = executeWrite(proto.WriteOperationV2.Mode.MODE_APPEND)

  /** Overwrite rows matching the given filter with the contents of the DataFrame. */
  def overwrite(condition: Column): Unit =
    executeWrite(
      proto.WriteOperationV2.Mode.MODE_OVERWRITE,
      _.withOverwriteCondition(condition.expr)
    )

  /** Overwrite all partitions touched by the contents of the DataFrame. */
  def overwritePartitions(): Unit =
    executeWrite(proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)

  private def executeWrite(
      mode: proto.WriteOperationV2.Mode,
      f: proto.WriteOperationV2 => proto.WriteOperationV2 = identity
  ): Unit = {
    val write = f(
      proto.WriteOperationV2(
        input = Some(ds.relation),
        tableName = table,
        provider = provider,
        partitioningColumns = partitioning,
        options = options.toMap,
        tableProperties = properties.toMap,
        clusteringColumns = clustering,
        mode = mode
      )
    )
    ds.sparkSession.execute(proto.Command().withWriteOperationV2(write))
  }
}
