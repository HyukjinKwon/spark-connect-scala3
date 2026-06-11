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
 * Interface used to write a [[Dataset]] to external storage systems (file systems, key-value
 * stores, tables, ...). Use [[Dataset.write]] to access this.
 *
 * {{{
 *   df.write.mode("overwrite").parquet("/path/to/output")
 *   df.write.saveAsTable("my_table")
 * }}}
 */
final class DataFrameWriter[T] private[sql] (ds: Dataset[T]) {

  private var source: Option[String] = None
  private var mode: SaveMode = SaveMode.ErrorIfExists
  private val extraOptions = mutable.LinkedHashMap.empty[String, String]
  private var partitioningColumns: Seq[String] = Nil
  private var clusteringColumns: Seq[String] = Nil
  private var sortColumnNames: Seq[String] = Nil
  private var numBuckets: Option[Int] = None
  private var bucketColumnNames: Seq[String] = Nil

  /** Specifies the underlying output data source. */
  def format(source: String): this.type = { this.source = Some(source); this }

  /** Specifies the behavior when data or table already exists. */
  def mode(saveMode: SaveMode): this.type = { this.mode = saveMode; this }

  /** Specifies the behavior when data or table already exists, by name. */
  def mode(saveMode: String): this.type = mode(SaveMode.fromString(saveMode))

  def option(key: String, value: String): this.type = { extraOptions(key) = value; this }
  def option(key: String, value: Boolean): this.type = option(key, value.toString)
  def option(key: String, value: Long): this.type = option(key, value.toString)
  def option(key: String, value: Double): this.type = option(key, value.toString)

  def options(options: scala.collection.Map[String, String]): this.type = {
    extraOptions ++= options; this
  }

  /** Partitions the output by the given columns on the file system. */
  def partitionBy(colNames: String*): this.type = { partitioningColumns = colNames.toSeq; this }

  /** Clusters the output by the given columns (Spark 3.4+ clustering). */
  def clusterBy(colNames: String*): this.type = { clusteringColumns = colNames.toSeq; this }

  /** Buckets the output by the given columns. */
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type = {
    this.numBuckets = Some(numBuckets)
    this.bucketColumnNames = colName +: colNames.toSeq
    this
  }

  /** Sorts the output in each bucket by the given columns. */
  def sortBy(colName: String, colNames: String*): this.type = {
    sortColumnNames = colName +: colNames.toSeq
    this
  }

  /** Saves the content of the DataFrame at the specified path. */
  def save(path: String): Unit = executeWrite(_.withPath(path))

  /** Saves the content of the DataFrame, for data sources that don't require a path. */
  def save(): Unit = executeWrite(identity)

  /** Saves the content of the DataFrame as the specified table. */
  def saveAsTable(tableName: String): Unit =
    executeWrite(
      _.withTable(
        proto.WriteOperation.SaveTable(
          tableName = tableName,
          saveMethod =
            proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE
        )
      )
    )

  /** Inserts the content of the DataFrame into the specified table. */
  def insertInto(tableName: String): Unit =
    executeWrite(
      _.withTable(
        proto.WriteOperation.SaveTable(
          tableName = tableName,
          saveMethod = proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
        )
      )
    )

  def json(path: String): Unit = format("json").save(path)
  def csv(path: String): Unit = format("csv").save(path)
  def parquet(path: String): Unit = format("parquet").save(path)
  def orc(path: String): Unit = format("orc").save(path)
  def text(path: String): Unit = format("text").save(path)

  private def executeWrite(setSaveType: proto.WriteOperation => proto.WriteOperation): Unit = {
    var write = proto.WriteOperation(
      input = Some(ds.relation),
      source = source,
      mode = protoMode,
      sortColumnNames = sortColumnNames,
      partitioningColumns = partitioningColumns,
      clusteringColumns = clusteringColumns,
      options = extraOptions.toMap
    )
    numBuckets.foreach { n =>
      write = write.withBucketBy(
        proto.WriteOperation.BucketBy(bucketColumnNames = bucketColumnNames, numBuckets = n)
      )
    }
    write = setSaveType(write)
    ds.sparkSession.execute(proto.Command().withWriteOperation(write))
  }

  private def protoMode: proto.WriteOperation.SaveMode = mode match {
    case SaveMode.Append => proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
    case SaveMode.Overwrite => proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
    case SaveMode.ErrorIfExists => proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
    case SaveMode.Ignore => proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
  }
}
