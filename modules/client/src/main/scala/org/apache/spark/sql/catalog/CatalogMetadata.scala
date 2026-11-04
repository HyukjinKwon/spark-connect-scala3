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

package org.apache.spark.sql.catalog

/**
 * A catalog in Spark, as returned by `Catalog.listCatalogs`.
 *
 * @param name
 *   name of the catalog.
 * @param description
 *   description of the catalog (may be `null`).
 */
case class CatalogMetadata(name: String, description: String) {
  override def toString: String =
    s"Catalog[name='$name'${Option(description).map(d => s", description='$d'").getOrElse("")}]"
}

/**
 * A partition of a table, as returned by `Catalog.listPartitions` (the same form as `SHOW
 * PARTITIONS`, e.g. `year=2024/month=1`).
 *
 * @param partition
 *   the partition description string.
 */
case class TablePartition(partition: String) {
  override def toString: String = s"TablePartition[partition='$partition']"
}

/** A table property key and value, as returned by `Catalog.getTableProperties`. */
case class CatalogTableProperty(key: String, value: String) {
  override def toString: String = s"CatalogTableProperty[key='$key', value='$value']"
}

/**
 * A column of a table, as returned by `Catalog.listColumns`.
 *
 * @param name
 *   name of the column.
 * @param description
 *   description of the column (may be `null`).
 * @param dataType
 *   the column's data type as a string.
 * @param nullable
 *   whether the column is nullable.
 * @param isPartition
 *   whether the column is a partition column.
 * @param isBucket
 *   whether the column is a bucket column.
 * @param isCluster
 *   whether the column is a clustering column (always `false` on Spark 3.5 servers).
 */
case class Column(
    name: String,
    description: String,
    dataType: String,
    nullable: Boolean,
    isPartition: Boolean,
    isBucket: Boolean,
    isCluster: Boolean
) {
  override def toString: String =
    s"Column[name='$name'${Option(description).map(d => s", description='$d'").getOrElse("")}, " +
      s"dataType='$dataType', nullable='$nullable', isPartition='$isPartition', " +
      s"isBucket='$isBucket', isCluster='$isCluster']"
}

/**
 * A database in the catalog, as returned by [[Catalog.getDatabase]] / [[Catalog.listDatabases]].
 *
 * @param name
 *   name of the database.
 * @param catalog
 *   name of the catalog the database belongs to (may be `null` on older servers).
 * @param description
 *   description of the database.
 * @param locationUri
 *   path (in the form of a uri) to data files.
 */
case class Database(name: String, catalog: String, description: String, locationUri: String) {
  override def toString: String =
    s"Database[name='$name'${Option(catalog).map(c => s", catalog='$c'").getOrElse("")}" +
      s"${Option(description).map(d => s", description='$d'").getOrElse("")}, path='$locationUri']"
}

/**
 * A table or view in the catalog, as returned by [[Catalog.getTable]] / [[Catalog.listTables]].
 *
 * @param name
 *   name of the table.
 * @param catalog
 *   name of the catalog the table belongs to (may be `null` on older servers).
 * @param namespace
 *   the namespace (database) the table belongs to; empty for temporary views.
 * @param description
 *   description of the table.
 * @param tableType
 *   type of the table (e.g. `MANAGED`, `EXTERNAL`, `VIEW`, `TEMPORARY`).
 * @param isTemporary
 *   whether the table is a temporary view.
 */
case class Table(
    name: String,
    catalog: String,
    namespace: Array[String],
    description: String,
    tableType: String,
    isTemporary: Boolean
) {

  /** The database (single-level namespace) the table belongs to, or `null` if not applicable. */
  def database: String = if (namespace == null || namespace.isEmpty) null else namespace.last

  override def toString: String =
    s"Table[name='$name'${Option(catalog).map(c => s", catalog='$c'").getOrElse("")}" +
      s"${Option(database).map(d => s", database='$d'").getOrElse("")}" +
      s"${Option(description).map(d => s", description='$d'").getOrElse("")}, " +
      s"tableType='$tableType', isTemporary='$isTemporary']"
}

/**
 * A function in the catalog, as returned by [[Catalog.getFunction]] / [[Catalog.listFunctions]].
 *
 * @param name
 *   name of the function.
 * @param catalog
 *   name of the catalog the function belongs to (may be `null` on older servers).
 * @param namespace
 *   the namespace the function belongs to; `null`/empty for temporary functions.
 * @param description
 *   description of the function.
 * @param className
 *   the fully qualified class name of the function.
 * @param isTemporary
 *   whether the function is a temporary function.
 */
case class Function(
    name: String,
    catalog: String,
    namespace: Array[String],
    description: String,
    className: String,
    isTemporary: Boolean
) {

  override def toString: String =
    s"Function[name='$name'${Option(catalog).map(c => s", catalog='$c'").getOrElse("")}" +
      s"${Option(description).map(d => s", description='$d'").getOrElse("")}, " +
      s"className='$className', isTemporary='$isTemporary']"
}
