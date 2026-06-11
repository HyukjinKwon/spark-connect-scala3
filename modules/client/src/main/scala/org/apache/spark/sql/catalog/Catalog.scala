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

import org.apache.spark.connect.proto
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.StructType

/**
 * The catalog interface for inspecting and managing databases, tables, functions, and the query
 * cache.
 *
 * Methods that return rows ([[listDatabases]], [[listTables]], ...) return a [[DataFrame]];
 * predicate methods return a `Boolean`; command-like methods return `Unit`. Mirrors PySpark's
 * `Catalog`.
 *
 * {{{
 *   spark.catalog.listTables().show()
 *   spark.catalog.tableExists("my_table")  // => true
 * }}}
 */
class Catalog private[sql] (sparkSession: SparkSession) {

  // -- Catalogs --------------------------------------------------------------

  /** Returns the current default catalog. */
  def currentCatalog: String =
    scalarString(proto.Catalog.CatType.CurrentCatalog(proto.CurrentCatalog()))

  /** Sets the current default catalog. */
  def setCurrentCatalog(catalogName: String): Unit =
    run(proto.Catalog.CatType.SetCurrentCatalog(proto.SetCurrentCatalog(catalogName = catalogName)))

  /** Returns a [[DataFrame]] of all catalogs. */
  def listCatalogs(): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListCatalogs(proto.ListCatalogs()))

  // -- Databases -------------------------------------------------------------

  /** Returns the current default database. */
  def currentDatabase: String =
    scalarString(proto.Catalog.CatType.CurrentDatabase(proto.CurrentDatabase()))

  /** Sets the current default database. */
  def setCurrentDatabase(dbName: String): Unit =
    run(proto.Catalog.CatType.SetCurrentDatabase(proto.SetCurrentDatabase(dbName = dbName)))

  /** Returns a [[DataFrame]] of all databases. */
  def listDatabases(): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListDatabases(proto.ListDatabases()))

  /** Returns whether a database with the given name exists. */
  def databaseExists(dbName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.DatabaseExists(proto.DatabaseExists(dbName = dbName)))

  /** Gets the database with the given name as a [[Database]]. */
  def getDatabase(dbName: String): Database =
    toDatabase(single(proto.Catalog.CatType.GetDatabase(proto.GetDatabase(dbName = dbName))))

  // -- Tables ----------------------------------------------------------------

  /** Returns a [[DataFrame]] of tables (and views) in the current database. */
  def listTables(): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListTables(proto.ListTables()))

  /** Returns a [[DataFrame]] of tables (and views) in the given database. */
  def listTables(dbName: String): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListTables(proto.ListTables(dbName = Some(dbName))))

  /** Returns whether a table or view with the given name exists. */
  def tableExists(tableName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.TableExists(proto.TableExists(tableName = tableName)))

  /** Returns whether a table or view with the given name in the given database exists. */
  def tableExists(dbName: String, tableName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.TableExists(
        proto.TableExists(tableName = tableName, dbName = Some(dbName))
      )
    )

  /** Gets the table or view with the given name as a [[Table]]. */
  def getTable(tableName: String): Table =
    toTable(single(proto.Catalog.CatType.GetTable(proto.GetTable(tableName = tableName))))

  /** Gets the table or view of the given name in the given database as a [[Table]]. */
  def getTable(dbName: String, tableName: String): Table =
    toTable(
      single(
        proto.Catalog.CatType.GetTable(
          proto.GetTable(tableName = tableName, dbName = Some(dbName))
        )
      )
    )

  /**
   * Creates a table from the given path (using the default data source) and returns the
   * corresponding [[DataFrame]].
   */
  def createTable(tableName: String, path: String): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.CreateTable(
        proto.CreateTable(tableName = tableName, path = Some(path))
      )
    )

  /**
   * Creates a table from the given source and options, returning the corresponding [[DataFrame]].
   */
  def createTable(tableName: String, source: String, options: Map[String, String]): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.CreateTable(
        proto.CreateTable(tableName = tableName, source = Some(source), options = options)
      )
    )

  /**
   * Creates a table from the given source, schema and options, returning the corresponding
   * [[DataFrame]].
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.CreateTable(
        proto.CreateTable(
          tableName = tableName,
          source = Some(source),
          schema = Some(DataTypeProtoConverter.toConnectProtoType(schema)),
          options = options
        )
      )
    )

  /** Creates a table from the given path and returns the corresponding [[DataFrame]]. */
  def createExternalTable(tableName: String, path: String): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.CreateExternalTable(
        proto.CreateExternalTable(tableName = tableName, path = Some(path))
      )
    )

  /**
   * Creates a table from the given source and options, returning the corresponding [[DataFrame]].
   */
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]
  ): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.CreateExternalTable(
        proto.CreateExternalTable(tableName = tableName, source = Some(source), options = options)
      )
    )

  // -- Columns ---------------------------------------------------------------

  /** Returns a [[DataFrame]] of the columns of the given table. */
  def listColumns(tableName: String): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListColumns(proto.ListColumns(tableName = tableName)))

  /** Returns a [[DataFrame]] of the columns of the given table in the given database. */
  def listColumns(dbName: String, tableName: String): DataFrame =
    catalogDataFrame(
      proto.Catalog.CatType.ListColumns(
        proto.ListColumns(tableName = tableName, dbName = Some(dbName))
      )
    )

  // -- Functions -------------------------------------------------------------

  /** Returns a [[DataFrame]] of functions registered in the catalog. */
  def listFunctions(): DataFrame =
    catalogDataFrame(proto.Catalog.CatType.ListFunctions(proto.ListFunctions()))

  /** Returns whether a function with the given name exists. */
  def functionExists(functionName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.FunctionExists(proto.FunctionExists(functionName = functionName))
    )

  /** Gets the function with the given name as a [[Function]]. */
  def getFunction(functionName: String): Function =
    toFunction(
      single(proto.Catalog.CatType.GetFunction(proto.GetFunction(functionName = functionName)))
    )

  /** Gets the function of the given name in the given database as a [[Function]]. */
  def getFunction(dbName: String, functionName: String): Function =
    toFunction(
      single(
        proto.Catalog.CatType.GetFunction(
          proto.GetFunction(functionName = functionName, dbName = Some(dbName))
        )
      )
    )

  // -- Views -----------------------------------------------------------------

  /** Drops a session-local temporary view. Returns whether the view was dropped. */
  def dropTempView(viewName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.DropTempView(proto.DropTempView(viewName = viewName)))

  /** Drops a global temporary view. Returns whether the view was dropped. */
  def dropGlobalTempView(viewName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.DropGlobalTempView(proto.DropGlobalTempView(viewName = viewName))
    )

  // -- Cache -----------------------------------------------------------------

  /** Returns whether the given table is cached. */
  def isCached(tableName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.IsCached(proto.IsCached(tableName = tableName)))

  /** Caches the given table in memory. */
  def cacheTable(tableName: String): Unit =
    run(proto.Catalog.CatType.CacheTable(proto.CacheTable(tableName = tableName)))

  /** Removes the given table from the cache. */
  def uncacheTable(tableName: String): Unit =
    run(proto.Catalog.CatType.UncacheTable(proto.UncacheTable(tableName = tableName)))

  /** Clears all cached tables. */
  def clearCache(): Unit =
    run(proto.Catalog.CatType.ClearCache(proto.ClearCache()))

  // -- Refresh ---------------------------------------------------------------

  /** Invalidates and refreshes cached metadata for the given table. */
  def refreshTable(tableName: String): Unit =
    run(proto.Catalog.CatType.RefreshTable(proto.RefreshTable(tableName = tableName)))

  /** Invalidates and refreshes cached data (and metadata) at the given path. */
  def refreshByPath(path: String): Unit =
    run(proto.Catalog.CatType.RefreshByPath(proto.RefreshByPath(path = path)))

  /** Recovers all partitions of the given table. */
  def recoverPartitions(tableName: String): Unit =
    run(proto.Catalog.CatType.RecoverPartitions(proto.RecoverPartitions(tableName = tableName)))

  // -- Internals -------------------------------------------------------------

  private def catalogDataFrame(catType: proto.Catalog.CatType): DataFrame =
    sparkSession.newDataFrame(proto.Relation.RelType.Catalog(proto.Catalog(catType = catType)))

  private def rows(catType: proto.Catalog.CatType): Array[Row] = {
    val relation =
      sparkSession.newRelation(proto.Relation.RelType.Catalog(proto.Catalog(catType = catType)))
    sparkSession.execute(proto.Plan(proto.Plan.OpType.Root(relation))).toArray
  }

  private def scalarString(catType: proto.Catalog.CatType): String =
    rows(catType).head.getString(0)

  private def scalarBoolean(catType: proto.Catalog.CatType): Boolean =
    rows(catType).head.getBoolean(0)

  private def single(catType: proto.Catalog.CatType): Row = rows(catType).head

  private def hasField(r: Row, name: String): Boolean =
    r.schema != null && r.schema.fieldNames.contains(name)

  private def optStr(r: Row, name: String): String =
    if (hasField(r, name) && !r.isNullAt(r.fieldIndex(name))) r.getString(r.fieldIndex(name))
    else null

  private def optBool(r: Row, name: String): Boolean =
    hasField(r, name) && !r.isNullAt(r.fieldIndex(name)) && r.getBoolean(r.fieldIndex(name))

  private def namespaceOf(r: Row): Array[String] = {
    def asArr(v: Any): Array[String] = v match {
      case null => Array.empty[String]
      case a: Array[?] => a.map(String.valueOf)
      case s: scala.collection.Seq[?] => s.map(String.valueOf).toArray
      case other => Array(String.valueOf(other))
    }
    if (hasField(r, "namespace") && !r.isNullAt(r.fieldIndex("namespace")))
      asArr(r.get(r.fieldIndex("namespace")))
    else if (hasField(r, "database") && !r.isNullAt(r.fieldIndex("database")))
      Array(r.getString(r.fieldIndex("database")))
    else Array.empty[String]
  }

  private def toDatabase(r: Row): Database =
    Database(
      optStr(r, "name"),
      optStr(r, "catalog"),
      optStr(r, "description"),
      optStr(r, "locationUri")
    )

  private def toTable(r: Row): Table =
    Table(
      optStr(r, "name"),
      optStr(r, "catalog"),
      namespaceOf(r),
      optStr(r, "description"),
      optStr(r, "tableType"),
      optBool(r, "isTemporary")
    )

  private def toFunction(r: Row): Function =
    Function(
      optStr(r, "name"),
      optStr(r, "catalog"),
      namespaceOf(r),
      optStr(r, "description"),
      optStr(r, "className"),
      optBool(r, "isTemporary")
    )

  private def run(catType: proto.Catalog.CatType): Unit = {
    rows(catType)
    ()
  }
}
