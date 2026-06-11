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

import org.apache.spark.connect.proto

/**
 * Catalog interface for Spark, backed by the Spark Connect `Catalog` relations. Mirrors
 * `org.apache.spark.sql.catalog.Catalog`. List operations return [[DataFrame]]s describing
 * catalogs/databases/tables/columns/functions; existence and "current" operations return scalars.
 *
 * Access via [[SparkSession.catalog]].
 */
class Catalog private[sql] (sparkSession: SparkSession) {

  // --- current catalog / database ---

  def currentCatalog(): String =
    scalarString(catalog(_.withCurrentCatalog(proto.CurrentCatalog())))

  def setCurrentCatalog(catalogName: String): Unit =
    run(catalog(_.withSetCurrentCatalog(proto.SetCurrentCatalog(catalogName = catalogName))))

  def listCatalogs(): DataFrame = catalog(_.withListCatalogs(proto.ListCatalogs()))

  def listCatalogs(pattern: String): DataFrame =
    catalog(_.withListCatalogs(proto.ListCatalogs(pattern = Some(pattern))))

  def currentDatabase: String =
    scalarString(catalog(_.withCurrentDatabase(proto.CurrentDatabase())))

  def setCurrentDatabase(dbName: String): Unit =
    run(catalog(_.withSetCurrentDatabase(proto.SetCurrentDatabase(dbName = dbName))))

  // --- listing ---

  def listDatabases(): DataFrame = catalog(_.withListDatabases(proto.ListDatabases()))

  def listDatabases(pattern: String): DataFrame =
    catalog(_.withListDatabases(proto.ListDatabases(pattern = Some(pattern))))

  def listTables(): DataFrame = catalog(_.withListTables(proto.ListTables()))

  def listTables(dbName: String): DataFrame =
    catalog(_.withListTables(proto.ListTables(dbName = Some(dbName))))

  def listColumns(tableName: String): DataFrame =
    catalog(_.withListColumns(proto.ListColumns(tableName = tableName)))

  def listColumns(dbName: String, tableName: String): DataFrame =
    catalog(_.withListColumns(proto.ListColumns(tableName = tableName, dbName = Some(dbName))))

  def listFunctions(): DataFrame = catalog(_.withListFunctions(proto.ListFunctions()))

  def listFunctions(dbName: String): DataFrame =
    catalog(_.withListFunctions(proto.ListFunctions(dbName = Some(dbName))))

  // --- existence ---

  def databaseExists(dbName: String): Boolean =
    scalarBool(catalog(_.withDatabaseExists(proto.DatabaseExists(dbName = dbName))))

  def tableExists(tableName: String): Boolean =
    scalarBool(catalog(_.withTableExists(proto.TableExists(tableName = tableName))))

  def tableExists(dbName: String, tableName: String): Boolean =
    scalarBool(
      catalog(_.withTableExists(proto.TableExists(tableName = tableName, dbName = Some(dbName))))
    )

  def functionExists(functionName: String): Boolean =
    scalarBool(catalog(_.withFunctionExists(proto.FunctionExists(functionName = functionName))))

  // --- views / cache ---

  def dropTempView(viewName: String): Boolean =
    scalarBool(catalog(_.withDropTempView(proto.DropTempView(viewName = viewName))))

  def dropGlobalTempView(viewName: String): Boolean =
    scalarBool(catalog(_.withDropGlobalTempView(proto.DropGlobalTempView(viewName = viewName))))

  def isCached(tableName: String): Boolean =
    scalarBool(catalog(_.withIsCached(proto.IsCached(tableName = tableName))))

  def cacheTable(tableName: String): Unit =
    run(catalog(_.withCacheTable(proto.CacheTable(tableName = tableName))))

  def uncacheTable(tableName: String): Unit =
    run(catalog(_.withUncacheTable(proto.UncacheTable(tableName = tableName))))

  def clearCache(): Unit = run(catalog(_.withClearCache(proto.ClearCache())))

  // --- maintenance ---

  def recoverPartitions(tableName: String): Unit =
    run(catalog(_.withRecoverPartitions(proto.RecoverPartitions(tableName = tableName))))

  def refreshTable(tableName: String): Unit =
    run(catalog(_.withRefreshTable(proto.RefreshTable(tableName = tableName))))

  def refreshByPath(path: String): Unit =
    run(catalog(_.withRefreshByPath(proto.RefreshByPath(path = path))))

  // --- internals ---

  private def catalog(f: proto.Catalog => proto.Catalog): DataFrame =
    sparkSession.newDataFrame(proto.Relation().withCatalog(f(proto.Catalog())))

  private def scalarString(df: DataFrame): String = df.collect().head.getString(0)
  private def scalarBool(df: DataFrame): Boolean = df.collect().head.getBoolean(0)
  private def run(df: DataFrame): Unit = df.collect()
}
