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

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.connect.proto
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.storage.StorageLevel

/**
 * The catalog interface for inspecting and managing catalogs, databases, tables, views, functions,
 * and the query cache. Access it with `spark.catalog`. Mirrors `org.apache.spark.sql.catalog`.
 *
 * Listing methods return typed Datasets (`Dataset[Table]`, `Dataset[Database]`, ...); predicate
 * methods return a `Boolean`; command-like methods return `Unit` and run immediately.
 *
 * {{{
 *   spark.catalog.listTables().show()
 *   spark.catalog.listTables().collect().map(_.name)
 *   spark.catalog.tableExists("my_table")  // => true
 * }}}
 */
class Catalog private[sql] (sparkSession: SparkSession) {

  import Catalog._

  // -- Catalogs --------------------------------------------------------------

  /** Returns the current default catalog. */
  def currentCatalog(): String =
    scalarString(proto.Catalog.CatType.CurrentCatalog(proto.CurrentCatalog()))

  /** Sets the current default catalog. */
  def setCurrentCatalog(catalogName: String): Unit =
    run(proto.Catalog.CatType.SetCurrentCatalog(proto.SetCurrentCatalog(catalogName = catalogName)))

  /** Returns a list of the catalogs available in this session. */
  def listCatalogs(): Dataset[CatalogMetadata] =
    catalogDataset(proto.Catalog.CatType.ListCatalogs(proto.ListCatalogs()), catalogEncoder)

  /** Returns the catalogs whose name matches the SQL LIKE `pattern`. */
  def listCatalogs(pattern: String): Dataset[CatalogMetadata] =
    catalogDataset(
      proto.Catalog.CatType.ListCatalogs(proto.ListCatalogs(pattern = Some(pattern))),
      catalogEncoder
    )

  // -- Databases -------------------------------------------------------------

  /** Returns the current default database. */
  def currentDatabase: String =
    scalarString(proto.Catalog.CatType.CurrentDatabase(proto.CurrentDatabase()))

  /** Sets the current default database. */
  def setCurrentDatabase(dbName: String): Unit =
    run(proto.Catalog.CatType.SetCurrentDatabase(proto.SetCurrentDatabase(dbName = dbName)))

  /** Returns a list of the databases (namespaces) in the current catalog. */
  def listDatabases(): Dataset[Database] =
    catalogDataset(proto.Catalog.CatType.ListDatabases(proto.ListDatabases()), databaseEncoder)

  /** Returns the databases whose name matches the SQL LIKE `pattern`. */
  def listDatabases(pattern: String): Dataset[Database] =
    catalogDataset(
      proto.Catalog.CatType.ListDatabases(proto.ListDatabases(pattern = Some(pattern))),
      databaseEncoder
    )

  /** Returns true if the database exists. */
  def databaseExists(dbName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.DatabaseExists(proto.DatabaseExists(dbName = dbName)))

  /** Returns the database with the given name; throws if it does not exist. */
  def getDatabase(dbName: String): Database =
    toDatabase(single(proto.Catalog.CatType.GetDatabase(proto.GetDatabase(dbName = dbName))))

  /** Creates a database (namespace); `dbName` may be multi-part (Spark 4.2+). */
  def createDatabase(dbName: String, ifNotExists: Boolean = false): Unit =
    createDatabase(dbName, ifNotExists, Map.empty[String, String])

  /**
   * Creates a database with the given properties (e.g. `comment`, `location`), like `CREATE
   * NAMESPACE ... WITH DBPROPERTIES` (Spark 4.2+).
   */
  def createDatabase(dbName: String, ifNotExists: Boolean, properties: Map[String, String]): Unit =
    run(
      proto.Catalog.CatType.CreateDatabase(
        proto.CreateDatabase(dbName = dbName, ifNotExists = ifNotExists, properties = properties)
      )
    )

  /** (Java-specific) Creates a database with the given properties (Spark 4.2+). */
  def createDatabase(
      dbName: String,
      ifNotExists: Boolean,
      properties: java.util.Map[String, String]
  ): Unit = createDatabase(dbName, ifNotExists, properties.asScala.toMap)

  /** Drops a database; with `cascade`, also drops the objects it contains (Spark 4.2+). */
  def dropDatabase(dbName: String, ifExists: Boolean = false, cascade: Boolean = false): Unit =
    run(
      proto.Catalog.CatType.DropDatabase(
        proto.DropDatabase(dbName = dbName, ifExists = ifExists, cascade = cascade)
      )
    )

  // -- Tables ----------------------------------------------------------------

  /** Returns the tables and views in the current database, including temporary views. */
  def listTables(): Dataset[Table] =
    catalogDataset(proto.Catalog.CatType.ListTables(proto.ListTables()), tableEncoder)

  /** Returns the tables and views in the given database, including temporary views. */
  def listTables(dbName: String): Dataset[Table] =
    catalogDataset(
      proto.Catalog.CatType.ListTables(proto.ListTables(dbName = Some(dbName))),
      tableEncoder
    )

  /** Returns the tables and views of `dbName` whose name matches the SQL LIKE `pattern`. */
  def listTables(dbName: String, pattern: String): Dataset[Table] =
    catalogDataset(
      proto.Catalog.CatType.ListTables(
        proto.ListTables(dbName = Some(dbName), pattern = Some(pattern))
      ),
      tableEncoder
    )

  /** Returns true if the table or view exists. */
  def tableExists(tableName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.TableExists(proto.TableExists(tableName = tableName)))

  /** Returns true if the table or view exists in the given database. */
  def tableExists(dbName: String, tableName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.TableExists(
        proto.TableExists(tableName = tableName, dbName = Some(dbName))
      )
    )

  /** Returns the table or view with the given name; throws if it does not exist. */
  def getTable(tableName: String): Table =
    toTable(single(proto.Catalog.CatType.GetTable(proto.GetTable(tableName = tableName))))

  /** Returns the table or view with the given name in the given database. */
  def getTable(dbName: String, tableName: String): Table =
    toTable(
      single(
        proto.Catalog.CatType.GetTable(
          proto.GetTable(tableName = tableName, dbName = Some(dbName))
        )
      )
    )

  /**
   * Creates a table from the files at `path`, using the default data source
   * (`spark.sql.sources.default`), and returns it as a [[DataFrame]].
   */
  def createTable(tableName: String, path: String): DataFrame =
    createTableInternal(tableName, None, new StructType, "", Map("path" -> path))

  /** Creates a table from the files at `path` using the data source `source`. */
  def createTable(tableName: String, path: String, source: String): DataFrame =
    createTable(tableName, source, Map("path" -> path))

  /** Creates a table from a data source and options. */
  def createTable(tableName: String, source: String, options: Map[String, String]): DataFrame =
    createTable(tableName, source, new StructType, options)

  /** (Java-specific) Creates a table from a data source and options. */
  def createTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, options.asScala.toMap)

  /** Creates a table from a data source, a description, and options. */
  def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]
  ): DataFrame = createTable(tableName, source, new StructType, description, options)

  /** (Java-specific) Creates a table from a data source, a description, and options. */
  def createTable(
      tableName: String,
      source: String,
      description: String,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, description, options.asScala.toMap)

  /** Creates a table from a data source, a schema, and options. */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame = createTable(tableName, source, schema, "", options)

  /** (Java-specific) Creates a table from a data source, a schema, and options. */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, schema, options.asScala.toMap)

  /** Creates a table from a data source, a schema, a description, and options. */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]
  ): DataFrame = createTableInternal(tableName, Some(source), schema, description, options)

  /** (Java-specific) Creates a table from a data source, schema, description, and options. */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, schema, description, options.asScala.toMap)

  /**
   * Creates the table right away (like Apache Spark 4.3) and returns it as a [[DataFrame]]. With no
   * `source`, the server's default data source (`spark.sql.sources.default`) is used.
   */
  private def createTableInternal(
      tableName: String,
      source: Option[String],
      schema: StructType,
      description: String,
      options: Map[String, String]
  ): DataFrame = {
    run(
      proto.Catalog.CatType.CreateTable(
        proto.CreateTable(
          tableName = tableName,
          source = source,
          schema = Some(DataTypeProtoConverter.toConnectProtoType(schema)),
          description = Some(description),
          options = options
        )
      )
    )
    sparkSession.table(tableName)
  }

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String): DataFrame =
    createTable(tableName, path)

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String, source: String): DataFrame =
    createTable(tableName, path, source)

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]
  ): DataFrame = createTable(tableName, source, options)

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, options)

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame = createTable(tableName, source, schema, options)

  @deprecated("use createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]
  ): DataFrame = createTable(tableName, source, schema, options)

  /** Drops a persistent table; temporary views are dropped with [[dropTempView]] (Spark 4.2+). */
  def dropTable(tableName: String, ifExists: Boolean = false, purge: Boolean = false): Unit =
    run(
      proto.Catalog.CatType.DropTable(
        proto.DropTable(tableName = tableName, ifExists = ifExists, purge = purge)
      )
    )

  /** Removes all rows from a table; not supported for views (Spark 4.2+). */
  def truncateTable(tableName: String): Unit =
    run(proto.Catalog.CatType.TruncateTable(proto.TruncateTable(tableName = tableName)))

  /** Computes table statistics like `ANALYZE TABLE ... COMPUTE STATISTICS` (Spark 4.2+). */
  def analyzeTable(tableName: String, noScan: Boolean = false): Unit =
    run(
      proto.Catalog.CatType.AnalyzeTable(
        proto.AnalyzeTable(tableName = tableName, noScan = noScan)
      )
    )

  /** Returns the partitions of a table, like `SHOW PARTITIONS` (Spark 4.2+). */
  def listPartitions(tableName: String): Dataset[TablePartition] =
    catalogDataset(
      proto.Catalog.CatType.ListPartitions(proto.ListPartitions(tableName = tableName)),
      partitionEncoder
    )

  /** Returns all properties of a table or view, like `SHOW TBLPROPERTIES` (Spark 4.2+). */
  def getTableProperties(tableName: String): java.util.Map[String, String] = {
    val properties = new java.util.LinkedHashMap[String, String]()
    rows(
      proto.Catalog.CatType.GetTableProperties(proto.GetTableProperties(tableName = tableName))
    ).foreach(r => properties.put(r.getString(0), r.getString(1)))
    properties
  }

  /**
   * Returns the `SHOW CREATE TABLE` statement of a table or view; with `asSerde`, the Hive SerDe
   * form when applicable (Spark 4.2+).
   */
  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    rows(
      proto.Catalog.CatType.GetCreateTableString(
        proto.GetCreateTableString(tableName = tableName, asSerde = asSerde)
      )
    ).headOption.map(_.getString(0)).getOrElse("")

  // -- Columns ---------------------------------------------------------------

  /** Returns the columns of the given table or view. */
  def listColumns(tableName: String): Dataset[Column] =
    catalogDataset(
      proto.Catalog.CatType.ListColumns(proto.ListColumns(tableName = tableName)),
      columnEncoder
    )

  /** Returns the columns of the given table or view in the given database. */
  def listColumns(dbName: String, tableName: String): Dataset[Column] =
    catalogDataset(
      proto.Catalog.CatType.ListColumns(
        proto.ListColumns(tableName = tableName, dbName = Some(dbName))
      ),
      columnEncoder
    )

  // -- Functions -------------------------------------------------------------

  /** Returns the functions registered in the current database, including temporary ones. */
  def listFunctions(): Dataset[Function] =
    catalogDataset(proto.Catalog.CatType.ListFunctions(proto.ListFunctions()), functionEncoder)

  /** Returns the functions registered in the given database, including temporary ones. */
  def listFunctions(dbName: String): Dataset[Function] =
    catalogDataset(
      proto.Catalog.CatType.ListFunctions(proto.ListFunctions(dbName = Some(dbName))),
      functionEncoder
    )

  /** Returns the functions of `dbName` whose name matches the SQL LIKE `pattern`. */
  def listFunctions(dbName: String, pattern: String): Dataset[Function] =
    catalogDataset(
      proto.Catalog.CatType.ListFunctions(
        proto.ListFunctions(dbName = Some(dbName), pattern = Some(pattern))
      ),
      functionEncoder
    )

  /** Returns true if the function exists. */
  def functionExists(functionName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.FunctionExists(proto.FunctionExists(functionName = functionName))
    )

  /** Returns true if the function exists in the given database. */
  def functionExists(dbName: String, functionName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.FunctionExists(
        proto.FunctionExists(functionName = functionName, dbName = Some(dbName))
      )
    )

  /** Returns the function with the given name; throws if it does not exist. */
  def getFunction(functionName: String): Function =
    toFunction(
      single(proto.Catalog.CatType.GetFunction(proto.GetFunction(functionName = functionName)))
    )

  /** Returns the function with the given name in the given database. */
  def getFunction(dbName: String, functionName: String): Function =
    toFunction(
      single(
        proto.Catalog.CatType.GetFunction(
          proto.GetFunction(functionName = functionName, dbName = Some(dbName))
        )
      )
    )

  // -- Views -----------------------------------------------------------------

  /** Returns the views in the current database (Spark 4.2+). */
  def listViews(): Dataset[Table] =
    catalogDataset(proto.Catalog.CatType.ListViews(proto.ListViews()), tableEncoder)

  /** Returns the views in the given (possibly catalog-qualified) database (Spark 4.2+). */
  def listViews(dbName: String): Dataset[Table] =
    catalogDataset(
      proto.Catalog.CatType.ListViews(proto.ListViews(dbName = Some(dbName))),
      tableEncoder
    )

  /** Returns the views of `dbName` whose name matches the SQL LIKE `pattern` (Spark 4.2+). */
  def listViews(dbName: String, pattern: String): Dataset[Table] =
    catalogDataset(
      proto.Catalog.CatType.ListViews(
        proto.ListViews(dbName = Some(dbName), pattern = Some(pattern))
      ),
      tableEncoder
    )

  /** Drops a persistent view; temporary views are dropped with [[dropTempView]] (Spark 4.2+). */
  def dropView(viewName: String, ifExists: Boolean = false): Unit =
    run(proto.Catalog.CatType.DropView(proto.DropView(viewName = viewName, ifExists = ifExists)))

  /** Drops a local temporary view; returns true if it existed. */
  def dropTempView(viewName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.DropTempView(proto.DropTempView(viewName = viewName)))

  /** Drops a global temporary view; returns true if it existed. */
  def dropGlobalTempView(viewName: String): Boolean =
    scalarBoolean(
      proto.Catalog.CatType.DropGlobalTempView(proto.DropGlobalTempView(viewName = viewName))
    )

  // -- Cache -----------------------------------------------------------------

  /** Returns true if the table is cached in memory. */
  def isCached(tableName: String): Boolean =
    scalarBoolean(proto.Catalog.CatType.IsCached(proto.IsCached(tableName = tableName)))

  /** Caches the table in memory. */
  def cacheTable(tableName: String): Unit =
    run(proto.Catalog.CatType.CacheTable(proto.CacheTable(tableName = tableName)))

  /** Caches the table with the given storage level. */
  def cacheTable(tableName: String, storageLevel: StorageLevel): Unit =
    run(
      proto.Catalog.CatType.CacheTable(
        proto.CacheTable(tableName = tableName, storageLevel = Some(storageLevel.toProto))
      )
    )

  /** Removes the table from the in-memory cache. */
  def uncacheTable(tableName: String): Unit =
    run(proto.Catalog.CatType.UncacheTable(proto.UncacheTable(tableName = tableName)))

  /** Removes all cached tables from the in-memory cache. */
  def clearCache(): Unit =
    run(proto.Catalog.CatType.ClearCache(proto.ClearCache()))

  // -- Refresh ---------------------------------------------------------------

  /** Invalidates and refreshes the cached data and metadata of the table. */
  def refreshTable(tableName: String): Unit =
    run(proto.Catalog.CatType.RefreshTable(proto.RefreshTable(tableName = tableName)))

  /** Invalidates and refreshes the cached data of any Dataset that reads from `path`. */
  def refreshByPath(path: String): Unit =
    run(proto.Catalog.CatType.RefreshByPath(proto.RefreshByPath(path = path)))

  /** Recovers all the partitions of the table and updates the catalog. */
  def recoverPartitions(tableName: String): Unit =
    run(proto.Catalog.CatType.RecoverPartitions(proto.RecoverPartitions(tableName = tableName)))

  // -- Internals -------------------------------------------------------------

  private def catalogDataFrame(catType: proto.Catalog.CatType): DataFrame =
    sparkSession.newDataFrame(proto.Relation.RelType.Catalog(proto.Catalog(catType = catType)))

  private def catalogDataset[T](catType: proto.Catalog.CatType, enc: Encoder[T]): Dataset[T] =
    catalogDataFrame(catType).as[T](using enc)

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

  private def run(catType: proto.Catalog.CatType): Unit = {
    rows(catType)
    ()
  }
}

private[sql] object Catalog {

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

  private[sql] def toDatabase(r: Row): Database =
    Database(
      optStr(r, "name"),
      optStr(r, "catalog"),
      optStr(r, "description"),
      optStr(r, "locationUri")
    )

  private[sql] def toTable(r: Row): Table =
    Table(
      optStr(r, "name"),
      optStr(r, "catalog"),
      namespaceOf(r),
      optStr(r, "description"),
      optStr(r, "tableType"),
      optBool(r, "isTemporary")
    )

  private[sql] def toFunction(r: Row): Function =
    Function(
      optStr(r, "name"),
      optStr(r, "catalog"),
      namespaceOf(r),
      optStr(r, "description"),
      optStr(r, "className"),
      optBool(r, "isTemporary")
    )

  private[sql] def toColumn(r: Row): Column =
    Column(
      optStr(r, "name"),
      optStr(r, "description"),
      optStr(r, "dataType"),
      optBool(r, "nullable"),
      optBool(r, "isPartition"),
      optBool(r, "isBucket"),
      optBool(r, "isCluster")
    )

  /**
   * An encoder that decodes catalog result rows by field name, so it tolerates servers that send
   * fewer or reordered columns (e.g. Spark 3.5 has no `isCluster` column).
   */
  private def byName[T](fields: Seq[(String, DataType)], decode: Row => T)(using
      ct: ClassTag[T]
  ): Encoder[T] = new Encoder[T] {
    override def dataType: DataType =
      StructType(fields.map { case (n, t) => StructField(n, t) }.toArray)
    override def nullable: Boolean = true
    override def classTag: ClassTag[T] = ct
    override private[sql] def fromValue(value: Any): T = decode(value.asInstanceOf[Row])
    override private[sql] def toValue(value: T): Any =
      Row.fromSeq(value.asInstanceOf[Product].productIterator.toSeq)
    override private[sql] def fromRow(row: Row): T = decode(row)
  }

  private val strings = ArrayType(StringType)

  private val catalogEncoder: Encoder[CatalogMetadata] =
    byName(
      Seq("name" -> StringType, "description" -> StringType),
      r => CatalogMetadata(optStr(r, "name"), optStr(r, "description"))
    )

  private val databaseEncoder: Encoder[Database] =
    byName(
      Seq(
        "name" -> StringType,
        "catalog" -> StringType,
        "description" -> StringType,
        "locationUri" -> StringType
      ),
      toDatabase
    )

  private val tableEncoder: Encoder[Table] =
    byName(
      Seq(
        "name" -> StringType,
        "catalog" -> StringType,
        "namespace" -> strings,
        "description" -> StringType,
        "tableType" -> StringType,
        "isTemporary" -> BooleanType
      ),
      toTable
    )

  private val functionEncoder: Encoder[Function] =
    byName(
      Seq(
        "name" -> StringType,
        "catalog" -> StringType,
        "namespace" -> strings,
        "description" -> StringType,
        "className" -> StringType,
        "isTemporary" -> BooleanType
      ),
      toFunction
    )

  private val columnEncoder: Encoder[Column] =
    byName(
      Seq(
        "name" -> StringType,
        "description" -> StringType,
        "dataType" -> StringType,
        "nullable" -> BooleanType,
        "isPartition" -> BooleanType,
        "isBucket" -> BooleanType,
        "isCluster" -> BooleanType
      ),
      toColumn
    )

  private val partitionEncoder: Encoder[TablePartition] =
    byName(Seq("partition" -> StringType), r => TablePartition(r.getString(0)))
}
