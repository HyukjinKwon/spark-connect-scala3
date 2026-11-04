# Catalog

The catalog exposes the server's metadata: catalogs, databases, tables,
columns, and functions, plus management of temporary views and the table cache.
It mirrors Apache Spark's `Catalog` API and is reached through
`spark.catalog`.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
val catalog = spark.catalog
```

## Catalogs and databases

```scala
catalog.currentCatalog()             // the active catalog name
catalog.setCurrentCatalog("spark_catalog")
catalog.listCatalogs().show()        // a Dataset[CatalogMetadata]

catalog.currentDatabase              // the active database
catalog.setCurrentDatabase("analytics")
catalog.listDatabases().show()       // a Dataset[Database]
catalog.listDatabases("ana*").show() // filtered by a pattern
catalog.databaseExists("analytics")  // Boolean
catalog.getDatabase("analytics")     // Database

// Spark 4.2+ servers.
catalog.createDatabase("analytics", ifNotExists = true)
catalog.createDatabase("staging", ifNotExists = true, Map("comment" -> "scratch"))
catalog.dropDatabase("staging", ifExists = true, cascade = true)
```

## Tables and columns

The listing methods return typed Datasets (`Dataset[Table]`, `Dataset[Column]`,
`Dataset[Function]`, ...), as in Apache Spark, so you can transform them on the
server or collect them into the catalog case classes:

```scala
import org.apache.spark.sql.catalog.Table

val tables: Array[Table] = catalog.listTables().collect()
tables.map(_.name)

catalog.listTables("analytics").show()
catalog.listTables("analytics", "daily_*").show()

catalog.listColumns("daily_summary").show()
catalog.listColumns("analytics", "daily_summary").show()

catalog.tableExists("daily_summary")               // Boolean
catalog.tableExists("analytics", "daily_summary")  // Boolean
catalog.getTable("analytics", "daily_summary")     // Table
```

## Creating and dropping tables

`createTable` runs immediately and returns a DataFrame over the new table:

```scala
import org.apache.spark.sql.types.*

val schema = new StructType().add("id", LongType).add("name", StringType)
catalog.createTable("people", "parquet", schema, Map.empty[String, String])
catalog.createTable("events", "/data/events", "parquet")  // over existing files

// Spark 4.2+ servers.
catalog.dropTable("people", ifExists = true)
catalog.dropView("people_view", ifExists = true)
```

## Table details (Spark 4.2+)

```scala
catalog.listViews("analytics").show()                // Dataset[Table]
catalog.listPartitions("analytics.sales").show()     // Dataset[TablePartition]
catalog.getTableProperties("analytics.sales")        // java.util.Map[String, String]
catalog.getCreateTableString("analytics.sales")      // the CREATE TABLE statement
catalog.truncateTable("analytics.staging")           // delete all rows
catalog.analyzeTable("analytics.sales", noScan = true)  // compute table statistics
```

## Functions

```scala
catalog.listFunctions().show()                 // Dataset[Function]
catalog.listFunctions("default", "array_*").show()
catalog.functionExists("explode")   // Boolean
catalog.getFunction("explode")      // Function
```

## Temporary views

```scala
spark.range(10).createOrReplaceTempView("t")

catalog.dropTempView("t")           // returns true if a view was dropped
catalog.dropGlobalTempView("g")
```

## Caching

Cache tables and views in cluster memory, and manage the cache:

```scala
import org.apache.spark.storage.StorageLevel

catalog.cacheTable("daily_summary")
catalog.cacheTable("daily_summary", StorageLevel.MEMORY_ONLY)
catalog.isCached("daily_summary")    // Boolean
catalog.uncacheTable("daily_summary")
catalog.clearCache()
```

## Maintenance

```scala
catalog.refreshTable("daily_summary")  // refresh cached metadata and data
catalog.refreshByPath("/data/sales")   // refresh everything under a path
catalog.recoverPartitions("sales")     // re-discover partitions on disk
```
