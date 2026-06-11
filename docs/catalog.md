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
catalog.listCatalogs().show()        // a DataFrame of available catalogs

catalog.currentDatabase              // the active database
catalog.setCurrentDatabase("analytics")
catalog.listDatabases().show()       // a DataFrame of databases
catalog.databaseExists("analytics")  // Boolean
```

## Tables and columns

The listing methods return DataFrames, so you can filter and collect them like
any other query result:

```scala
catalog.listTables().show()
catalog.listTables("analytics").show()

catalog.listColumns("daily_summary").show()
catalog.listColumns("analytics", "daily_summary").show()

catalog.tableExists("daily_summary")               // Boolean
catalog.tableExists("analytics", "daily_summary")  // Boolean
```

## Functions

```scala
catalog.listFunctions().show()
catalog.functionExists("explode")   // Boolean
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
catalog.cacheTable("daily_summary")
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
