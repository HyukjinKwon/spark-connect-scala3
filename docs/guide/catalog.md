# Catalog

The `Catalog` is the entry point for inspecting and managing metadata: catalogs,
databases, tables, columns, and functions, plus the query cache and temporary views.
Obtain it from `SparkSession.catalog`. The API mirrors Apache Spark's `spark.catalog`.

Methods that list metadata return a `DataFrame`; predicate methods (`*Exists`,
`isCached`) return `Boolean`; and management methods (`cacheTable`,
`setCurrentDatabase`, ...) return `Unit`.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().remote("sc://localhost:15002").getOrCreate()
val cat = spark.catalog
```

## Catalogs and databases

```scala
cat.currentCatalog()              // "spark_catalog"
cat.listCatalogs().show()
cat.setCurrentCatalog("spark_catalog")

cat.currentDatabase               // "default"
cat.listDatabases().show()
cat.databaseExists("default")     // true
cat.setCurrentDatabase("default")
```

Each row carries the metadata fields the server returns (for example a database row has
`name`, `catalog`, `description`, and `locationUri`). You can list with a name pattern:

```scala
cat.listDatabases("analyt*").show()
```

## Tables, columns, and functions

```scala
// All tables and views in the current database, or in a named database.
cat.listTables().show()
cat.listTables("default").show()

cat.tableExists("my_table")             // true / false
cat.tableExists("default", "my_table")  // true / false

// Columns of a table.
cat.listColumns("my_table").show()
cat.listColumns("default", "my_table").show()

// Registered functions.
cat.listFunctions().show()
cat.functionExists("explode")           // true
```

## Temporary views

Register a temporary view with a SQL `CREATE` statement or
`DataFrame.createOrReplaceTempView`, query it through `spark.sql` or `spark.table`, then
drop it through the catalog. The `drop*` methods return a `Boolean` indicating whether a
view was removed.

```scala
// Create a session-local temp view.
spark.sql("CREATE OR REPLACE TEMPORARY VIEW nums AS SELECT * FROM range(5)")
spark.table("nums").show()
spark.sql("SELECT id * id AS sq FROM nums").show()

// Create a global temp view (lives in the global_temp database).
spark.sql("CREATE GLOBAL TEMPORARY VIEW global_nums AS SELECT * FROM range(5)")
spark.table("global_temp.global_nums").show()

// Drop views through the catalog.
cat.dropTempView("nums")               // true
cat.dropGlobalTempView("global_nums")  // true
```

## Cache management

Spark can cache a table or view in memory so repeated queries avoid recomputing it.

```scala
cat.cacheTable("my_table")
cat.isCached("my_table")     // true

// ... run queries that benefit from the cache ...

cat.uncacheTable("my_table")
cat.clearCache()             // drop every cached table at once
```

## Refresh and partition recovery

After data files change underneath a table, refresh its cached metadata. For partitioned
tables whose partitions were added out of band, recover them so the catalog sees the new
partition directories.

```scala
cat.refreshTable("my_table")
cat.refreshByPath("/data/my_table")
cat.recoverPartitions("my_partitioned_table")
```

## Method summary

| Method | Returns | Purpose |
|--------|---------|---------|
| `currentCatalog()` | `String` | active catalog name |
| `setCurrentCatalog(name)` | `Unit` | switch catalog |
| `listCatalogs()` | `DataFrame` | all catalogs |
| `currentDatabase` | `String` | active database name |
| `setCurrentDatabase(name)` | `Unit` | switch database |
| `listDatabases()` | `DataFrame` | all databases |
| `databaseExists(db)` | `Boolean` | database presence |
| `listTables(db?)` | `DataFrame` | tables and views |
| `tableExists(name)` / `tableExists(db, name)` | `Boolean` | table/view presence |
| `listColumns(table)` / `listColumns(db, table)` | `DataFrame` | a table's columns |
| `listFunctions(db?)` | `DataFrame` | registered functions |
| `functionExists(name)` | `Boolean` | function presence |
| `dropTempView(name)` | `Boolean` | drop a session temp view |
| `dropGlobalTempView(name)` | `Boolean` | drop a global temp view |
| `isCached(table)` | `Boolean` | is the table cached |
| `cacheTable(table)` | `Unit` | cache in memory |
| `uncacheTable(table)` | `Unit` | remove from cache |
| `clearCache()` | `Unit` | clear all cached tables |
| `refreshTable(table)` | `Unit` | refresh cached metadata |
| `refreshByPath(path)` | `Unit` | refresh by storage path |
| `recoverPartitions(table)` | `Unit` | rediscover partitions |

## See also

- [Configuration](../reference/configuration.md) for `spark.conf` and connection setup.
- [DataFrame & Dataset](dataframe.md) for working with the data behind these tables.
- [Reading & Writing Data](io.md) for loading and saving tables.
