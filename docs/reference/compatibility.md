# Supported Features

`spark-connect-scala3` implements the Spark Connect DataFrame, SQL, and Structured
Streaming surface. The public API mirrors Apache Spark's Scala API, so familiar code
ports unchanged.

## Platform

| | Supported |
|---|---|
| Scala | 3.3.x (LTS) |
| JDK | 17, 21 |
| Protocol | Spark Connect **4.1** |
| Spark Connect servers | Apache Spark **3.5 and above** |
| Transport | gRPC over HTTP/2, optional TLS + bearer token |

## SparkSession

- `builder().remote(...)`, `config`, `getOrCreate`
- `sql(...)` and parameterised `sql(...)` (named and positional parameters)
- `range(...)`
- `table(...)`
- `createDataFrame(rows, schema)`, `toDF`, `Seq(...).toDF(...)` via implicits
- `conf.get` / `conf.set`, `version`, `stop`
- `catalog`
- `streams` (the `StreamingQueryManager`)

## DataFrame / Dataset

- Projection: `select`, `selectExpr`, `withColumn`, `withColumns`, `withColumnRenamed`,
  `drop`
- Filtering: `filter` / `where`
- Aggregation: `groupBy` / `agg`, `RelationalGroupedDataset`, `rollup`, `cube`, `pivot`
- Ordering and limiting: `orderBy` / `sort`, `limit`, `offset`
- De-duplication: `distinct`, `dropDuplicates`, `dropDuplicatesWithinWatermark`
- Joins: inner / left / right / full outer / semi / anti / cross
- Set operations: `union` / `unionByName`, `intersect`, `except`
- Reshaping: `unpivot` / `melt`, `transpose`
- Partitioning: `repartition`, `repartitionByRange`, `coalesce`
- Sampling: `sample`, `randomSplit`
- Null / stat helpers: `na` (`drop`, `fill`, `replace`), `stat` (`approxQuantile`,
  `cov`, `corr`, `crosstab`, `freqItems`, `sampleBy`), `describe`, `summary`
- Hints, watermarks, observations
- Actions: `collect`, `collectAsList`, `count`, `take`, `head`, `first`, `show`,
  `isEmpty`, `toLocalIterator`, `foreach`, `foreachPartition`
- Inspection: `printSchema`, `schema`, `columns`, `dtypes`, `explain`
- Typed `Dataset[T]` with case-class encoders

## Column & functions

- Expression DSL: arithmetic, comparison, boolean, null checks, `isin`, `like`,
  `rlike`, `between`, `cast`, alias, sort order, `getItem`, `getField`, struct / array /
  map access
- `when` / `otherwise`, `lit`, `expr`, `col`
- The `functions` object: 400+ aggregate, math, string, date/time, collection,
  conditional, and window functions
- Window functions via `Window` and `.over(spec)`

## Data sources

- `read` / `write` for csv, json, parquet, orc, text, jdbc, and `table`
- `option(s)`, `schema`, `mode`, `format`
- `partitionBy`, `bucketBy`, `sortBy`
- `saveAsTable`, `insertInto`
- `DataFrameWriterV2` (`writeTo(...)`)

## Catalog

- Databases, tables, columns, and functions listing
- `currentDatabase` / `setCurrentDatabase`, `tableExists`, `functionExists`
- Temp and global temp views
- Caching: `cacheTable`, `uncacheTable`, `clearCache`, `isCached`

## Structured Streaming

- `readStream` / `writeStream`
- Triggers: processing-time, once, available-now, continuous
- Output modes: append, complete, update
- Watermarks
- `StreamingQuery` (status, progress, `awaitTermination`, `stop`, ...)
- `StreamingQueryManager` (`active`, `get`, `awaitAnyTermination`, `resetTerminated`)

See the [Structured Streaming guide](../guide/streaming.md).

## Types

- The full `org.apache.spark.sql.types` hierarchy
- Protobuf <-> `DataType` round-trip
- `StructType.fromDDL`, `DataTypes` factories

## Not supported

Two parts of the Spark API require shipping user JVM closures to the server, which the
Spark Connect protocol does not transport, so they are out of scope:

- User-defined functions (UDFs / UDAFs / UDTFs).
- The `foreach` / `foreachBatch` sinks in Structured Streaming.

Everything else in the Spark Connect surface is implemented. The
[Scaladoc](https://hyukjinkwon.github.io/spark-connect-scala3/api/) documents every
method and signature for a given release.
