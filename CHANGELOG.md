# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-11

The first release of `spark-connect-scala3`: a native Scala 3 client for Apache Spark
Connect. The public API lives under `org.apache.spark.sql.*` and mirrors Apache Spark's
Scala API. It targets the Spark Connect 4.1 protocol and is verified end-to-end against
Apache Spark 3.5 and above Connect servers.

### Added

- **SparkSession** - `builder().remote(...)` with a Spark Connect connection string
  (`token`, `user_id`, `user_agent`, `use_ssl`, `session_id`), builder and runtime
  `config`/`conf`, `sql` (including named and positional parameters), `range`, `table`,
  `createDataFrame`, `version`, `catalog`, `streams`, and `stop`.
- **DataFrame / Dataset** - the full relational API: `select`, `selectExpr`, `filter` /
  `where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `join` (inner / left / right /
  full / semi / anti / cross), `groupBy` / `agg`, `rollup`, `cube`, `pivot`, window
  aggregation, `orderBy` / `sort`, `limit`, `offset`, `distinct`, `dropDuplicates`,
  `union` / `unionByName`, `intersect`, `except`, `unpivot` / `transpose`,
  `repartition` / `coalesce`, `sample` / `randomSplit`, the `na` and `stat` sub-APIs,
  `describe` / `summary`, observations, and typed `Dataset[T]` with case-class encoders.
- **Actions** - `collect`, `collectAsList`, `count`, `take`, `head`, `first`, `show`,
  `isEmpty`, `toLocalIterator`, `foreach`, `foreachPartition`; `printSchema`, `schema`,
  `columns`, `dtypes`, `explain`.
- **Column & functions** - the complete expression DSL and the `functions` object with
  400+ aggregate, math, string, date/time, collection, conditional, and window
  functions. `when` / `otherwise`, `lit`, `expr`, `col`.
- **Window** - `Window.partitionBy(...).orderBy(...)`, `rowsBetween`, `rangeBetween`,
  and `.over(spec)`.
- **Implicits** - `import spark.implicits._` for `$"col"` and `Seq(...).toDF(...)`.
- **I/O** - `DataFrameReader` / `DataFrameWriter` for csv, json, parquet, orc, text,
  jdbc, and `table`; `option(s)`, `schema`, `mode`, `format`, `partitionBy`, `bucketBy`,
  `sortBy`, `saveAsTable`, `insertInto`; and `DataFrameWriterV2` (`writeTo`).
- **Catalog** - databases, tables, columns, and functions listing; current-database
  management; temp and global temp views; table caching.
- **Structured Streaming** - `readStream` / `writeStream`, triggers (processing-time,
  once, available-now, continuous), output modes (append, complete, update), watermarks,
  `StreamingQuery`, and `StreamingQueryManager`.
- **Types** - the full `org.apache.spark.sql.types` hierarchy with protobuf
  round-tripping, `StructType.fromDDL`, and `DataTypes` factories.
- **Results** - Apache Arrow columnar decoding into `org.apache.spark.sql.Row`.
- Transport over gRPC (HTTP/2) via grpc-netty, ScalaPB-generated stubs, and optional TLS
  with bearer-token authentication.
- MkDocs documentation site, Scaladoc, and runnable examples.

### Not supported

- User-defined functions (UDFs / UDAFs / UDTFs) and the `foreach` / `foreachBatch` streaming
  sinks, because they require shipping user JVM closures to the server.

[0.1.0]: https://github.com/HyukjinKwon/spark-connect-scala3/releases/tag/v0.1.0
