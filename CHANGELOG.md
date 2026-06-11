# Changelog

All notable changes to this project are documented in this file. The format is
based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [0.1.0] - 2026-06-11

Initial release of Spark Connect for Scala 3: a pure Scala 3 client for the
Apache Spark Connect gRPC protocol. No local Spark and no JVM Spark installation
is required on the client; it talks to a remote Spark Connect server.

### Added

- `SparkSession` with a builder, `sc://` connection strings, `range`, `sql`
  (named and positional parameters), `createDataFrame`, `table`, `conf`, and
  `version`.
- Lazy `DataFrame`/`Dataset` API: projections, filters, joins
  (inner/outer/left/right/semi/anti/cross), set operations, ordering,
  `limit`/`offset`, `distinct`, sampling, repartitioning, grouping and
  aggregation (`groupBy`/`rollup`/`cube`/`pivot`), `describe`/`summary`, and
  actions (`collect`, `count`, `show`, `head`/`take`/`first`, `toLocalIterator`).
- `Column` expression API and a comprehensive `functions` library: aggregate,
  math, string, datetime, collection, conditional, hashing, JSON/CSV, and window
  functions.
- `DataFrameReader` and `DataFrameWriter` for csv, json, parquet, orc, text,
  table, and jdbc sources.
- `Catalog` API: databases, tables, functions, caching, and temporary views.
- Structured Streaming: `DataStreamReader`, `DataStreamWriter`, `StreamingQuery`,
  and `StreamingQueryManager`.
- Spark Declarative Pipelines (Spark 4.1 and newer).
- `DataFrameNaFunctions`, `DataFrameStatFunctions`, and `Observation` metrics.
- `org.apache.spark.sql.expressions.Window` for window specifications.
- Apache Arrow result decoding and local-data encoding.

### Compatibility

- Built and verified against Apache Spark 4.0.x and 4.1.x. Continuous integration
  runs the integration suite against Spark 3.5.x, 4.0.x, and 4.1.x.
- Declarative Pipelines require a Spark 4.1 or newer server.
- User-defined functions and Structured Streaming `foreach`/`foreachBatch` are not
  supported.
