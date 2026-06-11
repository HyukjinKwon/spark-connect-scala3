# API coverage

`spark-connect-scala3` implements the mainstream, untyped Spark Connect surface
completely, and is verified end to end against live Apache Spark 3.5, 4.0, and
4.1 servers. This page records exactly what is and is not implemented so there
are no surprises.

## Supported

| Area | What is covered |
| ---- | --------------- |
| `SparkSession` | builder + `remote(...)`, `sql`, `range`, `table`, `createDataFrame`, `conf`, `catalog`, `read`, `readStream`, `streams`, `version`, `addTag`/`interrupt*`, `implicits` |
| `Dataset` / `DataFrame` | `select`/`selectExpr`, `filter`/`where`, `withColumn(s)`/`withColumnRenamed(s)`, `drop`, `join`/`crossJoin`/`lateralJoin` (inner/left/right/outer/semi/anti/cross), `groupBy`/`rollup`/`cube`/`agg`/`pivot`, `orderBy`/`sort`/`sortWithinPartitions`, `limit`/`offset`, `distinct`/`dropDuplicates`, `union`/`unionByName`/`intersect(All)`/`except(All)`, `sample`/`randomSplit`, `repartition`/`repartitionByRange`/`coalesce`, `hint`, `unpivot`/`melt`, `transpose`, `toJSON`, `describe`/`summary`, `na`, `stat`, `observe`, `withWatermark`, `checkpoint`/`localCheckpoint`, `persist`/`cache`/`unpersist`/`storageLevel`, `to(Local)Iterator`, `collect`/`count`/`head`/`take`/`first`/`show`, `schema`/`columns`/`dtypes`/`printSchema`/`explain`/`inputFiles`, `sameSemantics`/`semanticHash`, temp/global-temp view creation |
| `Column` | full expression algebra: arithmetic, comparison, boolean logic, `like`/`rlike`/`ilike`, `contains`/`startsWith`/`endsWith`, `isin`, `between`, `isNull`/`isNotNull`/`isNaN`, `cast`, `substr`, `getItem`/`getField`, `when`/`otherwise`, `over`, `asc`/`desc`, bitwise ops |
| `functions` | 400+ functions: aggregate, string, math, date/time, array/map/struct, JSON, conditional, and window functions |
| `Window` / `WindowSpec` | `partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween` |
| Data sources | `DataFrameReader`/`DataFrameWriter` for CSV, JSON, Parquet, ORC, text, JDBC, and tables, with options, schema, `partitionBy`/`bucketBy`/`sortBy`, save modes, `saveAsTable`/`insertInto`; parse an in-memory `Dataset[String]` as CSV/JSON (`read.csv(ds)`/`read.json(ds)`); `DataFrameWriterV2` (`writeTo`); `MergeIntoWriter` (`df.mergeInto(...)`, MERGE INTO) |
| SQL | `spark.sql(...)` with named and positional parameters; temporary and global temporary views |
| Catalog | `spark.catalog`: list/inspect catalogs, databases, tables, columns, functions; existence checks; `createTable`/`createExternalTable`; temp-view drops; cache management |
| Structured Streaming | `DataStreamReader`/`DataStreamWriter`, output modes, triggers (`ProcessingTime`/`Once`/`AvailableNow`/`Continuous`), `start`/`toTable`, `StreamingQuery`, `StreamingQueryManager`, and `StreamingQueryListener` registration (`addListener`/`removeListener`/`listListeners`) with client-side event dispatch |
| Declarative Pipelines | build graphs of tables, materialized views, temporary views, sinks, and flows, then run them on the server |
| Typed Datasets | `as[T]` and `spark.createDataset` with compile-time `Encoder` derivation for case classes, tuples, primitives, `Option`, collections, and maps; typed actions (`collect`/`head`/`first`/`take`/`collectAsList`/`toLocalIterator`) |
| Observation | `Observation` for collecting named aggregate metrics while a query runs |
| Config | `RuntimeConfig` (`spark.conf.get`/`set`/`unset`/`isModifiable`) |
| Results | Apache Arrow IPC decoding into name-addressable `Row`s; connection-string parsing (`sc://host:port/;k=v`); bearer-token auth (implies TLS); server errors surfaced as typed Spark exceptions (AnalysisException/ParseException/SparkException) with the original message and error class |

## Not supported

These are deliberately out of scope for now.

- **User-defined functions (UDFs/UDAFs).** Registering or applying a Scala
  closure runs user JVM code on the server, which the Spark Connect protocol
  does not transport for this client.
- **Streaming `foreach` / `foreachBatch` sinks.** Same reason: they execute a
  user function per row/batch on the server. All built-in sinks
  (parquet/console/memory/kafka/...) are supported.
- **Typed `Dataset` operations that take a Scala closure.** `as[T]` and
  `createDataset` are supported (encoder-only, no closure), but the operations
  below run a user function on the server and are not available:
    - typed `map` / `flatMap` / `mapPartitions` / `reduce`
    - `groupByKey` and `KeyValueGroupedDataset` (`mapGroups`, `flatMapGroups`,
      `cogroup`, typed `agg`)
    - typed `Aggregator`s
  Use the relational API (`select`/`agg`/`functions`) for these.
- **MLlib over Connect** (`spark.ml` / `pyspark.ml` equivalent). Experimental
  upstream and not exposed here.
- **Artifact upload (`addArtifact`).** Shipping local JARs/files to the server
  is a niche client feature not exposed yet.
- **Protocol messages that are not standard Scala API.** A handful of Spark
  Connect relations/commands have no plain Scala DataFrame method because they
  exist for other languages or for closure execution: the closure relations
  (`mapPartitions`, `groupMap`/`coGroupMap`, `applyInPandasWithState`), Python
  user-defined table functions and data sources, the ML relation/commands, and
  internal optimization relations (server-cached/chunked local relations,
  `withRelations`). The standard DataFrame, SQL, streaming, and pipeline surface
  is fully covered.

## Why these are excluded

UDFs, `foreach`/`foreachBatch`, and the typed closure operations all require
sending user-compiled JVM code to the server (artifact upload + class loading),
which is a separate, security-sensitive subsystem outside this client's scope.
The encoder-derivation half of the typed `Dataset[T]` API does not require that, so
`as[T]` and `createDataset` are fully supported; only the closure-driven typed
operations listed above remain out of scope.

## Known limitations

- **No automatic retry or reattachable execution.** If the gRPC connection drops
  mid-query, the error is surfaced to the caller rather than transparently retried
  or resumed. Wrap calls in your own retry logic if you need it.
- **Server stack traces are not reconstructed.** Errors carry the server's message
  and error class (and map to `AnalysisException` / `ParseException` /
  `SparkException`), but the remote JVM stack trace is not rebuilt on the client.
