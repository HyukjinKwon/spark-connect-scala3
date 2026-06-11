# API coverage

`spark-connect-scala3` implements the mainstream, untyped Spark Connect surface
completely, and is verified end to end against live Apache Spark 3.5, 4.0, and
4.1 servers. This page records exactly what is and is not implemented so there
are no surprises.

## Supported

| Area | What is covered |
| ---- | --------------- |
| `SparkSession` | builder + `remote(...)`, `sql`, `range`, `table`, `createDataFrame`, `conf`, `catalog`, `read`, `readStream`, `streams`, `version`, `addTag`/`interrupt*`, `implicits` |
| `Dataset` / `DataFrame` | `select`/`selectExpr`, `filter`/`where`, `withColumn(s)`/`withColumnRenamed(s)`, `drop`, `join` (inner/left/right/outer/semi/anti/cross), `groupBy`/`rollup`/`cube`/`agg`/`pivot`, `orderBy`/`sort`/`sortWithinPartitions`, `limit`/`offset`, `distinct`/`dropDuplicates`, `union`/`unionByName`/`intersect(All)`/`except(All)`, `sample`/`randomSplit`, `repartition`/`repartitionByRange`/`coalesce`, `hint`, `unpivot`/`melt`, `transpose`, `toJSON`, `describe`/`summary`, `na`, `stat`, `observe`, `withWatermark`, `checkpoint`/`localCheckpoint`, `persist`/`cache`/`unpersist`/`storageLevel`, `to(Local)Iterator`, `collect`/`count`/`head`/`take`/`first`/`show`, `schema`/`columns`/`dtypes`/`printSchema`/`explain`/`inputFiles`, `sameSemantics`/`semanticHash`, temp/global-temp view creation |
| `Column` | full expression algebra: arithmetic, comparison, boolean logic, `like`/`rlike`/`ilike`, `contains`/`startsWith`/`endsWith`, `isin`, `between`, `isNull`/`isNotNull`/`isNaN`, `cast`, `substr`, `getItem`/`getField`, `when`/`otherwise`, `over`, `asc`/`desc`, bitwise ops |
| `functions` | 400+ functions: aggregate, string, math, date/time, array/map/struct, JSON, conditional, and window functions |
| `Window` / `WindowSpec` | `partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween` |
| Data sources | `DataFrameReader`/`DataFrameWriter` for CSV, JSON, Parquet, ORC, text, JDBC, and tables, with options, schema, `partitionBy`/`bucketBy`/`sortBy`, save modes, `saveAsTable`/`insertInto`; `DataFrameWriterV2` (`writeTo`) |
| SQL | `spark.sql(...)` with named and positional parameters; temporary and global temporary views |
| Catalog | `spark.catalog`: list/inspect catalogs, databases, tables, columns, functions; existence checks; `createTable`/`createExternalTable`; temp-view drops; cache management |
| Structured Streaming | `DataStreamReader`/`DataStreamWriter`, output modes, triggers (`ProcessingTime`/`Once`/`AvailableNow`/`Continuous`), `start`/`toTable`, `StreamingQuery`, `StreamingQueryManager` |
| Declarative Pipelines | build graphs of tables, materialized views, temporary views, sinks, and flows, then run them on the server |
| Observation | `Observation` for collecting named aggregate metrics while a query runs |
| Config | `RuntimeConfig` (`spark.conf.get`/`set`/`unset`/`isModifiable`) |
| Results | Apache Arrow IPC decoding into name-addressable `Row`s; connection-string parsing (`sc://host:port/;k=v`), bearer-token auth (implies TLS), and retry on transient gRPC errors |

## Not supported

These are deliberately out of scope for now.

- **User-defined functions (UDFs/UDAFs).** Registering or applying a Scala
  closure runs user JVM code on the server, which the Spark Connect protocol
  does not transport for this client.
- **Streaming `foreach` / `foreachBatch` sinks.** Same reason: they execute a
  user function per row/batch on the server. All built-in sinks
  (parquet/console/memory/kafka/...) are supported.
- **The typed `Dataset[T]` / `Encoder` API.** `Dataset` is effectively untyped
  (`DataFrame = Dataset[Row]`). There is no `Encoder` derivation, so the
  following are not available:
    - `as[T]` to a case class, and typed `collect[T]`/`map`/`flatMap`/`reduce`
    - `groupByKey` and `KeyValueGroupedDataset` (`mapGroups`, `flatMapGroups`,
      `cogroup`, typed `agg`)
    - typed `Aggregator`s
  Use the relational API (`select`/`agg`/`functions`) and read results as `Row`.
- **MLlib over Connect** (`spark.ml` / `pyspark.ml` equivalent). Experimental
  upstream and not exposed here.
- **A few niche / advanced APIs**, including artifact upload (`addArtifact`),
  `MergeIntoWriter`, query/streaming listener registration, and some
  less-common method overloads and protocol relations/commands.

## Why these are excluded

UDFs, `foreach`/`foreachBatch`, and the typed closure operations all require
sending user-compiled JVM code to the server (artifact upload + class loading),
which is a separate, security-sensitive subsystem outside this client's scope.
The encoder-derivation half of the typed `Dataset[T]` API does not require that,
and is the most likely candidate for a future release; everything else above is
fully supported today.
