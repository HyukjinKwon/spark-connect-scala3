# API coverage

`spark-connect-scala3` follows the public Scala API of Apache Spark's own Spark
Connect client, currently the Apache Spark **4.3.0** surface. It speaks the Spark
Connect 4.3.0 protocol and is verified end to end against live Apache Spark 3.5,
4.0, 4.1, 4.2, and 4.3 servers; an API that Apache Spark added in a given release
needs a server at least that new (see
[Version compatibility](troubleshooting.md#version-compatibility)). The plans the
client builds are checked against the ones Apache Spark 4.3.0's client builds for
the same calls. This page records exactly what is and is not implemented so there
are no surprises.

## Supported

| Area | What is covered |
| ---- | --------------- |
| `SparkSession` | builder (`remote`, `config`, `create`/`getOrCreate`), `sql` (commands run eagerly; named, positional, and `java.util.Map` parameters), typed `range`, `table`, `createDataFrame`/`createDataset`, `emptyDataFrame`/`emptyDataset`, `tvf` (table-valued functions), `executeCommand`, `conf`, `catalog`, `read`, `readStream`, `streams`, `pipeline`, `version`, `newSession`/`cloneSession`, tags (`addTag`/`removeTag`/`getTags`/`clearTags`), interrupts (`interruptAll`/`interruptTag`/`interruptOperation`), `time`, `withActive`, active and default session management, `implicits`, `stop`/`close` (releases the server-side session) |
| `Dataset` / `DataFrame` | `select`/`selectExpr` (including typed `select` of up to five `TypedColumn`s), `filter`/`where`, `withColumn(s)`/`withColumnRenamed(s)`/`withMetadata`, `drop`, `join`/`crossJoin`/`lateralJoin`/`joinWith`, `nearestByJoin`, `zip`, `zipWithIndex`, `groupBy`/`rollup`/`cube`/`groupingSets`/`agg`/`pivot`, `orderBy`/`sort`/`sortWithinPartitions`, `limit`/`offset`/`tail`, `distinct`/`dropDuplicates`/`dropDuplicatesWithinWatermark`, `union`/`unionByName`/`intersect(All)`/`except(All)`, `sample`/`randomSplit(AsList)`, `repartition`/`repartitionByRange`/`repartitionById`/`coalesce`, `hint`, `unpivot`/`melt`, `transpose`, `transform`, `toJSON`, `describe`/`summary`, `na`, `stat`, `observe`, `withWatermark`, `checkpoint`/`localCheckpoint`, `persist`/`cache`/`unpersist`/`storageLevel`, subquery expressions (`scalar`/`exists`), `colRegex`/`metadataColumn`, `to(Local)Iterator`, `collect`/`count`/`head`/`take`/`first`/`show`, `schema`/`columns`/`dtypes`/`printSchema`/`explain`/`inputFiles`, `sameSemantics`/`semanticHash`, temp/global-temp view creation. Operators that keep the row shape keep the element type, so a `Dataset[T]` stays a `Dataset[T]` |
| `Column` | full expression algebra: arithmetic, comparison, and boolean operators (plus the Java-friendly `plus`/`equalTo`/`gt`/... aliases), `like`/`rlike`/`ilike`, `contains`/`startsWith`/`endsWith`, `isin`/`isInCollection`/`isin(dataset)`, `between`, `isNull`/`isNotNull`/`isNaN`, `cast`/`try_cast`, `substr`, `getItem`/`getField`/`apply`, `withField`/`dropFields`, `when`/`otherwise`, `over`, `asc`/`desc` with null ordering, `outer`, `transform`, `as[T]` (typed columns), bitwise operators, `explain` |
| `functions` | every built-in function of Apache Spark 4.3.0 except `udf`/`udaf`, with the same overloads: aggregate, string, math, date/time, array/map/struct, JSON/CSV/XML, URL, Variant, sketches (HLL, KLL, Theta, Tuple), geospatial (`st_*`), vectors, partition transforms (`partitioning.*`), higher-order (lambda) functions, `call_function`, `typedLit`, `broadcast`, and window functions |
| `Window` / `WindowSpec` | `partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween` |
| Data sources | `DataFrameReader`/`DataFrameWriter` for CSV, JSON, XML, Parquet, ORC, text, JDBC (including partitioned and predicate reads), and tables, with options, schema, `partitionBy`/`bucketBy`/`sortBy`/`clusterBy`, save modes, `saveAsTable`/`insertInto`, and `withSchemaEvolution`; `textFile` (`Dataset[String]`); parse an in-memory `Dataset[String]` as CSV/JSON/XML; change data capture (`read.changes`); `DataFrameWriterV2` (`writeTo`); `MergeIntoWriter` (`df.mergeInto(...)`, MERGE INTO) |
| SQL | `spark.sql(...)` with named and positional parameters (commands execute immediately, queries stay lazy); temporary and global temporary views; table-valued functions through `spark.tvf` |
| Catalog | `spark.catalog` with typed listings (`Dataset[CatalogMetadata]`, `Dataset[Database]`, `Dataset[Table]`, `Dataset[Column]`, `Dataset[Function]`, `Dataset[TablePartition]`) and patterns: catalogs, databases, tables, views, columns, functions, and partitions; existence checks and getters; `createTable`/`createExternalTable` (eager); `createDatabase`/`dropDatabase`/`dropTable`/`dropView`/`truncateTable`/`analyzeTable`/`getTableProperties`/`getCreateTableString`; temp-view drops; cache management (including a storage level) |
| Structured Streaming | `DataStreamReader` (including `name`, `changes`, `textFile`, `xml`) and `DataStreamWriter` (including `clusterBy`), `OutputMode`, triggers (`ProcessingTime`/`Once`/`AvailableNow`/`Continuous`/`RealTime`), `start`/`toTable`, `StreamingQuery`, `StreamingQueryManager`, and `StreamingQueryListener` registration (`addListener`/`removeListener`/`listListeners`) with client-side event dispatch |
| Declarative Pipelines | build graphs of tables, materialized views, temporary views, sinks, flows, and Auto CDC flows, then run them on the server |
| Plugin extensions | build a `Relation.extension` from a packed protobuf message (`SparkSession.newDataFrame`/`newRelation`, `Dataset.relation`/`plan`) for server-side `RelationPlugin`s such as GraphFrames |
| Typed Datasets | `as[T]`, `createDataset`, `emptyDataset`, typed `range`, typed `select`, `joinWith`, and typed catalog listings, with compile-time `Encoder` derivation for case classes, tuples, primitives (and their boxed Java forms), `java.time` values, `Option`, collections, and maps, plus the `Encoders` factories (`Encoders.product`, `Encoders.tuple`, `Encoders.STRING`, ...); typed actions return `T` |
| Observation | `Observation` for collecting named aggregate metrics while a query runs (`get`/`getAsJava`); errors the server reports for the metrics are raised from `get` |
| Config | `RuntimeConfig` (`spark.conf.get`/`getOption`/`getAll`/`set`/`unset`/`isModifiable`) |
| Types | every Spark SQL type, including `TimeType`, the nanosecond timestamp types, `VariantType`, `GeometryType`/`GeographyType`, `CharType`/`VarcharType`, and user-defined types (decoded as their SQL type); `DataType.sql`/`catalogString`, `StructType.toDDL` |
| Results | Apache Arrow IPC decoding into name-addressable `Row`s (`Row.json`/`prettyJson`, `getGeometry`/`getGeography`, `getJavaMap`, ...); connection-string parsing (`sc://host:port/;k=v`); bearer-token auth (implies TLS); server errors surfaced as typed Spark exceptions (`AnalysisException`/`ParseException`/`SparkException`) with the server message, error class (`getCondition`), message parameters, SQL state, server stack trace, and the operation id |
| Resiliency | automatic retry of transient gRPC failures (exponential backoff + jitter); reattachable execution that resumes a broken `ExecutePlan` result stream via `ReattachExecute` (with `ReleaseExecute` to free server buffers); per-RPC deadlines (`RpcDeadlines`) with automatic reattach on `DEADLINE_EXCEEDED`; gRPC keepalive. All on by default, and working against Spark 3.5+ |
| Operations | an operation id and the thread's tags on every execution, `SparkConnectClient.getOperationId(error)`, and `getOperationStatuses` |

## Not supported

These are deliberately out of scope.

- **User-defined functions (UDFs/UDAFs and typed `Aggregator`s).** Registering
  or applying a Scala closure runs user JVM code on the server, which this client
  does not transport (`functions.udf`/`udaf`, `spark.udf`).
- **Streaming `foreach` / `foreachBatch` sinks.** Same reason: they execute a
  user function per row/batch on the server. All built-in sinks
  (parquet/console/memory/kafka/...) are supported.
- **Typed `Dataset` operations that take a Scala closure.** `as[T]`,
  `createDataset`, typed columns, and `joinWith` are supported (encoder-only, no
  closure), but the operations below run a user function on the server and are
  not available:
    - typed `map` / `flatMap` / `mapPartitions` / `foreach` / `reduce`, and
      `filter(T => Boolean)`
    - `groupByKey` and `KeyValueGroupedDataset` (`mapGroups`, `flatMapGroups`,
      `cogroup`, typed `agg`)
  Use the relational API (`select`/`agg`/`functions`) for these.
- **In-process JVM state.** `SparkContext`, `Dataset.rdd`, `queryExecution`,
  `sessionState`, and `sharedState` exist only inside a classic (non-Connect)
  Spark driver.
- **Artifact upload (`addArtifact(s)`).** Shipping local JARs/files to the server
  is only needed for UDF code.
- **MLlib over Connect** (`spark.ml` equivalents). Not exposed here.
- **Classic-only builder settings.** `Builder.master` and
  `Builder.enableHiveSupport` are accepted for source compatibility but do
  nothing, as in Apache Spark's Connect client.
- **Protocol messages that are not standard Scala API.** A handful of Spark
  Connect relations/commands have no plain Scala DataFrame method because they
  exist for other languages or for closure execution: the closure relations
  (`mapPartitions`, `groupMap`/`coGroupMap`, `applyInPandasWithState`), Python
  user-defined table functions and data sources, the ML relations/commands, the
  pipeline `ExecuteOutputFlows` command, and internal optimization relations
  (server-cached and chunked local relations).

## Differences from Apache Spark's client

- `StreamingQuery.id` and `runId` are `String`s rather than `java.util.UUID`s,
  and `lastProgress` / `recentProgress` return the progress JSON rather than
  `StreamingQueryProgress` objects. `spark.streams.get` accepts either form.
- Local data (`createDataFrame`, `createDataset`) is sent inline in the plan;
  there is no plan compression or artifact-backed chunking, so one local
  collection is bounded by the server's gRPC message size limit (128 MiB by
  default).
- Encoders are derived at compile time for the shapes listed above. The bean,
  Kryo, Java-serialization, and UDT-specific encoders (`Encoders.bean`,
  `Encoders.kryo`, `Encoders.javaSerialization`) are not provided.
- `Row.json` / `prettyJson` render date and timestamp values with their
  `toString` form.

## Why these are excluded

UDFs, `foreach`/`foreachBatch`, and the typed closure operations all require
sending user-compiled JVM code to the server (artifact upload + class loading),
which is a separate, security-sensitive subsystem outside this client's scope.
The encoder-derivation half of the typed `Dataset[T]` API does not require that,
so it is fully supported; only the closure-driven typed operations listed above
remain out of scope.
