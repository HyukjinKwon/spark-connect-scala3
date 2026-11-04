# Changelog

All notable changes to this project are documented in this file. The format is
based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

Brings the client to parity with the Scala Spark Connect client of Apache Spark
4.3.0. Plans built for the same calls are checked against the ones Apache Spark
4.3.0's client builds, call shape by call shape.

### Added

- The Spark Connect 4.3.0 protocol: the protobufs are now sourced from Apache Spark
  4.3.0. Features that a release added need a server at least that new; see
  "Version compatibility" in the troubleshooting guide.
- `functions`: every built-in function of Apache Spark 4.3.0 except `udf`/`udaf`,
  with the same overloads (about 250 new functions and 560 new overloads), including the
  XML, URL, Variant, sketch (HLL, KLL, Theta, Tuple), geospatial (`st_*`), vector,
  and partition-transform (`partitioning.*`) families, `typedLit`, and the Spark
  4.2/4.3 additions such as `collect_union`, `time_bucket`, and the
  `instr(str, substring, start, occurrence)` overloads.
- Typed Datasets: `spark.range` returns `Dataset[java.lang.Long]`; operators that
  keep the row shape (`filter`, `where`, `orderBy`, `limit`, `union`, `sample`,
  `repartition`, `persist`, ...) return `Dataset[T]`; typed `select` of up to five
  `TypedColumn`s (`col("a").as[Int]`); `joinWith`; `tail`; `emptyDataset`; and the
  `Encoders` factories (`Encoders.product`, `Encoders.tuple`, `Encoders.STRING`,
  boxed and `java.time` encoders, ...).
- `Dataset`: `zip` (Spark 4.3+), `zipWithIndex` and `nearestByJoin` (Spark 4.2+),
  subquery expressions `scalar()` / `exists()` and `Column.isin(dataset)` (Spark
  4.0+), `groupingSets`, `repartitionById`, `withColumns(Map)`, `withMetadata`,
  `metadataColumn`, `randomSplitAsList`, and the remaining overloads of `select`,
  `agg`, `join`, `drop`, `dropDuplicates`, `cube`, `rollup`, `observe`, and friends.
- `Column`: `try_cast`, `withField` / `dropFields`, `isInCollection`, `outer`,
  `transform`, `explain`, `!==`, the Java-friendly aliases (`plus`, `minus`,
  `multiply`, `divide`, `mod`, `equalTo`, `gt`, `lt`, `geq`, `leq`), and the
  `ColumnName` field constructors (`$"a".int`, `.string`, `.struct(...)`, ...).
- `SparkSession`: `tvf` (table-valued functions), `executeCommand`, `cloneSession`
  (Spark 4.1+), `time`, `withActive`, `active`, default-session management,
  `sql` with a `java.util.Map` of arguments, `Builder.config(Map)`, and the
  deprecated no-op `Builder.master` / `Builder.enableHiveSupport`. The tag and
  interrupt methods (`addTag`, `removeTag`, `getTags`, `clearTags`,
  `interruptAll`, `interruptTag`, `interruptOperation`), which the API coverage
  page already listed, are now public.
- `Catalog`: typed listings with pattern overloads, `cacheTable` with a storage
  level, the remaining `createTable` / `createExternalTable` overloads, and (Spark
  4.2+) `createDatabase`, `dropDatabase`, `dropTable`, `dropView`, `listViews`,
  `listPartitions`, `getTableProperties`, `getCreateTableString`, `truncateTable`,
  and `analyzeTable`.
- Readers and writers: `read.changes` (change data capture, Spark 4.2+), XML files
  and `read.xml(Dataset[String])` (Spark 4.2+), `jdbc` overloads (partitioned and
  predicate reads), `options(java.util.Map)`, and `write.clusterBy`,
  `write.withSchemaEvolution`, `write.xml`, `write.jdbc`, and
  `writeTo(...).withSchemaEvolution` (Spark 4.2+).
- Structured Streaming: `readStream.name` (Spark 4.2+), `changes`, `textFile`, and
  `xml`; `writeStream.clusterBy` and `outputMode(OutputMode)` with an `OutputMode`
  object; `Trigger.RealTime` (Spark 4.2+) and the `Long` / `TimeUnit` / `Duration`
  trigger overloads; `StreamingQuery.sparkSession`; `StreamingQueryManager.get(UUID)`.
- Declarative Pipelines: `createAutoCdcFlow` (Spark 4.2+; SCD type 2 and history
  tracking need Spark 4.3+).
- Types: `TimeType`, `TimestampNTZNanosType` / `TimestampLTZNanosType` (Spark 4.3+),
  `GeometryType` / `GeographyType` with `Geometry` / `Geography` values,
  `StructType.toDDL`, and `StructField.sql` / `toDDL`. Results decode `TIME`,
  nanosecond timestamps, and geospatial values.
- `Row`: `json`, `prettyJson`, `getJavaMap`, `getGeometry`, `getGeography`,
  `Row.fromTuple`, and `Row.merge`. `RuntimeConfig.isModifiable`, which the API
  coverage page already listed, is now implemented.
- Errors carry their message parameters and SQL state (`getMessageParameters`,
  `getSqlState`, `getCondition` on `SparkException`), and the operation id of the
  failed execution (`SparkConnectClient.getOperationId(error)`); `Observation.get`
  raises the error the server reports for the metrics (Spark 4.2+).
- Client: `getOperationStatuses` (Spark 4.2+), per-RPC deadlines (`RpcDeadlines`)
  with automatic reattach on `DEADLINE_EXCEEDED`, a bound on reattach retries
  (`maxRetryExceptionElapsedTime`), and gRPC keepalive with the
  `grpc_keepalive_*` connection-string parameters.

### Changed

- `spark.sql` runs commands (DDL/DML such as `CREATE`, `INSERT`, `DROP`) as soon as
  it is called, as Apache Spark does; previously nothing ran until an action.
  Queries are still lazy. `Catalog.createTable` now also creates the table right
  away.
- `spark.range` returns `Dataset[java.lang.Long]`, the shape-preserving operators
  of a `Dataset[T]` return `Dataset[T]`, the catalog listings return typed
  Datasets (`Dataset[Table]`, ...), and `read.textFile` returns `Dataset[String]`,
  all as in Apache Spark. Call `.toDF()` where a `DataFrame` is needed.
- `rand()`, `randn()`, `uuid()`, and `shuffle(e)` pick their seed on the client,
  like Apache Spark's client, so re-running a DataFrame returns the same values.
  Spark 3.5 servers reject the seeded `uuid()` / `shuffle(e)`; use
  `expr("uuid()")` there.
- Expressions are encoded exactly as Apache Spark's client encodes them (for
  example `===` as `=`, `=!=` as `!(=)`, `!` as `!`, `bitwise_not` as `~`,
  `isnan` as `isNaN`, `first` / `last` with an
  explicit `ignoreNulls`, `lag` / `lead` / `nth_value` with explicit defaults,
  `round` / `bround` with scale 0, `window` with explicit slide and start times,
  and `unix_timestamp()` over `current_timestamp()`).
- Every execution carries an operation id and the current thread's tags, and
  closing a session releases it on the server.
- The client sends gRPC keepalive PINGs by default (60s interval), as Apache Spark
  4.3's client does; set `grpc_keepalive_enabled=false` to opt out.
- `NullType` is named `void`, as in Apache Spark.
- The Spark Connect integration matrix now runs against Spark 3.5.9, 4.0.5, 4.1.4,
  4.2.1, and 4.3.0.

### Fixed

- `product(e)` failed on Spark 4.0+ servers; it is now sent as an internal function.
- `call_function` marks the function as user-defined, so qualified names such as
  `call_function("catalog.db.fn", ...)` resolve.
- `read.schema(...).csv(ds)` and `.json(ds)` silently dropped the schema; it is now
  sent, with a DDL string resolved by the server.
- `DataType.sql` upper-cased struct field names; it now matches Apache Spark,
  quoting field names only when needed.

## [0.3.0] - 2026-07-07

### Added

- Public entry points for Spark Connect plugin authors (e.g. GraphFrames) to build
  `Relation.extension` plans and turn them into `DataFrame`s, mirroring the PySpark
  client's `plan.extension.Pack(msg)` / `DataFrame(plan, session)`:
  `SparkSession.newDataFrame` (a `google.protobuf.Any` overload and a raw
  `Relation.RelType` overload), `SparkSession.newRelation`, and `Dataset.relation` /
  `Dataset.plan` for extracting the built plan. See the `RelationExtension` example.

## [0.2.0] - 2026-06-15

### Fixed

- `YearMonthIntervalType` and `DayTimeIntervalType` now render their actual field
  range (e.g. `interval month`, `interval hour to minute`) instead of always
  reporting `interval year to month` / `interval day to second`.
- `DecimalType` now carries its precision and scale through `typeName`, so
  `printSchema` / `StructType.treeString` show `decimal(p,s)` rather than a bare
  `decimal`.
- `Observation`'s single-use check is now per-instance rather than keyed on a
  process-global name registry, so a fresh `Observation` may reuse a name that was
  already seen elsewhere in the JVM (for example, when re-running the same query).

### Changed

- Expanded the hermetic (server-free) unit test suite covering the type hierarchy,
  metadata, encoders, `Row`, window frames, offline plan building, and the
  multi-argument `functions` entry points, substantially raising client test
  coverage.

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
