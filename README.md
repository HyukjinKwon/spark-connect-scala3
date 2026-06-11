# Spark Connect for Scala 3

[![CI](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.hyukjinkwon/spark-connect-scala3-client_3.svg)](https://central.sonatype.com/artifact/io.github.hyukjinkwon/spark-connect-scala3-client_3)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://hyukjinkwon.github.io/spark-connect-scala3/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

A pure-Scala-3 client for **[Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)** - the gRPC-based, decoupled client/server protocol for Apache Spark.

`spark-connect-scala3` lets you build and run Spark DataFrame queries from Scala 3 against a remote Spark cluster, with an API that mirrors Apache Spark's own Scala DataFrame API. No local Spark installation, no `spark-submit`, no JVM Spark on the client - just a gRPC connection to a Spark Connect server.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder
  .remote("sc://localhost:15002")
  .appName("quickstart")
  .getOrCreate()

spark.range(1, 1000)
  .select(col("id"), (col("id") % 3).as("bucket"))
  .groupBy("bucket")
  .agg(count("*").as("n"), sum("id").as("total"))
  .orderBy("bucket")
  .show()

spark.stop()
```

```
+------+---+------+
|bucket|  n| total|
+------+---+------+
|     0|333|166833|
|     1|333|166167|
|     2|333|166500|
+------+---+------+
```

## Installation

The current release is `0.1.0`, published for **Scala 3.3.x** (the Scala 3 LTS line), running on **JDK 17 or newer**.

sbt (`build.sbt`):

```scala
libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3-client" % "0.1.0"
```

Maven (`pom.xml`) - Scala 3 artifacts use the `_3` suffix:

```xml
<dependency>
  <groupId>io.github.hyukjinkwon</groupId>
  <artifactId>spark-connect-scala3-client_3</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle (`build.gradle.kts`):

```kotlin
implementation("io.github.hyukjinkwon:spark-connect-scala3-client_3:0.1.0")
```

See the [Installation guide](https://hyukjinkwon.github.io/spark-connect-scala3/installation/) for Mill, the JVM flags Apache Arrow needs on JDK 17+, and how to start a Spark Connect server.

## Starting a Spark Connect server

You need a reachable Spark Connect server. The client is built and tested against **Apache Spark 4.0.x and 4.1.x** (latest 4.1.2); the protobufs are sourced from Spark 4.1.2.

```bash
# Download a Spark distribution (4.1.2 shown here).
curl -L https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz -o spark.tgz
tar xzf spark.tgz
cd spark-4.1.2-bin-hadoop3

# Start the Connect server (requires Java 17+).
# Spark 4.0+ bundles the Connect server, so no extra packages are needed.
./sbin/start-connect-server.sh
```

The server listens on `sc://localhost:15002` by default.

On **Spark 3.5.x** the Connect server is not bundled. Use a Scala 2.13 distribution and pull the server in explicitly:

```bash
./sbin/start-connect-server.sh --packages "org.apache.spark:spark-connect_2.13:3.5.5"
```

## What it supports

`spark-connect-scala3` implements the Spark Connect DataFrame, SQL, Structured Streaming, and Declarative Pipelines surface, modeled directly on Apache Spark's Scala API:

- **DataFrame operations**: `select`, `selectExpr`, `filter`/`where`, `withColumn`, `withColumnRenamed`, `drop`, `join` (inner/left/right/outer/semi/anti/cross), `groupBy`/`rollup`/`cube`/`agg`, `orderBy`/`sort`, `limit`/`offset`, `distinct`/`dropDuplicates`, `union`/`unionByName`/`intersect`/`except`, `sample`, `repartition`/`coalesce`, and `hint`.
- **Columns and functions**: a full `Column` expression algebra (arithmetic, comparison, boolean logic, `like`/`rlike`, `isin`, `between`, `cast`, `when`/`otherwise`, `over`) and a large `org.apache.spark.sql.functions` library (aggregates, string, math, date/time, array/map, JSON, and window functions).
- **Data sources**: read and write CSV, JSON, Parquet, ORC, and text through `DataFrameReader`/`DataFrameWriter`, including format options, schemas, partitioning, bucketing, and save modes.
- **SQL**: `spark.sql(...)` with named and positional parameters, plus temporary and global temporary views.
- **Catalog**: list and inspect catalogs, databases, tables, columns, and functions; manage temp views and the table cache via `spark.catalog`.
- **Structured Streaming**: streaming sources and sinks, output modes, triggers, watermarks, and a `StreamingQuery` / `StreamingQueryManager` for lifecycle control.
- **Declarative Pipelines**: build dataflow graphs of tables, materialized views, and flows, then run them on the server.
- **NA and stat helpers**: `na` for null/NaN handling (`drop`, `fill`, `replace`) and `stat` for `corr`, `cov`, `crosstab`, `freqItems`, `approxQuantile`, and `sampleBy`.
- **Window functions**: `org.apache.spark.sql.expressions.Window` with `partitionBy`, `orderBy`, `rowsBetween`, and `rangeBetween`.
- **Observation**: attach an `Observation` to a query to collect aggregate metrics as it runs.

Results decode through Apache Arrow into ordered, name-addressable `Row`s. Method names match Apache Spark's Scala DataFrame API, so existing Spark Scala code ports almost verbatim.

### Not supported

User-defined functions (UDFs) and the streaming `foreach`/`foreachBatch` sinks are not supported. Everything else in the surface above is.

## Documentation

Full documentation, including guides for every part of the API and the generated Scaladoc, lives at **<https://hyukjinkwon.github.io/spark-connect-scala3/>**.

Runnable programs under [`modules/examples/`](modules/examples/) cover the quickstart, aggregations, joins, word count, SQL and views, window functions, reading and writing, Structured Streaming, and Declarative Pipelines.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build the project, run the unit and integration tests, and follow the code style.

## License

`spark-connect-scala3` is licensed under the [Apache License 2.0](LICENSE). It vendors the Spark Connect protobuf definitions from Apache Spark; see [NOTICE](NOTICE) for attribution.
