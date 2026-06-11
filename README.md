# spark-connect-scala3

A native **Scala 3** client for [Apache Spark Connect](https://spark.apache.org/spark-connect/).

[![CI](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml)
[![Docs](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/docs.yml/badge.svg)](https://hyukjinkwon.github.io/spark-connect-scala3/)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.hyukjinkwon/spark-connect-scala3_3.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.hyukjinkwon/spark-connect-scala3_3)
[![Scala 3](https://img.shields.io/badge/scala-3.3%20LTS-red.svg)](https://www.scala-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Talk to a Spark cluster from a lightweight Scala 3 application — no Spark JARs, no
Scala 2 classpath, no embedded JVM driver. `spark-connect-scala3` speaks the Spark
Connect gRPC protocol directly and gives you the `DataFrame` API you already know.

> **Status:** early but functional. The DataFrame/Column/functions surface mirrors
> Apache Spark's Scala API. See the [compatibility matrix](https://hyukjinkwon.github.io/spark-connect-scala3/reference/compatibility/).

---

## Why?

The official Spark Connect Scala client is published for Scala 2.12/2.13 and pulls in
a large part of the Spark codebase. If your application is on **Scala 3**, you either
cross-compile against Scala 2 artifacts or you can't use it at all.

`spark-connect-scala3` is:

- **Scala 3 first** — published as `_3`, built with the Scala 3 LTS compiler.
- **Thin** — a few MB of gRPC + Arrow + ScalaPB, instead of the full Spark assembly.
- **Familiar** — the public API lives under `org.apache.spark.sql.*` and matches the
  names and shapes of Apache Spark, so existing Spark Scala code largely just compiles.
- **Remote** — connects to any Spark Connect server (Apache Spark 3.5+, Databricks
  Connect-compatible endpoints, local `start-connect-server.sh`).

## Quick start

Add the dependency (Scala 3):

```scala
libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3" % "0.1.0"
```

Start a Spark Connect server (Apache Spark 4.0):

```bash
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.0.0
```

Then:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

@main def demo(): Unit =
  val spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate()

  import spark.implicits.*

  val df = spark.range(0, 1000)
    .select($"id", ($"id" % 3).as("bucket"))
    .groupBy($"bucket")
    .agg(count("*").as("n"), avg($"id").as("avg_id"))
    .orderBy($"bucket")

  df.show()
  spark.stop()
```

```
+------+---+------+
|bucket|  n|avg_id|
+------+---+------+
|     0|334| 499.5|
|     1|333| 499.0|
|     2|333| 500.0|
+------+---+------+
```

See the [**Getting Started guide**](https://hyukjinkwon.github.io/spark-connect-scala3/getting-started/)
and [**runnable examples**](examples/).

## Features

| Area | Highlights |
|------|------------|
| Session | `SparkSession.builder().remote(...)`, config, `sql(...)`, `range(...)`, `createDataFrame(...)` |
| DataFrame | `select`, `filter`/`where`, `withColumn`, `join`, `groupBy`/`agg`, `orderBy`, `union`, `distinct`, `limit`, `drop`, `withColumnRenamed`, `na`, `sample`, … |
| Column | full expression DSL: arithmetic, comparison, `when`/`otherwise`, `cast`, `isin`, `like`, `getItem`, `getField`, `over(window)` |
| functions | aggregate, math, string, date/time, collection, conditional, window functions |
| Types | complete `org.apache.spark.sql.types` hierarchy with proto round-tripping |
| Results | Apache Arrow columnar decode → `Row`, `collect`/`show`/`count`/`take`/`head`/`toLocalIterator` |
| I/O | `DataFrameReader`/`DataFrameWriter` (parquet/json/csv/orc/text), `Catalog` |

A full, always-current list lives in the [compatibility matrix](https://hyukjinkwon.github.io/spark-connect-scala3/reference/compatibility/).

## How it works

```
your Scala 3 app
      │  org.apache.spark.sql.DataFrame  (lazy, builds a proto plan)
      ▼
SparkConnectClient ──gRPC (HTTP/2)──▶ Spark Connect server ──▶ Spark cluster
      ▲                                        │
      └────── Apache Arrow record batches ◀────┘
```

A `DataFrame` is a thin builder over a `spark.connect.Relation` protobuf message.
Transformations are lazy and just nest relations. Actions send an `ExecutePlan` gRPC
request; results stream back as Arrow IPC batches that are decoded into `Row`s.

## Documentation

- [Getting started](https://hyukjinkwon.github.io/spark-connect-scala3/getting-started/)
- [User guide](https://hyukjinkwon.github.io/spark-connect-scala3/guide/sparksession/)
- [API reference (Scaladoc)](https://hyukjinkwon.github.io/spark-connect-scala3/api/)
- [Examples](examples/)

## Building from source

Requires JDK 17+ and `sbt`.

```bash
sbt compile          # compile everything (generates proto stubs first)
sbt test             # unit tests (no server needed)
sbt scalafmtCheckAll # formatting
```

Integration tests run against a live Spark Connect server; see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Compatibility

- **Scala:** 3.3.x (LTS).
- **JDK:** 17, 21.
- **Spark Connect protocol:** Apache Spark **4.0.0** (works with 3.5.x servers for the
  supported surface).

## Contributing

Contributions are very welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License 2.0](LICENSE). The vendored Spark Connect protobuf
definitions are from Apache Spark, also Apache 2.0. See [NOTICE](NOTICE).

This is an independent project and is **not** affiliated with or endorsed by the Apache
Software Foundation. "Apache Spark" and "Spark" are trademarks of the ASF.
