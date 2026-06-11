# spark-connect-scala3

A native **Scala 3** client for [Apache Spark Connect](https://spark.apache.org/spark-connect/).

[![CI](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml)
[![Docs](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/docs.yml/badge.svg)](https://hyukjinkwon.github.io/spark-connect-scala3/)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.hyukjinkwon/spark-connect-scala3_3.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.hyukjinkwon/spark-connect-scala3_3)
[![Scala 3](https://img.shields.io/badge/scala-3.3%20LTS-red.svg)](https://www.scala-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Talk to a Spark cluster from a lightweight Scala 3 application - no Spark JARs, no
Scala 2 classpath, no embedded JVM driver. `spark-connect-scala3` speaks the Spark
Connect gRPC protocol directly and gives you the full `DataFrame`/`Dataset` API you
already know from Apache Spark.

The public API lives under `org.apache.spark.sql.*` and mirrors Apache Spark's Scala
API: `SparkSession`, `DataFrame`/`Dataset`, `Column`, `functions`, `Window`, `Catalog`,
`DataFrameReader`/`Writer`, and Structured Streaming all work exactly as you'd expect.

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

---

## Why?

The official Spark Connect Scala client is published for Scala 2.12/2.13 and pulls in
a large part of the Spark codebase. If your application is on **Scala 3**, you either
cross-compile against Scala 2 artifacts or you can't use it at all.

`spark-connect-scala3` is:

- **Scala 3 first** - published as `_3`, built with the Scala 3 LTS compiler.
- **Thin** - a few MB of gRPC + Arrow + ScalaPB, instead of the full Spark assembly.
- **Familiar** - the public API lives under `org.apache.spark.sql.*` and matches the
  names and shapes of Apache Spark, so existing Spark Scala code largely just compiles.
- **Remote** - connects to any Spark Connect server over gRPC.

## Install

Add the dependency (the `%%` operator appends the Scala 3 `_3` suffix):

```scala
libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3" % "0.1.0"
```

## Start a Spark Connect server

Download Apache Spark and start a server locally:

```bash
curl -fsSL https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz \
  | tar xz
cd spark-4.1.0-bin-hadoop3
./sbin/start-connect-server.sh
```

Spark 4.0.0 and later bundle the Connect server, so no `--packages` flag is required.
The server listens on `sc://localhost:15002` by default; pass
`--conf spark.connect.grpc.binding.port=15002` to set the port explicitly. Stop it with
`./sbin/stop-connect-server.sh`.

> Only Spark 3.5.x needs the server to be added explicitly, with
> `--packages org.apache.spark:spark-connect_2.13:3.5.x`. From Spark 4.0.0 onward it is
> bundled.

## Connecting

`remote(...)` takes a standard Spark Connect connection string:

```scala
// local, plaintext
SparkSession.builder().remote("sc://localhost:15002").getOrCreate()

// TLS + bearer token (a token implies TLS)
SparkSession.builder()
  .remote(s"sc://spark.example.com:443/;token=${sys.env("SPARK_TOKEN")};user_id=alice")
  .getOrCreate()
```

Supported parameters: `token`, `user_id`, `user_agent`, `use_ssl`, `session_id`.

## Features

| Area | What's included |
|------|-----------------|
| Session | `SparkSession.builder().remote(...)`, `config`, `sql`, `range`, `table`, `createDataFrame`, `conf`, `catalog`, `version`, `stop` |
| DataFrame / Dataset | `select`, `filter`/`where`, `withColumn`, `join`, `groupBy`/`agg`, `rollup`, `cube`, `pivot`, window, `union`, `sort`, `limit`, `distinct`, `na`, `stat`, `unpivot`, `describe`, and the rest of the relational API |
| Column | full expression DSL: arithmetic, comparison, `when`/`otherwise`, `cast`, `isin`, `like`, `getItem`, `getField`, `over(window)` |
| functions | the `functions` object - 400+ aggregate, math, string, date/time, collection, conditional, and window functions |
| Implicits | `import spark.implicits._` for `$"col"` and `Seq(...).toDF(...)` |
| Window | `Window.partitionBy(...).orderBy(...)`, `rowsBetween`, `rangeBetween` |
| Types | complete `org.apache.spark.sql.types` hierarchy with proto round-tripping |
| Results | Apache Arrow columnar decode -> `Row`, `collect`/`show`/`count`/`take`/`head`/`toLocalIterator` |
| I/O | `DataFrameReader`/`Writer` (csv/json/parquet/orc/text/jdbc/table), `saveAsTable`/`insertInto`, `partitionBy`/`bucketBy`/`sortBy`, `DataFrameWriterV2` |
| Catalog | databases, tables, columns, functions, temp views, caching |
| Streaming | Structured Streaming: `readStream`/`writeStream`, triggers, output modes, watermarks, `StreamingQuery`, `StreamingQueryManager` |

**Not supported:** user-defined functions (UDFs/UDAFs/UDTFs) and the `foreach`/`foreachBatch`
streaming sinks, because they require shipping user JVM closures to the server.
Everything else in the Spark Connect surface is implemented.

## How it works

```
your Scala 3 app
      |  org.apache.spark.sql.DataFrame  (lazy, builds a proto plan)
      v
SparkConnectClient --gRPC (HTTP/2)--> Spark Connect server --> Spark cluster
      ^                                        |
      +------ Apache Arrow record batches <----+
```

A `DataFrame` is a thin builder over a `spark.connect.Relation` protobuf message.
Transformations are lazy and just nest relations. Actions send an `ExecutePlan` gRPC
request; results stream back as Arrow IPC batches that are decoded into `Row`s.

## Documentation

- [Getting started](https://hyukjinkwon.github.io/spark-connect-scala3/getting-started/)
- [User guide](https://hyukjinkwon.github.io/spark-connect-scala3/guide/sparksession/)
- [Structured Streaming](https://hyukjinkwon.github.io/spark-connect-scala3/guide/streaming/)
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
- **Protocol:** targets the Spark Connect **4.1** protocol.
- **Spark Connect servers:** Apache Spark **3.5 and above**.

## Contributing

Contributions are very welcome - see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License 2.0](LICENSE). The vendored Spark Connect protobuf
definitions are from Apache Spark, also Apache 2.0. See [NOTICE](NOTICE).

This is an independent project and is **not** affiliated with or endorsed by the Apache
Software Foundation. "Apache Spark" and "Spark" are trademarks of the ASF.
