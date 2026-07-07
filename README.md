# Spark Connect for Scala 3

[![CI](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/spark-connect-scala3/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.hyukjinkwon/spark-connect-scala3-client_3.svg)](https://central.sonatype.com/artifact/com.github.hyukjinkwon/spark-connect-scala3-client_3)
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

The current release is `0.3.0`, published for **Scala 3.3.x** (the Scala 3 LTS line), running on **JDK 17 or newer**.

sbt (`build.sbt`):

```scala
libraryDependencies += "com.github.hyukjinkwon" %% "spark-connect-scala3-client" % "0.3.0"
```

Maven (`pom.xml`) - Scala 3 artifacts use the `_3` suffix:

```xml
<dependency>
  <groupId>com.github.hyukjinkwon</groupId>
  <artifactId>spark-connect-scala3-client_3</artifactId>
  <version>0.3.0</version>
</dependency>
```

Gradle (`build.gradle.kts`):

```kotlin
implementation("com.github.hyukjinkwon:spark-connect-scala3-client_3:0.3.0")
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

## Interactive shell

Use any Scala REPL with the client as a dependency. With
[scala-cli](https://scala-cli.virtuslab.org/):

```bash
scala-cli repl \
  --dep com.github.hyukjinkwon::spark-connect-scala3-client:0.3.0 \
  --java-opt --add-opens=java.base/java.nio=ALL-UNNAMED \
  --java-opt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

Then connect a session and explore:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
import spark.implicits.*
spark.range(1, 6).select($"id", ($"id" * $"id").as("square")).show()
```

## What it supports

`spark-connect-scala3` implements the Spark Connect DataFrame, SQL, Structured
Streaming, and Declarative Pipelines API, modeled directly on Apache Spark's Scala
API (`SparkSession`, `DataFrame`, `Column`, `functions`, `Dataset[T]`, ...), so
existing Spark Scala code ports almost verbatim. Results decode through Apache
Arrow into ordered, name-addressable `Row`s.

It also includes typed Datasets: `df.as[T]` and `spark.createDataset(values)` for
case classes, tuples, primitives, `Option`, collections, and maps. The encoders run
entirely on the client, so no closure is sent to the server.

### Plugin extensions

Spark Connect plugins (server-side `RelationPlugin` and friends, e.g. GraphFrames)
are supported: you can build a `Relation.extension` from a packed protobuf message
and turn it into a `DataFrame`, mirroring the PySpark client's
`plan.extension.Pack(msg)` / `DataFrame(plan, session)`. See the
[Plugin extensions guide](https://hyukjinkwon.github.io/spark-connect-scala3/plugin-extensions/).

### Not supported

The following all require running a user-provided JVM closure on the server (the
same mechanism as a UDF), which Spark Connect for Scala 3 does not provide:

- User-defined functions: `functions.udf`, `spark.udf.register`, and typed
  `Aggregator` / UDAFs.
- Typed `Dataset` transformations whose argument is a Scala function: `map`,
  `flatMap`, `mapPartitions`, `groupByKey` (and its `mapGroups` / `flatMapGroups` /
  `reduceGroups`), and `reduce`. Note that `as[T]` and `createDataset`, which only
  attach an encoder and ship no closure, ARE supported.
- Structured Streaming `foreach` and `foreachBatch` sinks.

Also out of scope because they are not part of the Spark Connect protocol at all:

- The RDD API (`Dataset.rdd`, `SparkContext`, accumulators, broadcast variables).
- The MLlib-over-Connect surface.

Class and method names otherwise match Apache Spark's Scala API, so existing Spark
Scala code ports almost verbatim.

The untyped `DataFrame` surface is complete, and typed Datasets (`as[T]`,
`createDataset`, `Encoder` derivation) are supported. The closure-driven typed
operations (`map`/`flatMap`/`reduce`, `groupByKey` / `KeyValueGroupedDataset`,
typed aggregators) and a few niche APIs are not implemented. See
[API coverage](https://hyukjinkwon.github.io/spark-connect-scala3/api-coverage/)
for the exact breakdown.

## Documentation

Full documentation, including guides for every part of the API and the generated Scaladoc, lives at **<https://hyukjinkwon.github.io/spark-connect-scala3/>**.

Runnable programs under [`modules/examples/`](modules/examples/) cover the quickstart, aggregations, joins, word count, SQL and views, window functions, reading and writing, Structured Streaming, and Declarative Pipelines.
