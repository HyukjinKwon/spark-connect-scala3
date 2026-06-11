# Spark Connect for Scala 3

A pure-Scala-3 client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) - a gRPC DataFrame API that mirrors Apache Spark's own Scala API.

If you have written Spark in Scala, you already know most of this library. There
is no JVM Spark on the client, no `spark-submit`, and no local Spark
installation - only a reachable Spark Connect server.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder
  .remote("sc://localhost:15002")
  .appName("quickstart")
  .getOrCreate()

spark.range(10)
  .select(col("id"), (col("id") * 2).as("doubled"))
  .filter(col("id") % 2 === 0)
  .show()

spark.stop()
```

## What is Spark Connect?

Classic Spark applications run your driver code inside the cluster's JVM. Spark
Connect splits that apart: your program is a thin **client** that builds an
unresolved logical plan and ships it to a remote **server** over gRPC. The
server plans, optimizes, and executes the query, then streams results back as
[Apache Arrow](https://arrow.apache.org/) batches.

```
  Your Scala 3 program            Spark Connect server            Spark cluster
  spark-connect-scala3   --gRPC-->   (plan + optimize)   ------>   (execute)
         ^                                                              |
         +--------------------- Arrow result batches ------------------+
```

Because the protocol is language-agnostic, the client can live in any language.
This project is that client for Scala 3.

## What it supports

`spark-connect-scala3` implements the Spark Connect DataFrame, SQL, Structured Streaming, and Declarative Pipelines API, modeled directly on Apache Spark's Scala API: everything except user-defined functions (UDFs) and the `foreach`/`foreachBatch` streaming sinks, which run user JVM code on the server that the Spark Connect protocol does not transport. (MLlib over Connect is also out of scope.)

Results decode through Apache Arrow into ordered, name-addressable `Row`s. Class and method names match Apache Spark's Scala API (`SparkSession`, `DataFrame`, `Column`, `functions`, `Window`, `Catalog`, ...), so existing Spark Scala code ports almost verbatim.

## Project facts

- **Maven coordinates**: `io.github.hyukjinkwon` :: `spark-connect-scala3-client`, built for Scala 3.3.x.
- **Spark compatibility**: built and tested against Apache Spark 4.0.x and 4.1.x (latest 4.1.2). The protobufs are sourced from Spark 4.1.2.
- **Source**: [HyukjinKwon/spark-connect-scala3](https://github.com/HyukjinKwon/spark-connect-scala3).

## Where to next

| Guide | What is inside |
| ----- | -------------- |
| [Installation](installation.md) | The dependency, JDK flags, and a local server |
| [Quickstart](quickstart.md) | Connecting and your first DataFrames |
| [DataFrames](dataframes.md) | The full transformation and action surface |
| [Columns and Functions](columns-and-functions.md) | Expressions and the functions library |
| [Data Sources](data-sources.md) | Reading and writing files and tables |
| [SQL](sql.md) | Running SQL and using views |
| [Catalog](catalog.md) | Inspecting and managing metadata |
| [Structured Streaming](streaming.md) | Streaming sources, sinks, and queries |
| [Declarative Pipelines](pipelines.md) | Dataflow graphs |
| [Configuration and Connection](configuration.md) | Connection strings and runtime config |
| [Examples](examples.md) | Runnable programs |
| [API (Scaladoc)](https://hyukjinkwon.github.io/spark-connect-scala3/api/) | Generated method-level reference |
