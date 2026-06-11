# spark-connect-scala3

A native **Scala 3** client for [Apache Spark Connect](https://spark.apache.org/spark-connect/).

Talk to a Spark cluster from a lightweight Scala 3 application — no Spark JARs, no
Scala 2 classpath, no embedded JVM driver. `spark-connect-scala3` speaks the Spark
Connect gRPC protocol directly and gives you the `DataFrame` API you already know.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .getOrCreate()

import spark.implicits.*

spark.range(0, 1000)
  .select($"id", ($"id" % 3).as("bucket"))
  .groupBy($"bucket")
  .agg(count("*").as("n"), avg($"id").as("avg_id"))
  .orderBy($"bucket")
  .show()
```

## Why this project?

The official Spark Connect Scala client is published for Scala 2.12/2.13 and pulls in
a large part of the Spark codebase. If your application is on **Scala 3**, that's a
hard wall. `spark-connect-scala3` is:

<div class="grid cards" markdown>

-   :material-language-scala: __Scala 3 first__

    Published as `_3`, compiled with the Scala 3 LTS toolchain. No cross-building
    against Scala 2 artifacts.

-   :material-feather: __Thin & fast to start__

    A few MB of gRPC + Arrow + ScalaPB instead of the full Spark assembly. Cold start
    in milliseconds.

-   :material-check-all: __Familiar API__

    The public API lives under `org.apache.spark.sql.*` and mirrors Apache Spark, so
    existing Spark Scala code largely just compiles.

-   :material-lan-connect: __Connects anywhere__

    Any Spark Connect server: Apache Spark 3.5+, a local `start-connect-server.sh`, or
    a compatible managed endpoint.

</div>

## Next steps

- [**Getting Started**](getting-started.md) — install, start a server, run your first query.
- [**User Guide**](guide/sparksession.md) — sessions, DataFrames, functions, I/O, SQL.
- [**Architecture**](reference/architecture.md) — how the client maps to the protocol.
- [**Compatibility**](reference/compatibility.md) — what's supported today.
- [**API Reference**](https://hyukjinkwon.github.io/spark-connect-scala3/api/) — Scaladoc.

!!! note "Not affiliated with the ASF"
    This is an independent project and is **not** affiliated with or endorsed by the
    Apache Software Foundation. "Apache Spark" and "Spark" are trademarks of the ASF.
