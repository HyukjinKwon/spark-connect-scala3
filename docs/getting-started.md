# Getting Started

This guide takes you from zero to a running query in a few minutes.

## 1. Requirements

- **JDK 17 or 21**
- **Scala 3.3.x** (LTS) — via sbt, Mill, or scala-cli
- A reachable **Spark Connect server** (Apache Spark 3.5+ / 4.0)

## 2. Add the dependency

=== "sbt"

    ```scala
    libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3" % "0.1.0"
    ```

=== "Mill"

    ```scala
    def ivyDeps = Agg(ivy"io.github.hyukjinkwon::spark-connect-scala3:0.1.0")
    ```

=== "scala-cli"

    ```scala
    //> using dep io.github.hyukjinkwon::spark-connect-scala3:0.1.0
    ```

## 3. Start a Spark Connect server

If you don't already have a server, download Apache Spark and start one locally:

```bash
curl -fsSL https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz \
  | tar xz
cd spark-4.0.0-bin-hadoop3
./sbin/start-connect-server.sh
```

The server listens on `sc://localhost:15002` by default. Stop it later with
`./sbin/stop-connect-server.sh`.

!!! tip "Already have a cluster?"
    Point `remote(...)` at any Spark Connect endpoint, e.g.
    `sc://my-host:443/;token=...;use_ssl=true`. See
    [Configuration](reference/configuration.md) for the connection string format.

## 4. Your first program

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

@main def hello(): Unit =
  val spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate()

  import spark.implicits.*

  val df = spark.range(1, 6).select(
    $"id",
    ($"id" * $"id").as("square")
  )

  df.show()
  // +---+------+
  // | id|square|
  // +---+------+
  // |  1|     1|
  // |  2|     4|
  // |  3|     9|
  // |  4|    16|
  // |  5|    25|
  // +---+------+

  println(s"count = ${df.count()}")

  spark.stop()
```

## 5. Run it

=== "sbt"

    ```bash
    sbt run
    ```

=== "scala-cli"

    ```bash
    scala-cli run hello.scala
    ```

That's it — you're driving a Spark cluster from Scala 3.

## Where to go next

- [SparkSession](guide/sparksession.md) — building and configuring the session.
- [DataFrame & Dataset](guide/dataframe.md) — the transformation and action API.
- [Columns & Functions](guide/columns-and-functions.md) — the expression DSL.
- [Examples](examples.md) — complete, runnable programs.
