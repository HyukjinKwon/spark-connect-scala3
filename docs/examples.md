# Examples

Runnable programs live under
[`modules/examples/`](https://github.com/HyukjinKwon/spark-connect-scala3/tree/main/modules/examples).
Each one connects to the server named by the `SPARK_REMOTE` environment variable
(default `sc://localhost:15002`), or to a connection string passed as the first
program argument.

Run any example with sbt:

```bash
sbt "examples/runMain examples.QuickStart"
sbt "examples/runMain examples.Aggregations"
sbt "examples/runMain examples.JoinExample sc://localhost:15002"
```

| Program | What it shows |
| ------- | ------------- |
| `QuickStart` | Connect, build a range, project a derived column, run actions. |
| `Aggregations` | Grouped aggregation with several aggregate functions. |
| `JoinExample` | Joining two DataFrames and aggregating the result. |
| `WordCount` | Split and explode text, then group and count. |
| `SqlAndViews` | Mix the DataFrame API with raw SQL through temporary views, including parameterised SQL. |
| `WindowFunctions` | Window functions: per-department salary ranking and running totals. |
| `ReadWrite` | Round-trip a DataFrame through Parquet on the server's filesystem. |
| `StructuredStreaming` | Read the built-in `rate` source, transform it, write to the in-memory sink, and query the sink. |
| `DeclarativePipeline` | Build and run a Spark Declarative Pipelines dataflow graph (requires an Apache Spark 4.1+ server). |
| `Smoke` | A minimal end-to-end check against a live Spark Connect server. |

Run any of them by name, for example:

```bash
sbt "examples/runMain examples.WindowFunctions"
sbt "examples/runMain examples.StructuredStreaming"
```

## QuickStart

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

object QuickStart:
  def main(args: Array[String]): Unit =
    val remote = if args.nonEmpty then args(0) else "sc://localhost:15002"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try
      val df = spark.range(1, 6).select(col("id"), (col("id") * col("id")).as("square"))
      df.show()
      println(s"row count = ${df.count()}")
    finally
      spark.stop()
```

## Aggregations

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

object Aggregations:
  def main(args: Array[String]): Unit =
    val remote = if args.nonEmpty then args(0) else "sc://localhost:15002"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try
      val sales = spark.sql(
        "SELECT * FROM VALUES " +
          "('KR', 'book', 12.0), ('KR', 'pen', 3.5), ('KR', 'book', 9.0), " +
          "('US', 'book', 15.0), ('US', 'pen', 2.0) AS t(country, item, amount)")

      sales.groupBy("country")
        .agg(
          count("*").as("orders"),
          round(sum("amount"), 2).as("total"),
          round(avg("amount"), 2).as("avg"),
          max("amount").as("max"))
        .orderBy("country")
        .show()
    finally
      spark.stop()
```

## JoinExample

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

object JoinExample:
  def main(args: Array[String]): Unit =
    val remote = if args.nonEmpty then args(0) else "sc://localhost:15002"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try
      val orders = spark.sql(
        "SELECT * FROM VALUES (1, 'KR', 120.0), (2, 'US', 80.0), (3, 'KR', 50.0) " +
          "AS t(id, country, amount)")
      val regions = spark.sql(
        "SELECT * FROM VALUES ('KR', 'Asia'), ('US', 'Americas') AS t(country, region)")

      orders.join(regions, "country")
        .groupBy("region")
        .agg(round(sum("amount"), 2).as("total"))
        .orderBy("region")
        .show()
    finally
      spark.stop()
```

See [Quickstart](quickstart.md) and the guides for the full API.
