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
