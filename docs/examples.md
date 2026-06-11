# Examples

Complete, runnable programs live in the
[`examples/`](https://github.com/HyukjinKwon/spark-connect-scala3/tree/main/examples)
module. Each reads the target server from the `SPARK_REMOTE` environment variable,
falling back to `sc://localhost:15002`.

Run any of them with sbt:

```bash
export SPARK_REMOTE="sc://localhost:15002"
sbt "examples/runMain examples.QuickStart"
sbt "examples/runMain examples.WordCount"
sbt "examples/runMain examples.Aggregations"
sbt "examples/runMain examples.SqlAndViews"
sbt "examples/runMain examples.ReadWrite"
sbt "examples/runMain examples.WindowFunctions"
```

## QuickStart

The smallest end-to-end program: build a session, create a range, project a column,
and show the result.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

object QuickStart:
  def main(args: Array[String]): Unit =
    val spark = SparkSession.builder()
      .remote(sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002"))
      .getOrCreate()
    import spark.implicits.*

    spark.range(1, 6)
      .select($"id", ($"id" * $"id").as("square"))
      .show()

    spark.stop()
```

## Word count

The "hello world" of data processing - split text, explode to rows, group and count.

```scala
val lines = Seq(
  "the quick brown fox",
  "the lazy dog",
  "the quick dog"
).toDF("line")

lines
  .select(explode(split($"line", " ")).as("word"))
  .groupBy($"word")
  .count()
  .orderBy($"count".desc, $"word")
  .show()
```

## Aggregations

`groupBy` + `agg` with multiple aggregate functions, then ordering.

## SQL & views

Register a `DataFrame` as a temp view and query it with `spark.sql`, mixing SQL and
the DataFrame API.

## Read / write

Round-trip a `DataFrame` through parquet on the server's filesystem.

## Window functions

Per-partition ranking and running totals with `Window`.

## Structured Streaming

Read a stream, transform it, and write to a sink - see the
[Structured Streaming guide](guide/streaming.md) for the full API.

```scala
val rates = spark.readStream
  .format("rate")
  .option("rowsPerSecond", 10)
  .load()

val query = rates
  .selectExpr("value", "value % 5 AS bucket")
  .writeStream
  .format("memory")
  .queryName("rates")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("1 second"))
  .start()

query.processAllAvailable()
spark.sql("SELECT bucket, count(*) FROM rates GROUP BY bucket").show()
query.stop()
```

!!! tip
    The example sources are the most up-to-date reference for idiomatic usage -
    they're compiled in CI, so they never drift from the API.
