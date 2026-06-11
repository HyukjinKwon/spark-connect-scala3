# Structured Streaming

Structured Streaming treats a stream as an unbounded table. You build a
streaming DataFrame from a streaming source, transform it with the same
DataFrame API you use for batch data, and write it to a sink with
`writeStream`.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

## Reading a stream

`spark.readStream` returns a `DataStreamReader`. Configure a format and options,
then load:

```scala
val lines = spark.readStream
  .format("rate")
  .option("rowsPerSecond", 5)
  .load()

lines.isStreaming   // true
```

File and Kafka sources work the same way:

```scala
spark.readStream
  .format("json")
  .schema("id INT, ts TIMESTAMP, value DOUBLE")
  .load("/data/incoming")

spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "events")
  .load()
```

## Transforming a stream

Streaming DataFrames support the same transformations as batch DataFrames:

```scala
val counts = lines
  .groupBy(window(col("timestamp"), "10 seconds"))
  .count()
```

### Watermarks

Bound state for aggregations over event time with a watermark:

```scala
val withWatermark = lines
  .withWatermark("timestamp", "1 minute")
  .groupBy(window(col("timestamp"), "10 seconds"))
  .count()
```

## Writing a stream

`writeStream` returns a `DataStreamWriter`. Set the output mode, format, and
trigger, then `start` the query:

```scala
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

val query = counts.writeStream
  .format("console")
  .outputMode(OutputMode.Complete)
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .queryName("counts")
  .start()
```

### Output modes

| Mode | Meaning |
| ---- | ------- |
| `OutputMode.Append` | Only new rows are written. The default. |
| `OutputMode.Complete` | The full result table is written every trigger. |
| `OutputMode.Update` | Only rows that changed are written. |

You can also pass the mode as a string: `outputMode("complete")`.

### Triggers

```scala
Trigger.ProcessingTime("5 seconds")  // fire on a fixed interval
Trigger.AvailableNow()               // process all available data, then stop
Trigger.Continuous("1 second")       // continuous processing
```

### Sinks

```scala
// Console (for development).
df.writeStream.format("console").start()

// Files.
df.writeStream
  .format("parquet")
  .option("checkpointLocation", "/checkpoints/events")
  .start("/data/out")

// A managed table.
df.writeStream
  .option("checkpointLocation", "/checkpoints/events")
  .toTable("events_stream")
```

## Managing queries

A `StreamingQuery` controls the lifecycle of a running stream:

```scala
query.isActive          // Boolean
query.id                // the stable query id
query.name              // the query name, if set
query.status            // the current status
query.lastProgress      // the most recent progress as JSON
query.awaitTermination()        // block until the query stops
query.awaitTermination(10000)   // block up to 10s, returns Boolean
query.stop()
```

`spark.streams` is the `StreamingQueryManager` for all queries in the session:

```scala
spark.streams.active                 // Array[StreamingQuery]
spark.streams.get(query.id)          // look a query up by id
spark.streams.awaitAnyTermination()  // block until any query stops
spark.streams.resetTerminated()
```

## Not supported

The `foreach` and `foreachBatch` streaming sinks are not supported. Use the
built-in sinks (`console`, file formats, Kafka, and `toTable`) instead.

## Listening to query events

Register a `StreamingQueryListener` with `spark.streams.addListener(...)` to receive
streaming-query lifecycle events. Over Spark Connect the server forwards serialized
events on a dedicated stream and the client invokes your listener locally, so no
user code runs on the server (unlike `foreach`/`foreachBatch`).

```scala
import org.apache.spark.sql.streaming.StreamingQueryListener

val listener = new StreamingQueryListener {
  def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit =
    println(s"started ${e.id} (${e.name})")
  def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit =
    println(e.json) // Spark's StreamingQueryProgress JSON
  def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit =
    println(s"terminated ${e.id} ${e.exception.getOrElse("")}")
}

spark.streams.addListener(listener)
// ... start and run queries ...
spark.streams.listListeners()      // Array(listener)
spark.streams.removeListener(listener)
```

Each event exposes the full server-side payload as `event.json`; the started, idle,
and terminated events also expose parsed `id` / `runId` (and `name` / `exception`)
accessors.
