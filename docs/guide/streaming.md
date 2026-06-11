# Structured Streaming

`spark-connect-scala3` supports Spark Structured Streaming: read from streaming sources,
build streaming `DataFrame`s with the same transformation API as batch, write to
streaming sinks, and manage the resulting queries.

The API mirrors Apache Spark's: `spark.readStream`, `df.writeStream`, `Trigger`,
`StreamingQuery`, and `spark.streams`.

!!! note "Not supported"
    The `foreach` / `foreachBatch` sinks are out of scope, because they require shipping
    user JVM closures to the server. Every other source and sink - file, Kafka, console,
    memory, rate - together with triggers, output modes, watermarks, and the query
    manager, is fully supported.

## Reading a stream

`spark.readStream` returns a `DataStreamReader` that mirrors the batch
`DataFrameReader`:

```scala
import org.apache.spark.sql.functions.*

val stream = spark.readStream
  .format("rate")
  .option("rowsPerSecond", 10)
  .load()

stream.isStreaming          // true
stream.printSchema()        // root |-- timestamp: timestamp |-- value: long
```

`format` / `option` / `options` / `schema` / `load` / `table`, plus the
`csv` / `json` / `parquet` / `orc` / `text` shortcuts, all behave like their batch
counterparts. The returned `DataFrame` is a normal `DataFrame`: apply `select`,
`filter`, `groupBy`, `withWatermark`, and so on.

## Watermarks and windowed aggregation

```scala
events
  .withWatermark("event_time", "10 minutes")
  .groupBy(window($"event_time", "5 minutes"))
  .count()
```

## Writing a stream

`df.writeStream` returns a `DataStreamWriter`. Calling `start()` (or `toTable(...)`)
launches the query and returns a `StreamingQuery`:

```scala
val query = stream.writeStream
  .format("memory")             // or "console", "parquet", "kafka", ...
  .queryName("rates")           // required for the memory sink
  .outputMode("append")         // "append" | "complete" | "update"
  .trigger(Trigger.ProcessingTime("1 second"))
  .start()

query.id        // stable query id (survives checkpoint restarts)
query.runId     // unique per start
query.isActive  // true
```

### Triggers

```scala
import org.apache.spark.sql.streaming.Trigger

Trigger.ProcessingTime("10 seconds")  // micro-batch every interval
Trigger.Once()                        // process available data once, then stop
Trigger.AvailableNow()                // process all available data, then stop
Trigger.Continuous("1 second")        // continuous processing
```

### Sinks

```scala
// Files (provide a checkpoint location)
stream.writeStream
  .format("parquet")
  .option("checkpointLocation", "/chk/out")
  .start("/data/out")

// A catalog table
stream.writeStream
  .format("parquet")
  .option("checkpointLocation", "/chk/tbl")
  .toTable("db.events")
```

## Inspecting and controlling a query

```scala
query.status                        // StreamingQueryStatus
query.recentProgress                // Array[StreamingQueryProgress]
query.lastProgress                  // the most recent progress object
query.processAllAvailable()
query.awaitTermination(10000)       // block up to 10s; returns terminated?
query.explain()
query.exception                     // the failure, if any
query.stop()
```

## Managing queries

`spark.streams` returns a `StreamingQueryManager`:

```scala
spark.streams.active                       // Array[StreamingQuery]
spark.streams.get(query.id)                // the query, or null
spark.streams.awaitAnyTermination(30000)
spark.streams.resetTerminated()
```
