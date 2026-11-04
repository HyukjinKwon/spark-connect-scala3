# Declarative Pipelines

Spark Declarative Pipelines let you describe a dataflow graph of tables,
materialized views, and the flows that populate them, then hand the whole graph
to the server to plan and run. Instead of orchestrating individual writes, you
declare the outputs and how each is computed, and the server resolves the
dependency order.

A pipeline is created from a session and bound to a server-side dataflow graph.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.pipelines.Pipeline

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

val pipeline = Pipeline.create(
  spark,
  defaultCatalog = Some("spark_catalog"),
  defaultDatabase = Some("analytics"))
```

## Declaring outputs

Each `create*` call registers an output and the flow that computes it, returning
the resolved identifier. Reference earlier outputs in the same graph with
`pipeline.read(name)`.

```scala
// A base table, populated from a query.
val events = pipeline.createTable(
  name = "events",
  df = Some(spark.read.format("json").load("/data/incoming")),
  comment = Some("Raw event records"))

// A materialized view derived from the table above.
pipeline.createMaterializedView(
  name = "daily_counts",
  df = Some(
    pipeline.read("events")
      .groupBy(to_date(col("ts")).as("day"))
      .agg(count("*").as("n"))),
  comment = Some("Event counts per day"))
```

`createTemporaryView` declares an intermediate view that is part of the graph
but not published as a table:

```scala
pipeline.createTemporaryView(
  name = "clean_events",
  df = Some(pipeline.read("events").where(col("value").isNotNull)))
```

## Flows

`createTable` and `createMaterializedView` define an output together with the
flow that fills it. To add a flow to an existing target explicitly, use
`defineFlow`:

```scala
pipeline.defineFlow(
  name = "events_backfill",
  df = spark.read.format("json").load("/data/backfill"),
  target = Some("events"))
```

## Auto CDC flows (Spark 4.2+)

`createAutoCdcFlow` applies a stream of change-data-capture events to a
streaming table, keyed by `keys` and ordered by `sequenceBy`. Define the target
table without a query first. SCD type 2 targets (`storedAsScdType = Some(2)`) and
history tracking need a Spark 4.3+ server:

```scala
pipeline.createTable("customers")
pipeline.createAutoCdcFlow(
  target = "customers",
  source = "customer_changes",
  keys = Seq(col("customer_id")),
  sequenceBy = col("updated_at"),
  applyAsDeletes = Some(col("op") === "DELETE"),
  exceptColumnList = Seq(col("op")),
  storedAsScdType = Some(2))
```

`columnList` and `exceptColumnList` are mutually exclusive, and
`trackHistoryColumnList` / `trackHistoryExceptColumnList` choose which columns
open a new history record for SCD type 2 targets.

## Defining a graph from SQL

You can register datasets and flows from a SQL definition instead of the
programmatic API:

```scala
pipeline.defineSql("""
  CREATE MATERIALIZED VIEW daily_counts AS
  SELECT date(ts) AS day, count(*) AS n
  FROM events
  GROUP BY date(ts)
""")
```

## Running the pipeline

`startRun` resolves the graph and runs an update. It blocks until the update
completes and returns the events the run emitted.

```scala
val events = pipeline.startRun()
events.foreach(e => println(s"${e.timestamp}: ${e.message}"))
```

Options control what gets recomputed:

```scala
// Validate the graph without executing any flows.
pipeline.startRun(dry = true)

// Reset and recompute everything.
pipeline.startRun(fullRefreshAll = true)

// Update only specific datasets.
pipeline.startRun(refresh = Seq("daily_counts"))

// Reset and recompute specific datasets, with an explicit storage location.
pipeline.startRun(
  fullRefresh = Seq("events"),
  storage = Some("/pipelines/analytics"))
```

## Tearing down

Drop the dataflow graph and stop any attached flows when you are done:

```scala
pipeline.drop()
```
