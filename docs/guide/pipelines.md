# Declarative Pipelines

Spark Declarative Pipelines (SDP) let you describe a dataflow graph of datasets and the
flows that populate them, then run the whole graph as a managed update. This requires
an **Apache Spark 4.1 or newer** Connect server.

Start from [`SparkSession.pipeline`](https://hyukjinkwon.github.io/spark-connect-scala3/api/):

```scala
val pipe = spark.pipeline()
```

## Defining outputs and flows

An output is a table, materialized view, temporary view, or sink. The flow that
populates it is just a `DataFrame`, so you compose it with the usual API. Use
`pipe.read(name)` to read one output from another.

```scala
import spark.implicits.*

// A materialized view backed by a query.
pipe.createMaterializedView("numbers", spark.range(0, 100))

// A table derived from an earlier output.
pipe.createTable("evens", pipe.read("numbers").where($"id" % 2 === 0))

// A non-published temporary view.
pipe.createTemporaryView("odds", pipe.read("numbers").where($"id" % 2 === 1))
```

`createTable` and `createMaterializedView` accept `comment`, `format`, `partitionCols`,
`clusteringColumns`, `tableProperties`, and an explicit `schema`.

You can also register a standalone flow into an existing target, or define the graph
from SQL:

```scala
pipe.defineFlow("more_evens", anotherDataFrame, target = "evens")
pipe.defineSql("CREATE MATERIALIZED VIEW silver AS SELECT * FROM bronze WHERE ok")
```

## Running the pipeline

`startRun` resolves the graph and executes the flows, blocking until the run finishes
and returning the events the server emitted. A storage location (an absolute URI) is
required for checkpoint/metadata:

```scala
val events = pipe.startRun(storage = "file:///tmp/pipeline_storage")
events.foreach(e => println(e.message))
// Flow spark_catalog.default.numbers is RUNNING.
// Flow spark_catalog.default.numbers has COMPLETED.
// ...
// Run is COMPLETED.

spark.read.table("evens").show()
```

`startRun` options:

| Parameter | Meaning |
|-----------|---------|
| `storage` | Absolute URI for checkpoint/metadata (e.g. `file://`, `s3a://`, `hdfs://`) |
| `fullRefresh` | Datasets to reset and recompute |
| `fullRefreshAll` | Reset and recompute everything |
| `refresh` | Datasets to update |
| `dry` | Validate the graph without executing flows |

Drop the graph when done:

```scala
pipe.drop()
```

!!! note "Not supported"
    Flows defined by a user query function (a JVM closure) are not supported, because
    Spark Connect does not transport user code. Define each flow with a relation
    (`DataFrame`) instead.

See the runnable
[`DeclarativePipeline`](https://github.com/HyukjinKwon/spark-connect-scala3/blob/main/examples/src/main/scala/examples/DeclarativePipeline.scala)
example.
