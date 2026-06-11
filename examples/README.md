# Examples

Runnable example programs for `spark-connect-scala3`. Each connects to the server
named by `SPARK_REMOTE` (default `sc://localhost:15002`).

## Running

Start a Spark Connect server (see the [getting started guide](../docs/getting-started.md)),
then:

```bash
export SPARK_REMOTE="sc://localhost:15002"

sbt "examples/runMain examples.QuickStart"
sbt "examples/runMain examples.WordCount"
sbt "examples/runMain examples.Aggregations"
sbt "examples/runMain examples.SqlAndViews"
sbt "examples/runMain examples.ReadWrite"
sbt "examples/runMain examples.WindowFunctions"
```

## Index

| Example | What it shows |
|---------|---------------|
| [`QuickStart`](src/main/scala/examples/QuickStart.scala) | Build a session, project a column, `show`/`count`. |
| [`WordCount`](src/main/scala/examples/WordCount.scala) | `split` + `explode` + `groupBy`/`count`. |
| [`Aggregations`](src/main/scala/examples/Aggregations.scala) | Multi-aggregate `groupBy`, conditional aggregation. |
| [`SqlAndViews`](src/main/scala/examples/SqlAndViews.scala) | Temp views, raw + parameterised SQL. |
| [`ReadWrite`](src/main/scala/examples/ReadWrite.scala) | Parquet write/read on the server filesystem. |
| [`WindowFunctions`](src/main/scala/examples/WindowFunctions.scala) | `Window`, `rank`, `dense_rank`, running totals. |

All examples are compiled in CI, so they stay in sync with the API.
