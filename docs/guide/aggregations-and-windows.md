# Aggregations & Windows

This page covers grouped aggregation (`groupBy`, `rollup`, `cube`), the
`RelationalGroupedDataset` interface, and window functions built with `Window`. The API
mirrors Apache Spark's Scala API, so `df.groupBy(...).agg(...)` and
`Window.partitionBy(...)` behave exactly as you would expect.

All examples assume a session and the functions are in scope:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder().remote("sc://localhost:15002").getOrCreate()
import spark.implicits.*
```

A sample DataFrame used throughout this page:

```scala
val employees = Seq(
  ("eng",   "ana",  130, 2023),
  ("eng",   "ben",  110, 2023),
  ("eng",   "cy",   150, 2024),
  ("sales", "dot",   90, 2023),
  ("sales", "evan", 120, 2024)
).toDF("dept", "name", "salary", "year")
```

## Grouping

`DataFrame.groupBy` returns a `RelationalGroupedDataset`. Columns may be given as
`Column`s or names.

```scala
employees.groupBy($"dept").count().show()
// +-----+-----+
// | dept|count|
// +-----+-----+
// |  eng|    3|
// |sales|    2|
// +-----+-----+
```

`rollup` and `cube` produce multi-dimensional aggregates with subtotals. A `rollup` of
`(a, b)` yields groupings for `(a, b)`, `(a)`, and `()`; a `cube` additionally yields
`(b)`.

```scala
employees.rollup($"dept", $"year").agg(sum($"salary").as("total")).orderBy($"dept", $"year").show()
employees.cube($"dept", $"year").count().show()
```

To aggregate the whole DataFrame with no grouping columns, call `DataFrame.agg`
directly:

```scala
employees.agg(sum($"salary").as("total"), avg($"salary").as("avg")).show()
```

## RelationalGroupedDataset

`RelationalGroupedDataset` exposes the standard aggregates. Each method returns a new
`DataFrame`.

### agg

`agg` accepts either aggregate `Column`s or a `Map[String, String]` mapping column names
to function names.

```scala
// Column form (most flexible: supports aliases, expressions, multiple stats).
employees.groupBy($"dept").agg(
  avg($"salary").as("avg_salary"),
  max($"salary").as("max_salary"),
  count("*").as("headcount")
).show()

// Map form: column -> function name.
employees.groupBy($"dept").agg(Map("salary" -> "max", "year" -> "min")).show()
```

### count, sum, avg/mean, max, min

`count()` counts rows per group. `sum`, `avg` (aliased `mean`), `max`, and `min` take
one or more numeric column names; with no arguments they apply to every numeric column.

```scala
employees.groupBy($"dept").count().show()
employees.groupBy($"dept").sum("salary").show()
employees.groupBy($"dept").avg("salary").show()   // avg and mean are equivalent
employees.groupBy($"dept").max("salary", "year").show()
employees.groupBy($"dept").min("salary").show()
```

### pivot

`pivot` rotates the values of a column into separate output columns. Supplying the list
of values explicitly is faster and deterministic, because the client does not have to
scan for distinct values first.

```scala
// Inferred pivot values.
employees.groupBy($"dept").pivot("year").sum("salary").show()

// Explicit pivot values (recommended in production).
employees.groupBy($"dept").pivot("year", Seq(2023, 2024)).sum("salary").show()
// +-----+----+----+
// | dept|2023|2024|
// +-----+----+----+
// |  eng| 240| 150|
// |sales|  90| 120|
// +-----+----+----+
```

## Window functions

Window functions compute a value for each row over a related set of rows (a "window")
without collapsing them into one row per group. Build a `WindowSpec` with the
`Window` factory and attach it to an analytic column with `Column.over`.

`WindowSpec` is immutable: each of `partitionBy`, `orderBy`, `rowsBetween`, and
`rangeBetween` returns a new spec, so you can chain them freely.

```scala
import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy($"dept").orderBy($"salary".desc)
```

### Ranking functions

`row_number()`, `rank()`, and `dense_rank()` are no-argument functions; call them and
attach a window with `over`.

```scala
employees.select(
  $"dept",
  $"name",
  $"salary",
  row_number().over(w).as("row_num"),
  rank().over(w).as("rank"),
  dense_rank().over(w).as("dense_rank")
).show()
```

`percent_rank()`, `cume_dist()`, and `ntile(n)` are also available:

```scala
employees.select(
  $"dept",
  $"salary",
  percent_rank().over(w).as("pct_rank"),
  cume_dist().over(w).as("cume_dist"),
  ntile(2).over(w).as("half")
).show()
```

### Analytic functions: lag and lead

`lag` and `lead` look at preceding or following rows in the window. Both take an optional
offset (default `1`) and an optional default value.

```scala
val ordered = Window.partitionBy($"dept").orderBy($"year")

employees.select(
  $"dept",
  $"year",
  $"salary",
  lag($"salary", 1).over(ordered).as("prev_salary"),
  lead($"salary", 1, 0).over(ordered).as("next_salary")
).show()
```

### Frames: rowsBetween and rangeBetween

A window frame restricts which rows in the partition are included relative to the current
row. `rowsBetween` counts physical rows; `rangeBetween` compares values of the ordering
column. Use the boundary constants for unbounded edges and the current row.

```scala
val running = Window
  .partitionBy($"dept")
  .orderBy($"year")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

employees.select(
  $"dept",
  $"year",
  $"salary",
  sum($"salary").over(running).as("running_total")
).show()
```

The boundary constants are:

| Constant | Meaning |
|----------|---------|
| `Window.unboundedPreceding` | start of the partition |
| `Window.unboundedFollowing` | end of the partition |
| `Window.currentRow` | the current row (offset `0`) |

Any integer is also valid as an offset, e.g. `rowsBetween(-1, 1)` for a three-row sliding
window. `rangeBetween` uses the same boundaries but interprets offsets as values of the
ordering expression:

```scala
val salaryBand = Window.partitionBy($"dept").orderBy($"salary").rangeBetween(-20, 20)

employees.select(
  $"name",
  $"salary",
  count("*").over(salaryBand).as("peers_within_20")
).show()
```

## See also

- [Columns & Functions](columns-and-functions.md) for the full expression DSL and the
  aggregate and analytic functions.
- [DataFrame & Dataset](dataframe.md) for the underlying transformation API.
- [Catalog](catalog.md) for inspecting the tables you aggregate over.
