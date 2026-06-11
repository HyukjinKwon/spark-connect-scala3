# Quickstart

This page assumes you have the dependency installed and a Spark Connect server
running (see [Installation](installation.md)). It walks through connecting,
building DataFrames, transforming them, and running actions.

## Connecting

`SparkSession` is the entry point. Build one with the fluent builder:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .remote("sc://localhost:15002")
  .appName("quickstart")
  .config("spark.sql.shuffle.partitions", "8")
  .getOrCreate()
```

- `remote(url)` sets the Spark Connect connection string.
- `appName(name)` sets the application name.
- `config(key, value)` sets a session configuration option (overloads accept
  `String`, `Boolean`, `Long`, and `Double`).
- `getOrCreate()` returns the active session if one exists, otherwise creates
  and activates a new one. Use `create()` to always build a fresh session.

You can inspect the server version and the session id:

```scala
spark.version    // the Spark version reported by the server, e.g. "4.1.2"
spark.sessionId  // the client session id
```

## Importing functions

Column expressions are built with the functions library and the `col` helper.
Bring them into scope with a single import:

```scala
import org.apache.spark.sql.functions.*
```

This gives you `col`, `lit`, `expr`, `when`, and the full function catalog
(`sum`, `avg`, `count`, `round`, `split`, `explode`, `rank`, and many more).

## range

The simplest DataFrame is an integer range with a single `id` column:

```scala
spark.range(5).show()
// +---+
// | id|
// +---+
// |  0|
// |  1|
// |  2|
// |  3|
// |  4|
// +---+
```

`range` accepts `range(end)`, `range(start, end)`, `range(start, end, step)`,
and `range(start, end, step, numPartitions)`:

```scala
spark.range(10, 20, 2).show()   // 10, 12, 14, 16, 18
```

## sql

Run Spark SQL and get back a lazy DataFrame:

```scala
spark.sql("SELECT 1 AS a, 'hello' AS b").show()
// +---+-----+
// |  a|    b|
// +---+-----+
// |  1|hello|
// +---+-----+
```

## A small transformation

Putting the DataFrame API together:

```scala
import org.apache.spark.sql.functions.*

spark.range(1, 1000)
  .select(col("id"), (col("id") % 3).as("bucket"))
  .groupBy("bucket")
  .agg(count("*").as("n"), sum("id").as("total"))
  .orderBy("bucket")
  .show()
// +------+---+------+
// |bucket|  n| total|
// +------+---+------+
// |     0|333|166833|
// |     1|333|166167|
// |     2|333|166500|
// +------+---+------+
```

## Actions: show and collect

DataFrames are lazy - nothing runs on the server until you call an action.

`show` renders a formatted table to stdout:

```scala
val df = spark.range(3)
df.show()                       // first 20 rows, truncated
df.show(5, truncate = false)    // first 5 rows, no truncation
df.show(5, 0, vertical = true)  // one field per line
```

`collect()` executes the plan and returns an `Array[Row]`. A `Row` supports
access by position with typed getters:

```scala
val rows = spark.range(3).collect()
rows.foreach(row => println(row.getLong(0)))
```

Other common actions:

```scala
spark.range(100).count()    // 100
spark.range(100).take(3)    // first 3 Rows
spark.range(100).first()    // the first Row
spark.range(0).isEmpty      // true
```

## Stopping the session

When you are done, release the server-side session:

```scala
spark.stop()
```

A robust pattern wraps the work in `try`/`finally`:

```scala
val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
try {
  spark.range(10).show()
} finally {
  spark.stop()
}
```

## Next steps

- [DataFrames](dataframes.md) for the full transformation and action surface.
- [Columns and Functions](columns-and-functions.md) for the expression library.
- [SQL](sql.md) for running SQL and using views.
