# DataFrame & Dataset

A `DataFrame` (`= Dataset[Row]`) is a **lazy** description of a computation. Each
transformation builds a new protobuf relation; nothing runs until you call an
**action**.

```scala
val df = spark.range(0, 100)
  .withColumn("bucket", $"id" % 5)   // transformation (lazy)
  .filter($"bucket" =!= 0)           // transformation (lazy)

df.show()                            // action -> sends ExecutePlan, returns data
```

## Transformations

These return a new `DataFrame` and do not contact the server.

| Method | Description |
|--------|-------------|
| `select(cols*)` / `selectExpr(exprs*)` | Project columns / SQL expressions |
| `filter(cond)` / `where(cond)` | Keep rows matching a `Column` or SQL string |
| `withColumn(name, col)` | Add or replace a column |
| `withColumnRenamed(old, new)` | Rename a column |
| `drop(cols*)` | Remove columns |
| `distinct()` / `dropDuplicates(cols*)` | De-duplicate |
| `orderBy(cols*)` / `sort(cols*)` | Sort |
| `limit(n)` | Keep the first `n` rows |
| `groupBy(cols*)` | Start an aggregation (see below) |
| `join(other, cond, type)` | Join two frames |
| `union(other)` / `unionByName(other)` | Set union |
| `intersect(other)` / `except(other)` | Set operations |
| `sample(fraction)` | Random sample |
| `repartition(n)` / `coalesce(n)` | Change partitioning |
| `na` | Null-handling sub-API (`drop`, `fill`, `replace`) |

```scala
val joined = orders
  .join(customers, orders("cust_id") === customers("id"), "left")
  .select(customers("name"), orders("amount"))
  .orderBy($"amount".desc)
```

## Aggregations

```scala
import org.apache.spark.sql.functions.*

val byKey = events
  .groupBy($"country")
  .agg(
    count("*").as("n"),
    sum($"amount").as("total"),
    avg($"amount").as("avg"),
    max($"ts").as("last_seen")
  )
```

`groupBy` returns a `RelationalGroupedDataset`, which also offers shortcuts such as
`count()`, `sum(cols*)`, `avg(cols*)`, `max(cols*)`, `min(cols*)`, `agg(Map)`, and
`pivot(col, values)`.

## Actions

Actions trigger execution and bring results back (decoded from Apache Arrow):

| Action | Returns |
|--------|---------|
| `show(numRows, truncate, vertical)` | Pretty-prints to stdout |
| `collect()` | `Array[Row]` |
| `collectAsList()` | `java.util.List[Row]` |
| `count()` | `Long` |
| `take(n)` / `head(n)` | `Array[Row]` |
| `head()` / `first()` | `Row` |
| `isEmpty` | `Boolean` |
| `toLocalIterator()` | `java.util.Iterator[Row]` (streamed) |
| `foreach(f)` / `foreachPartition(f)` | Unit |

```scala
val rows: Array[Row] = df.collect()
rows.foreach(r => println(s"${r.getLong(0)} -> ${r.getString(1)}"))

val n: Long = df.count()
```

!!! tip "Stream large results"
    `collect()` materialises everything in memory. For large result sets use
    `toLocalIterator()`, which pulls Arrow batches lazily.

## Working with `Row`

```scala
val r: Row = df.first()
r.getInt(0)
r.getString(1)
r.getAs[Long]("id")
r.isNullAt(2)
r.toSeq           // Seq[Any]
```

`Row` mirrors Apache Spark's `org.apache.spark.sql.Row`. See
[Data Types & Schemas](types.md) for the type mapping.

## Inspecting the plan

```scala
df.printSchema()
df.schema           // StructType
df.columns          // Array[String]
df.explain()        // server-side query plan
```
