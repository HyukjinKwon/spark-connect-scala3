# DataFrames

A `DataFrame` is an alias for `Dataset` of `Row` - an untyped, columnar table of
data. DataFrames are lazy: transformations build up a logical plan, and nothing
executes on the server until you call an action.

All examples below assume:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

## Inspecting a DataFrame

```scala
val df = spark.range(10).select(col("id"), (col("id") * 2).as("doubled"))

df.schema        // a StructType describing the columns
df.columns       // Array("id", "doubled")
df.dtypes        // Array(("id", "bigint"), ("doubled", "bigint"))
df.printSchema()
df.isStreaming   // false for a batch DataFrame
```

`explain` prints the query plan, which is useful when tuning:

```scala
df.explain()                  // simple plan
df.explain(extended = true)   // parsed, analyzed, optimized, and physical plans
df.explain("formatted")
```

## Selecting and projecting

```scala
val df = spark.range(5)

df.select(col("id"), (col("id") + 1).as("next")).show()
df.select("id").show()
df.selectExpr("id", "id * id AS squared").show()
```

## Filtering

`filter` and `where` are equivalent and accept either a `Column` or a SQL
expression string:

```scala
spark.range(10).filter(col("id") % 2 === 0).show()
spark.range(10).where("id >= 5").show()
```

## Adding, renaming, and dropping columns

```scala
val df = spark.range(5)

df.withColumn("squared", col("id") * col("id")).show()
df.withColumnRenamed("id", "n").show()
df.withColumnsRenamed(Map("id" -> "n")).show()
df.withColumn("squared", col("id") * col("id")).drop("id").show()
```

## Sorting and limiting

```scala
val df = spark.range(100)

df.orderBy(col("id").desc).show()
df.sort("id").show()
df.limit(5).show()
df.offset(10).limit(5).show()
df.sortWithinPartitions(col("id")).show()
```

## Distinct and deduplication

```scala
val nums = spark.sql("SELECT * FROM VALUES (1), (1), (2), (3) AS t(x)")

nums.distinct().show()
nums.dropDuplicates("x").show()
```

## Joins

`join` supports inner joins, equi-joins on shared column names, and arbitrary
join expressions with a join type.

```scala
val left = spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)")
val right = spark.sql("SELECT * FROM VALUES (1, 10), (2, 20) AS t(id, score)")

// Inner join on a shared column.
left.join(right, "id").show()

// Join on multiple shared columns.
left.join(right, Seq("id")).show()

// Join with an explicit type and condition.
left.join(right, left.col("id") === right.col("id"), "left").show()

// Cartesian product.
left.crossJoin(right).show()
```

Supported join types include `inner`, `cross`, `left`/`left_outer`,
`right`/`right_outer`, `full`/`full_outer`, `left_semi`, and `left_anti`.

## Grouping and aggregation

`groupBy` returns a `RelationalGroupedDataset`; call `agg` (or a named shortcut)
to produce results:

```scala
val sales = spark.sql(
  "SELECT * FROM VALUES ('KR', 12.0), ('KR', 9.0), ('US', 15.0) AS t(country, amount)")

sales.groupBy("country")
  .agg(
    count("*").as("orders"),
    round(sum("amount"), 2).as("total"),
    round(avg("amount"), 2).as("avg"))
  .orderBy("country")
  .show()

// Named shortcuts.
sales.groupBy("country").sum("amount").show()
sales.groupBy("country").count().show()
```

`rollup` and `cube` produce subtotals across grouping combinations:

```scala
sales.rollup("country").agg(sum("amount").as("total")).show()
sales.cube("country").agg(sum("amount").as("total")).show()
```

### Pivoting

```scala
val data = spark.sql(
  "SELECT * FROM VALUES ('KR', 'book', 2), ('KR', 'pen', 5), ('US', 'book', 3) AS t(country, item, qty)")

data.groupBy("country").pivot("item").sum("qty").show()
```

## Set operations

```scala
val a = spark.range(0, 5)
val b = spark.range(3, 8)

a.union(b).distinct().orderBy("id").show()
a.intersect(b).show()
a.except(b).show()
a.unionByName(b).show()
```

## Sampling and repartitioning

```scala
spark.range(1000).sample(fraction = 0.1, seed = 42).count()
spark.range(1000).repartition(4).show()
spark.range(1000).coalesce(1).show()
```

## Temporary views

Register a DataFrame as a view, then query it with SQL:

```scala
spark.range(0, 20).withColumn("bucket", col("id") % 4)
  .createOrReplaceTempView("nums")

spark.sql("SELECT bucket, count(*) AS n FROM nums GROUP BY bucket ORDER BY bucket").show()
```

`createGlobalTempView` registers a view in the cross-session `global_temp`
database.

## Actions

```scala
val df = spark.range(100)

df.collect()          // Array[Row]
df.collectAsList()    // java.util.List[Row]
df.count()            // Long
df.take(5)            // Array[Row]
df.head()             // Row
df.first()            // Row
df.isEmpty            // Boolean
df.toLocalIterator()  // streams Rows without materializing all at once
```

Reading values out of a `Row` uses positional, typed getters:

```scala
val row = spark.sql("SELECT 1 AS a, 'x' AS b").first()
row.getInt(0)      // 1
row.getString(1)   // "x"
```

## Typed Datasets (`as[T]` and `createDataset`)

A `DataFrame` is a `Dataset[Row]`. You can reinterpret it as a typed `Dataset[T]`
with `as[T]`, or build one from local values with `spark.createDataset`. Encoders
are derived at compile time for case classes, tuples, primitives, `Option`,
collections, and maps. This is a purely client-side decoding step, so no closure is
sent to the server.

```scala
import org.apache.spark.sql.SparkSession

case class Person(id: Long, name: String)

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

// Reinterpret query results as a typed Dataset.
val people: Array[Person] =
  spark.sql("SELECT 1L AS id, 'alice' AS name").as[Person].collect()

// Build a Dataset from local values and ship it to the server.
val ds = spark.createDataset(Seq(Person(1L, "alice"), Person(2L, "bob")))
ds.collect()      // Array[Person]
ds.head()         // Person(1, alice)

// Primitives and tuples work too.
spark.range(5).as[Long].collect()                       // Array[Long]
spark.sql("SELECT 1 AS a, 'x' AS b").as[(Int, String)]  // Dataset[(Int, String)]
```

Typed *actions* (`collect`, `head`, `first`, `take`, `toLocalIterator`,
`collectAsList`) return `T`. Note that the typed transformations that take a Scala
function -- `map`, `flatMap`, `mapPartitions`, `groupByKey`, `reduce` -- are not
supported, because they run a closure on the server (the UDF mechanism). Chain the
relational operators (`select`, `filter`, ...) and call `as[T]` at the end.
