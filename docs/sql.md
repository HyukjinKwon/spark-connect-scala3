# SQL

`spark.sql(...)` runs Spark SQL on the server and returns a lazy `DataFrame`.
You can freely mix SQL with the DataFrame API.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

## Running SQL

```scala
spark.sql("SELECT 1 AS a, 'hello' AS b").show()
// +---+-----+
// |  a|    b|
// +---+-----+
// |  1|hello|
// +---+-----+
```

The result is an ordinary DataFrame, so you can keep transforming it:

```scala
spark.sql("SELECT id FROM range(100)")
  .filter(col("id") % 7 === 0)
  .count()
```

## Parameterized SQL

Keep user input out of the query string by passing parameters. Use a `Map` for
named parameters (referenced as `:name`) or an `Array` for positional
parameters (referenced as `?`).

```scala
// Named parameters.
spark.sql(
  "SELECT * FROM range(100) WHERE id >= :lo AND id < :hi",
  Map("lo" -> 10, "hi" -> 13)).show()

// Positional parameters.
spark.sql("SELECT * FROM range(100) WHERE id = ?", Array(42)).show()
```

## Temporary views

Register a DataFrame as a view, then query it with SQL. This is the bridge
between the DataFrame API and raw SQL:

```scala
val nums = spark.range(0, 20).withColumn("bucket", col("id") % 4)
nums.createOrReplaceTempView("nums")

spark.sql("""
  SELECT bucket, count(*) AS n, avg(id) AS avg_id
  FROM nums
  GROUP BY bucket
  ORDER BY bucket
""").show()
```

`createOrReplaceTempView` replaces an existing view of the same name;
`createTempView` fails if the name is taken.

### Global temporary views

A global temporary view is visible to all sessions sharing the same Spark
application and lives in the `global_temp` database:

```scala
nums.createOrReplaceGlobalTempView("nums_global")
spark.sql("SELECT count(*) FROM global_temp.nums_global").show()
```

## Common SQL patterns

```scala
// Inline data with VALUES.
spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)").show()

// Generated ranges.
spark.sql("SELECT id, id * id AS squared FROM range(5)").show()

// Aggregation with HAVING.
spark.sql("""
  SELECT bucket, count(*) AS n
  FROM nums
  GROUP BY bucket
  HAVING count(*) > 4
""").show()
```
