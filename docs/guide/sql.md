# Running SQL

You can mix the DataFrame API with raw SQL freely - both compile to the same
server-side plans.

## Executing SQL

```scala
val df = spark.sql("SELECT id, id * 2 AS doubled FROM range(10)")
df.show()
```

`sql(...)` returns a `DataFrame`, so you can keep chaining:

```scala
spark.sql("SELECT * FROM events")
  .filter($"country" === "KR")
  .groupBy($"day")
  .count()
  .show()
```

## Parameterised SQL

Avoid string interpolation for user input - use named or positional parameters,
which are sent to the server separately:

```scala
// named parameters
spark.sql(
  "SELECT * FROM events WHERE country = :c AND day >= :d",
  Map("c" -> "KR", "d" -> "2026-01-01")
)

// positional parameters
spark.sql(
  "SELECT * FROM events WHERE country = ? AND amount > ?",
  Array("KR", 100)
)
```

## Temporary views

Register a `DataFrame` as a view to query it by name:

```scala
val df = spark.range(100).withColumn("bucket", $"id" % 4)

df.createOrReplaceTempView("nums")
spark.sql("SELECT bucket, count(*) AS n FROM nums GROUP BY bucket").show()

// global temp views live in the `global_temp` database for the session's lifetime
df.createGlobalTempView("g_nums")
spark.sql("SELECT * FROM global_temp.g_nums").show()
```

## DDL and commands

```scala
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("CREATE TABLE analytics.t (id INT, name STRING) USING parquet")
spark.sql("INSERT INTO analytics.t VALUES (1, 'a'), (2, 'b')")
spark.sql("SHOW TABLES IN analytics").show()
```

Commands that don't return rows (like `CREATE`/`INSERT`) execute immediately when
`sql(...)` is called; the returned `DataFrame` is typically empty.

## Tables

```scala
spark.table("analytics.t").show()      // read a table as a DataFrame
spark.catalog.listColumns("analytics", "t").show()
```
