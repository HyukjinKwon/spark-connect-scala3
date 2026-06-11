# Data Sources

Spark Connect for Scala 3 reads and writes data through `DataFrameReader` and
`DataFrameWriter`, mirroring Apache Spark's Scala API. Paths and tables are
resolved on the **server's** filesystem and catalog, not on your local machine.

## Reading

Configure a reader with a format and options, then load:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

val df = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load("/data/people.csv")
```

Format shortcuts cover the common sources:

```scala
spark.read.parquet("/data/events")
spark.read.json("/data/events.json")
spark.read.csv("/data/people.csv")
spark.read.orc("/data/events.orc")
spark.read.text("/data/log.txt")
```

### Schemas

Let Spark infer the schema, or provide one explicitly with a DDL string or a
`StructType`:

```scala
import org.apache.spark.sql.types.*

// DDL string.
spark.read.schema("id INT, name STRING, score DOUBLE").csv("/data/people.csv")

// StructType.
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType),
  StructField("score", DoubleType)))

spark.read.schema(schema).csv("/data/people.csv")
```

### Options

`option` accepts `String`, `Boolean`, `Long`, and `Double` values; `options`
takes a map:

```scala
spark.read
  .format("csv")
  .options(Map(
    "header" -> "true",
    "sep" -> ";",
    "nullValue" -> "NA"))
  .load("/data/people.csv")
```

## Writing

A `DataFrameWriter` is configured with a format, save mode, and options, then a
terminal call writes the data:

```scala
val df = spark.range(100)

df.write
  .format("parquet")
  .mode("overwrite")
  .save("/data/numbers")
```

Format shortcuts:

```scala
df.write.mode("overwrite").parquet("/data/numbers")
df.write.mode("append").json("/data/numbers.json")
df.write.csv("/data/numbers.csv")
```

### Save modes

`mode` accepts a string or a `SaveMode`:

| Mode | Behavior |
| ---- | -------- |
| `overwrite` | Replace existing data at the path. |
| `append` | Add to existing data. |
| `ignore` | Do nothing if data already exists. |
| `error` / `errorifexists` | Fail if data already exists (the default). |

```scala
import org.apache.spark.sql.SaveMode

df.write.mode(SaveMode.Overwrite).parquet("/data/numbers")
```

### Partitioning and bucketing

```scala
df.write
  .partitionBy("country", "year")
  .parquet("/data/sales")

df.write
  .bucketBy(8, "id")
  .sortBy("id")
  .saveAsTable("bucketed_table")
```

### Tables

Write to a managed or external table in the catalog:

```scala
df.write.mode("overwrite").saveAsTable("analytics.daily_summary")
df.write.insertInto("analytics.daily_summary")
```

## A round trip

```scala
val people = spark.sql(
  "SELECT * FROM VALUES (1, 'Ada', 36), (2, 'Alan', 41) AS t(id, name, age)")

people.write.mode("overwrite").parquet("/tmp/people")

spark.read.parquet("/tmp/people")
  .filter(org.apache.spark.sql.functions.col("age") >= 40)
  .show()
```
