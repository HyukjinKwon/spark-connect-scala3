# Reading & Writing Data

I/O happens **on the server** — paths are resolved relative to the cluster's file
systems, not your local machine.

## Reading

```scala
val df = spark.read
  .format("parquet")
  .load("/data/events")

// format shortcuts
spark.read.parquet("/data/events")
spark.read.json("/data/logs.json")
spark.read.orc("/data/orc")
spark.read.text("/data/lines.txt")

spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/data/people.csv")
```

Provide an explicit schema to avoid inference:

```scala
import org.apache.spark.sql.types.*

val schema = StructType(Seq(
  StructField("id", LongType, nullable = false),
  StructField("name", StringType),
  StructField("ts", TimestampType)
))

val df = spark.read.schema(schema).json("/data/logs")
```

Common reader options: `.option(k, v)`, `.options(map)`, `.schema(structType)`,
`.format(name)`.

## Writing

```scala
df.write
  .format("parquet")
  .mode("overwrite")
  .save("/data/out")

// format shortcuts
df.write.mode("append").parquet("/data/out")
df.write.json("/data/out-json")
df.write.option("header", "true").csv("/data/out-csv")
```

Save modes: `"append"`, `"overwrite"`, `"ignore"`, `"errorifexists"` (default).

### Partitioning & bucketing

```scala
df.write
  .partitionBy("year", "month")
  .mode("overwrite")
  .parquet("/data/partitioned")
```

### Saving to tables

```scala
df.write.saveAsTable("db.events")
df.write.mode("append").insertInto("db.events")
```

## The Catalog

Inspect and manage metadata through `spark.catalog`:

```scala
spark.catalog.listDatabases().show()
spark.catalog.listTables("default").show()
spark.catalog.tableExists("default", "events")
spark.catalog.currentDatabase
spark.catalog.setCurrentDatabase("analytics")
spark.catalog.dropTempView("v")
```

!!! warning "Server-side paths"
    `load`/`save` paths and table locations are interpreted by the Spark Connect
    server. A path like `/data/events` refers to the cluster's storage (HDFS, S3,
    local disk on the server), not your client machine.
