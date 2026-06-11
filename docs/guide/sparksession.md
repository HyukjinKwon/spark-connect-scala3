# SparkSession

`SparkSession` is the entry point. With Spark Connect it is a *thin* handle over a
gRPC connection to a remote server - creating one does not start a Spark driver in
your process.

## Building a session

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .getOrCreate()
```

`remote(...)` takes a Spark Connect connection string (see
[Configuration](../reference/configuration.md) for all options):

```
sc://host:port/;param1=value1;param2=value2
```

Common parameters:

| Parameter  | Meaning                                            |
|------------|----------------------------------------------------|
| `token`    | Bearer token for authentication (implies TLS)      |
| `use_ssl`  | `true` to use TLS                                  |
| `user_id`  | User identity to attach to the session             |
| `user_agent` | Overrides the client user-agent                  |
| `session_id` | Reconnect to an existing server-side session     |

If `remote(...)` is omitted, the builder falls back to the `SPARK_REMOTE`
environment variable, then to `sc://localhost:15002`.

## Configuration

Set Spark configuration on the builder or at runtime:

```scala
val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .config("spark.sql.shuffle.partitions", "8")
  .getOrCreate()

// runtime config
spark.conf.set("spark.sql.session.timeZone", "UTC")
val tz = spark.conf.get("spark.sql.session.timeZone")
```

## Creating data

```scala
// A range relation, computed on the server.
val r = spark.range(0, 100, step = 2)

// From local Scala data (shipped to the server as a local relation).
import spark.implicits.*
val df = Seq((1, "a"), (2, "b")).toDF("id", "label")

// From rows + an explicit schema.
import org.apache.spark.sql.types.*
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType)
))
val people = spark.createDataFrame(rows, schema)
```

## Running SQL

```scala
spark.sql("SELECT 1 AS one").show()
spark.sql("CREATE TEMP VIEW v AS SELECT * FROM range(10)")
spark.table("v").count()
```

See [Running SQL](sql.md) for more.

## Implicits

`import spark.implicits.*` brings in:

- the `$"col"` string interpolator for columns,
- `.toDF(...)` on local Scala collections,
- conversions used by the typed `Dataset` API.

## Lifecycle

```scala
spark.stop()   // closes the gRPC channel and releases the server-side session
```

A session is safe to share across threads. Closing it invalidates derived
`DataFrame`s.
