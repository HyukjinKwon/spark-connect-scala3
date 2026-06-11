# Configuration

## Connection string

`remote(...)` accepts a Spark Connect connection string:

```
sc://<host>:<port>/;<param1>=<value1>;<param2>=<value2>;...
```

- The scheme is always `sc://`.
- Port defaults to **15002** if omitted.
- Parameters follow the path, separated by `;`.

Examples:

```scala
// local, no TLS
.remote("sc://localhost:15002")

// remote with TLS and a bearer token
.remote("sc://spark.example.com:443/;use_ssl=true;token=eyJhbGci...")

// attach a user id and reuse a session
.remote("sc://host:15002/;user_id=alice;session_id=2c8f...")
```

### Recognised parameters

| Parameter | Type | Meaning |
|-----------|------|---------|
| `use_ssl` | bool | Use TLS for the channel |
| `token` | string | Bearer token (implies `use_ssl=true`) |
| `user_id` | string | User identity attached to requests |
| `user_agent` | string | Overrides the client user-agent |
| `session_id` | string | Reconnect to an existing server-side session |
| `grpc_max_message_size` | int | Max inbound gRPC message size (bytes) |

## Environment variables

| Variable | Effect |
|----------|--------|
| `SPARK_REMOTE` | Default connection string when `remote(...)` is not called |

```scala
// Reads SPARK_REMOTE, else sc://localhost:15002
val spark = SparkSession.builder().getOrCreate()
```

## Spark runtime configuration

Spark SQL configuration is set on the builder or at runtime and applies to the
**server-side** session:

```scala
val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .config("spark.sql.shuffle.partitions", "16")
  .config("spark.sql.session.timeZone", "UTC")
  .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.get("spark.sql.shuffle.partitions")  // "16"
```

## JVM flags for Apache Arrow (JDK 17+)

Decoding Arrow result batches requires access to internal NIO classes. The build
already adds these for `test` and `run`; if you embed the client in your own app,
add them to your launch flags:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

## Logging

The client logs through SLF4J. Add a binding (e.g. `slf4j-simple` or Logback) to see
output, and set levels as usual:

```scala
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.16"
```
