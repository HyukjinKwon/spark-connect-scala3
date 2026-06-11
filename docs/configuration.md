# Configuration and Connection

## Connection strings

You connect by giving the builder a Spark Connect connection string. The grammar
matches the official Spark Connect clients:

```
sc://host[:port][/;param=value;param=value...]
```

The host is required; the port defaults to **15002**. Parameters live after a
`/` and are separated by `;`. Common parameters:

| Parameter    | Meaning                                                         |
| ------------ | --------------------------------------------------------------- |
| `token`      | Bearer token. Implies TLS and adds an `authorization` header.   |
| `user_id`    | The Spark user id.                                              |
| `user_agent` | Client user agent.                                              |
| `use_ssl`    | `true` / `false` to force TLS on or off.                        |
| `session_id` | Reuse a specific server-side session id.                        |

Examples:

```scala
// Plain local server, no TLS.
SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

// A remote host with a bearer token (TLS is implied by the token).
SparkSession.builder
  .remote("sc://spark.example.com:443/;token=abc123;user_id=alice")
  .getOrCreate()

// Force TLS on and set a custom user agent.
SparkSession.builder
  .remote("sc://spark.example.com:15002/;use_ssl=true;user_agent=my-app/1.0")
  .getOrCreate()
```

## Building a session

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .remote("sc://localhost:15002")
  .appName("analytics")
  .userAgent("analytics-job/1.0")
  .config("spark.sql.shuffle.partitions", "8")
  .getOrCreate()
```

- `remote(url)` sets the connection string.
- `appName(name)` sets the application name.
- `userAgent(agent)` sets the gRPC user agent.
- `config(key, value)` sets a session configuration option. Overloads accept
  `String`, `Boolean`, `Long`, and `Double` values.
- `getOrCreate()` returns the active session if one exists, otherwise creates
  and activates a new session. `create()` always builds a fresh session.

## Runtime configuration

`spark.conf` is the live, server-side configuration for the session:

```scala
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.sql.adaptive.enabled", true)

spark.conf.get("spark.sql.shuffle.partitions")          // throws if unset
spark.conf.get("spark.sql.shuffle.partitions", "200")   // with a default
spark.conf.getOption("spark.sql.shuffle.partitions")    // Option[String]
spark.conf.getAll                                       // Map[String, String]

spark.conf.unset("spark.sql.shuffle.partitions")
```

## Session lifecycle

```scala
spark.version    // the Spark version reported by the server
spark.sessionId  // the client session id
spark.setActive()  // mark this session as the active one

spark.stop()     // release the server-side session and close the connection
```

A robust pattern wraps the work in `try`/`finally`:

```scala
val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
try {
  // ... work ...
} finally {
  spark.stop()
}
```

## Pointing examples and tests at a server

The example programs and integration tests read the connection string from an
environment variable so you can retarget them without editing code:

```bash
export SPARK_REMOTE=sc://localhost:15002
sbt "examples/runMain examples.QuickStart"

SPARK_CONNECT_TEST_REMOTE=sc://localhost:15002 sbt "client/testOnly *Integration*"
```
