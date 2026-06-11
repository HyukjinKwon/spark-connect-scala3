# Architecture

`spark-connect-scala3` is a client implementation of the
[Spark Connect protocol](https://spark.apache.org/docs/latest/spark-connect-overview.html).
It contains no query engine — all execution happens on the remote server.

## The big picture

```
┌──────────────────────────────────────────┐
│  Your Scala 3 application                  │
│                                            │
│  SparkSession ── DataFrame ── Column       │   builds a protobuf
│                    │                       │   Relation tree (lazy)
└────────────────────┼───────────────────────┘
                     │  ExecutePlan / AnalyzePlan (gRPC, HTTP/2)
                     ▼
┌──────────────────────────────────────────┐
│  Spark Connect server (driver)             │
│   parses the plan → Catalyst → executes    │
└────────────────────┬───────────────────────┘
                     │  Arrow IPC record batches
                     ▼
        decoded into org.apache.spark.sql.Row
```

## Modules

| Module | Package | Responsibility |
|--------|---------|----------------|
| `proto` | `org.apache.spark.connect.proto` | ScalaPB-generated messages + gRPC stubs from the vendored Spark Connect `.proto` files (Spark 4.0.0). |
| `client` | `org.apache.spark.sql.*` | Public API: `SparkSession`, `Dataset`/`DataFrame`, `Column`, `functions`, `types`, readers/writers, `Catalog`. |
| `client` (internal) | `org.apache.spark.sql.connect.client.*` | gRPC channel/transport (`SparkConnectClient`), Arrow result decoding (`SparkResult`), plan building. |

## How a query flows

1. **Build.** Every transformation (`select`, `filter`, …) wraps the current
   `proto.Relation` in a new relation. A `Column` is a `proto.Expression`. This is
   pure, lazy tree-building — no I/O.
2. **Trigger.** An action (`collect`, `show`, `count`, …) wraps the relation in a
   `proto.Plan` and issues an `ExecutePlan` gRPC call.
3. **Stream.** The server responds with a stream of `ExecutePlanResponse` messages.
   Result messages carry **Apache Arrow** IPC batches.
4. **Decode.** `SparkResult` reads each batch with an `ArrowStreamReader` and maps
   Arrow vectors to JVM values, yielding `Row`s — eagerly (`collect`) or lazily
   (`toLocalIterator`).

Schema/metadata queries (`schema`, `printSchema`, `explain`) use `AnalyzePlan`
instead of `ExecutePlan`.

## Transport

- **gRPC over HTTP/2** via `grpc-java` (Netty).
- The connection string (`sc://host:port/;k=v;…`) configures host, TLS, auth token,
  and user/session identity.
- Calls carry a `UserContext` and a client-generated `session_id` so the server can
  maintain session state (temp views, config) across requests.

## Why Arrow?

Arrow's columnar IPC format lets the server serialise result batches once and the
client decode them with zero per-value parsing overhead — the same mechanism the
official Spark Connect clients use. On JDK 17+ Arrow needs
`--add-opens=java.base/java.nio=ALL-UNNAMED` to access direct buffers (the build sets
this for tests and runs).

## What's intentionally *not* here

- No Catalyst optimizer, no execution engine, no RDD API — those are the server's job.
- No Spark JARs on your classpath. The only runtime dependencies are gRPC, Arrow,
  ScalaPB runtime, and SLF4J.
