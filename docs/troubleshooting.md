# Troubleshooting

## Apache Arrow: InaccessibleObjectException on the first action

Results are decoded with Apache Arrow, which reaches into internal `java.nio`
buffers. On JDK 17 and newer an unconfigured JVM throws on the first `collect()`,
`show()`, or `count()`, for example:

```
java.lang.RuntimeException: Failed to initialize MemoryUtil
Caused by: java.lang.InaccessibleObjectException: Unable to make ... accessible:
module java.base does not "opens java.nio" to unnamed module
```

Open the two modules on the application JVM:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

In sbt set them on the forked JVM (`javaOptions ++= ...` with `run / fork := true`);
for a packaged app pass them to `java` directly. See
[Installation](installation.md#jvm-flags-for-apache-arrow).

## gRPC connection problems

- **Connection refused / `UNAVAILABLE`:** confirm the server is running and
  reachable at the host and port in your `sc://` URL (default port `15002`).
  Test with `bash -c "</dev/tcp/HOST/PORT"`.
- **Hangs on the first request:** the client retries transient failures with
  backoff, so a wrong host retries several times before failing. Re-check the
  endpoint passed to `.remote("sc://host:port")`.
- **TLS errors:** a `token` in the connection string implies TLS. Use a plain
  `sc://host:port` for a local plaintext server, or append `use_ssl=true` for a
  TLS endpoint (`sc://host:443/;use_ssl=true;token=...`). See
  [Configuration and Connection](configuration.md).

## Version compatibility

The client is built against the Spark Connect 4.1 protocol and is tested against
Apache Spark 3.5, 4.0, and 4.1. The protocol is backward compatible, but features
added in a later server release are unavailable on older servers:

- **Declarative Pipelines** require Spark 4.1 or newer.
- Observed metrics (`Dataset.observe`) require Spark 4.0 or newer.

If the server rejects a relation or function, check the server version with
`spark.version`.

## createDataFrame type errors

When building a DataFrame from local data without an explicit schema, the schema
is inferred from the values. If a column mixes types or is entirely null, pass an
explicit `StructType` (or a DDL string such as `"id BIGINT, name STRING"`):

```scala
import org.apache.spark.sql.types.*
val schema = StructType(Seq(
  StructField("id", LongType),
  StructField("name", StringType)))
spark.createDataFrame(rows, schema)
```

See [Data Sources](data-sources.md) for reading from files instead.

## Closures, UDFs, and foreach are not supported

Operations that ship user code to the server are not available over Spark
Connect from this client: the closure forms of `Dataset.map`/`flatMap`/`filter`/
`reduce`/`mapPartitions`, user-defined functions, and the streaming
`foreach`/`foreachBatch` sinks. They throw `UnsupportedOperationException` with a
clear message. Use the column- and SQL-expression APIs (`select`, `selectExpr`,
`withColumn`, the `functions` library) instead, which run entirely on the server.

## Scala 2 vs Scala 3 artifacts

This client is Scala 3 only, published as `spark-connect-scala3-client_3`. The
official `spark-connect-client-jvm` is Scala 2.12/2.13 and cannot be used from a
Scala 3 build, which is the reason this project exists.

## Getting help

Open an issue at
<https://github.com/HyukjinKwon/spark-connect-scala3/issues> with your Scala
version, JDK version, Spark server version (`spark.version`), and a minimal
reproduction.
