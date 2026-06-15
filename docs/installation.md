# Installation

## Requirements

- **JDK 17 or newer** (Temurin is the reference JDK).
- **Scala 3.3.x** (the Scala 3 LTS line). The client is published with the
  `_3` Scala suffix.
- A reachable **Spark Connect server** running Apache Spark 4.0.x or 4.1.x.

## Add the dependency

The current release is `0.2.0`. Scala 3 artifacts carry the `_3` suffix, so the
fully qualified Maven artifact id is `spark-connect-scala3-client_3`.

sbt (`build.sbt`):

```scala
libraryDependencies += "com.github.hyukjinkwon" %% "spark-connect-scala3-client" % "0.2.0"
```

sbt with an explicit Scala suffix:

```scala
libraryDependencies += "com.github.hyukjinkwon" % "spark-connect-scala3-client_3" % "0.2.0"
```

Maven (`pom.xml`):

```xml
<dependency>
  <groupId>com.github.hyukjinkwon</groupId>
  <artifactId>spark-connect-scala3-client_3</artifactId>
  <version>0.2.0</version>
</dependency>
```

Gradle (`build.gradle.kts`):

```kotlin
implementation("com.github.hyukjinkwon:spark-connect-scala3-client_3:0.2.0")
```

Mill (`build.sc`):

```scala
ivy"com.github.hyukjinkwon::spark-connect-scala3-client:0.2.0"
```

The published versions are listed on the
[Maven Central page](https://central.sonatype.com/artifact/com.github.hyukjinkwon/spark-connect-scala3-client_3).

## JVM flags for Apache Arrow

Results are decoded with Apache Arrow, which performs off-heap memory access. On
JDK 17 and newer you must open two JDK modules for Arrow. Add these to your run
configuration:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

In sbt, set them on the forked JVM:

```scala
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
```

If you skip these flags you will see an `InaccessibleObjectException` the first
time a result is decoded.

## Running a local Spark Connect server

The client talks to a remote server; it does not start Spark for you. The
quickest way to get a server is to download a Spark distribution and run the
bundled Connect server.

```bash
# Download a Spark distribution (4.1.2 shown here).
curl -L https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz -o spark.tgz
tar xzf spark.tgz
cd spark-4.1.2-bin-hadoop3

# Start the Connect server (requires Java 17+).
# Spark 4.0+ bundles the Connect server, so no extra packages are needed.
./sbin/start-connect-server.sh
```

The server listens on `sc://localhost:15002` by default. Stop it with
`./sbin/stop-connect-server.sh`.

### Spark 3.5.x

On Spark 3.5.x the Connect server is not bundled. Use a Scala 2.13 distribution
and pull the server package in explicitly:

```bash
./sbin/start-connect-server.sh --packages "org.apache.spark:spark-connect_2.13:3.5.5"
```

## Verify the connection

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
println(spark.version)   // the Spark version reported by the server
spark.range(5).show()
spark.stop()
```

## Try it in a Scala REPL

With a Connect server running, the quickest way to explore is a plain Scala REPL
with the client on the classpath. Any REPL works; the examples below use
[scala-cli](https://scala-cli.virtuslab.org/) and [Ammonite](https://ammonite.io/).
Apache Arrow needs two JDK modules opened on the REPL's JVM (see
[the flags above](#jvm-flags-for-apache-arrow)).

### scala-cli

```bash
scala-cli repl \
  --dep com.github.hyukjinkwon::spark-connect-scala3-client:0.2.0 \
  --java-opt --add-opens=java.base/java.nio=ALL-UNNAMED \
  --java-opt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

### Ammonite

```bash
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" amm
```

```scala
import $ivy.`com.github.hyukjinkwon::spark-connect-scala3-client:0.2.0`
```

### Connect a session and explore

In either REPL, build a `SparkSession` pointed at your server and go (swap in your
own `sc://host:port`):

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
import spark.implicits.*

spark.range(1, 6).select($"id", ($"id" * $"id").as("square")).show()

Seq(("a", 1), ("b", 2), ("a", 3))
  .toDF("key", "value")
  .groupBy($"key")
  .agg(sum($"value").as("total"))
  .orderBy($"key")
  .show()

spark.stop()
```

Continue with the [Quickstart](quickstart.md).
