# Standalone example

A minimal Scala 3 application that uses `spark-connect-scala3-client` as an ordinary
library dependency. It is a complete, separate sbt project (not part of the client's
own build), so it shows exactly what a downstream project looks like.

## Run

1. Start a Spark Connect server. Spark 4.0+ bundles it:

   ```bash
   $SPARK_HOME/sbin/start-connect-server.sh
   ```

   (On Spark 3.5.x add `--packages org.apache.spark:spark-connect_2.13:3.5.8` and use a
   Scala 2.13 distribution.)

2. From this directory, run the app, pointing it at the server:

   ```bash
   SPARK_REMOTE=sc://localhost:15002 sbt run
   ```

Expected output:

```
Connected to Apache Spark 4.1.2 at sc://localhost:15002
+------+---+------+
|bucket|  n| total|
+------+---+------+
|     0|333|166833|
|     1|333|166167|
|     2|333|166500|
+------+---+------+
grand total = 499500
```

## Notes

- Requires JDK 17 or 21. The Apache Arrow `--add-opens` flags are already set in
  `build.sbt`, so you do not need to pass any JVM options yourself.
- The dependency line is the only thing your own project needs:

  ```scala
  libraryDependencies += "io.github.hyukjinkwon" %% "spark-connect-scala3-client" % "0.3.0"
  ```

- Trying it before `0.3.0` is on Maven Central? Publish the client locally first - from
  the repository root run `sbt publishLocal`, then set the version here to the locally
  published one.
