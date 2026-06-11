# Contributing

Contributions are welcome - bug reports, documentation, examples, and code.

## Development setup

Prerequisites:

- JDK 17 or newer (Temurin recommended)
- [sbt](https://www.scala-sbt.org/) 1.10+
- Java 17+ to run a local Spark Connect server for integration tests

```bash
git clone https://github.com/HyukjinKwon/spark-connect-scala3
cd spark-connect-scala3
sbt compile
```

The build has three modules:

- `proto` - the ScalaPB-generated gRPC and message classes, compiled from the
  vendored Spark Connect protobuf definitions.
- `client` - the public client API under `org.apache.spark.sql.*`.
- `examples` - runnable example programs (not published).

## Building and testing

```bash
sbt clean compile      # compile every module
sbt test               # run the unit tests (no server required)
sbt scalafmtCheckAll   # verify formatting
sbt scalafmtAll        # apply formatting
```

### Integration tests

Integration tests run against a live Spark Connect server and are selected by a
`*Integration*` test-name glob. Start a server, then point the tests at it:

```bash
curl -L https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz -o spark.tgz
tar xzf spark.tgz
./spark-4.1.2-bin-hadoop3/sbin/start-connect-server.sh

SPARK_CONNECT_TEST_REMOTE=sc://localhost:15002 sbt "client/testOnly *Integration*"
```

## Code style

Formatting is enforced with [scalafmt](https://scalameta.org/scalafmt/); the
`.scalafmt.conf` at the repository root is the source of truth. Run
`sbt scalafmtAll` before pushing and keep `sbt scalafmtCheckAll` green.

## Pull requests

1. Fork and create a feature branch.
2. Add or update tests for your change; keep the suite green.
3. Run `sbt scalafmtCheckAll test` before pushing.
4. Open a PR with a clear description of the motivation and behavior.

By contributing, you agree that your contributions are licensed under the
project's Apache 2.0 license. See the full
[CONTRIBUTING.md](https://github.com/HyukjinKwon/spark-connect-scala3/blob/main/CONTRIBUTING.md)
for more detail.
