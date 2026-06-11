# Contributing to spark-connect-scala3

Thanks for your interest in improving the project! This guide covers how to build,
test, and submit changes.

## Prerequisites

- **JDK 17 or 21**
- **sbt** (`brew install sbt`, or see https://www.scala-sbt.org/)
- For integration tests: a running **Spark Connect server** (Apache Spark 3.5+/4.0).

## Project layout

```
build.sbt                 sbt build definition (multi-module)
project/                  sbt plugins & version
modules/proto/            vendored Spark Connect .proto files (ScalaPB-generated stubs)
modules/client/           the public client API (org.apache.spark.sql.*)
modules/examples/         runnable examples (not published)
docs/                     MkDocs (Material) documentation site
.github/workflows/        CI, release, and docs-deploy pipelines
```

The protobuf-generated Scala sources are **not** committed; sbt regenerates them from
the `.proto` files on every build via [ScalaPB](https://scalapb.github.io/).

## Building

```bash
sbt compile            # generates proto stubs, then compiles all modules
sbt test               # unit tests — these do NOT require a server
sbt scalafmtCheckAll   # verify formatting (CI enforces this)
sbt scalafmtAll        # auto-format
sbt doc                # generate Scaladoc
```

## Running a Spark Connect server locally

```bash
# Apache Spark 4.0.0
wget https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
tar xzf spark-4.0.0-bin-hadoop3.tgz && cd spark-4.0.0-bin-hadoop3
./sbin/start-connect-server.sh
# server listens on sc://localhost:15002
```

## Integration tests

Integration tests are tagged and skipped unless a server URL is provided:

```bash
export SPARK_REMOTE="sc://localhost:15002"
sbt "client/testOnly *IntegrationSuite"
```

CI starts an ephemeral Spark Connect server and runs the integration suite on every PR.

## Coding conventions

- Public API lives under `org.apache.spark.sql.*` and should **mirror Apache Spark's
  Scala API** (names, signatures, semantics) wherever practical, so that user code
  ports with minimal changes.
- Internal/transport code lives under `org.apache.spark.sql.connect.client.*`.
- Format with `scalafmt` (config in `.scalafmt.conf`) before committing.
- Add a unit test for new public methods. Arrow/plan-building logic can be tested
  without a server by asserting on the generated protobuf plan.

## Submitting a change

1. Fork and create a feature branch.
2. Make your change with tests and docs.
3. Run `sbt scalafmtAll test` and ensure CI-equivalent checks pass.
4. Open a pull request describing the change and linking any relevant Spark API.

## Releasing (maintainers)

Releases are automated by [`sbt-ci-release`](https://github.com/sbt/sbt-ci-release).
Pushing a tag `vX.Y.Z` publishes to Maven Central:

```bash
git tag -s v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

Snapshots are published on every push to `main`.

## Code of conduct

Be respectful and constructive. We follow the spirit of the
[Contributor Covenant](https://www.contributor-covenant.org/).
