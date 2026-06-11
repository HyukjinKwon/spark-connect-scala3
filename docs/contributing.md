# Contributing

Contributions are very welcome! This page summarises the workflow; the canonical
version lives in
[`CONTRIBUTING.md`](https://github.com/HyukjinKwon/spark-connect-scala3/blob/main/CONTRIBUTING.md)
at the repository root.

## Build & test

```bash
sbt compile            # generates proto stubs, then compiles
sbt test               # unit tests (no server required)
sbt scalafmtCheckAll   # formatting (enforced by CI)
sbt scalafmtAll        # auto-format
sbt doc                # Scaladoc
```

## Run a local server

```bash
curl -fsSL https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz | tar xz
cd spark-4.1.0-bin-hadoop3
./sbin/start-connect-server.sh        # sc://localhost:15002
```

## Integration tests

```bash
export SPARK_REMOTE="sc://localhost:15002"
sbt test
```

## Conventions

- Public API under `org.apache.spark.sql.*` should **mirror Apache Spark** (names,
  signatures, semantics).
- Internal/transport code under `org.apache.spark.sql.connect.client.*`.
- Format with `scalafmt` before committing; add tests and docs for new public API.
- Plan-building logic can be tested without a server by asserting on the generated
  protobuf.

## Pull requests

1. Fork, branch, implement with tests + docs.
2. `sbt scalafmtAll test`.
3. Open a PR describing the change and linking the relevant Spark API.

## Documentation

This site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/).
Preview locally:

```bash
pip install mkdocs-material mkdocs-minify-plugin
mkdocs serve   # http://localhost:8000
```

Docs are deployed to GitHub Pages automatically on every push to `main`.
