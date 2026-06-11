# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial native Scala 3 client for the Apache Spark Connect protocol (Spark 4.0.0).
- `SparkSession` with `.builder().remote(...)`, runtime config, `sql`, `range`,
  `createDataFrame`.
- `Dataset`/`DataFrame` transformations: `select`, `filter`/`where`, `withColumn`,
  `withColumnRenamed`, `drop`, `join`, `groupBy`/`agg`, `orderBy`/`sort`, `union`,
  `distinct`, `limit`, `sample`, and more.
- `Column` expression DSL and the `functions` object (aggregate, math, string,
  date/time, collection, conditional, window).
- Complete `org.apache.spark.sql.types` hierarchy with protobuf round-tripping.
- Apache Arrow result decoding to `Row`; actions `collect`, `show`, `count`, `take`,
  `head`, `first`, `toLocalIterator`.
- `DataFrameReader`/`DataFrameWriter` and `Catalog`.
- MkDocs documentation site, Scaladoc, runnable examples.
- GitHub Actions: CI (build/test/format + integration against a live server),
  release (Maven Central via sbt-ci-release), and docs deploy (GitHub Pages).

[Unreleased]: https://github.com/HyukjinKwon/spark-connect-scala3/commits/main
