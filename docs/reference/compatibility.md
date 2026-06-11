# Compatibility

This page tracks what `spark-connect-scala3` supports today. The public API mirrors
Apache Spark's Scala API, so familiar code generally ports unchanged.

## Platform

| | Supported |
|---|---|
| Scala | 3.3.x (LTS) |
| JDK | 17, 21 |
| Spark Connect protocol | Apache Spark **4.0.0** (compatible with 3.5.x servers for the supported surface) |
| Transport | gRPC over HTTP/2, optional TLS + bearer token |

## API surface

Legend: ✅ implemented · 🚧 partial · ⬜ planned

### SparkSession

| Feature | Status |
|---|---|
| `builder().remote(...)`, `config`, `getOrCreate` | ✅ |
| `sql(...)`, parameterised `sql(...)` | ✅ |
| `range(...)` | ✅ |
| `createDataFrame(rows, schema)`, `toDF` | ✅ |
| `conf.get/set`, `version`, `stop` | ✅ |
| `table(...)`, `catalog` | ✅ |
| `udf` registration | ⬜ |

### DataFrame / Dataset

| Feature | Status |
|---|---|
| `select`, `selectExpr`, `filter`/`where`, `withColumn`, `withColumnRenamed`, `drop` | ✅ |
| `groupBy`/`agg`, `RelationalGroupedDataset`, `pivot` | ✅ |
| `orderBy`/`sort`, `limit`, `distinct`, `dropDuplicates` | ✅ |
| `join` (inner/left/right/outer/semi/anti/cross) | ✅ |
| `union`/`unionByName`, `intersect`, `except` | ✅ |
| `repartition`, `coalesce`, `sample`, `na` | ✅ |
| Actions: `collect`, `collectAsList`, `count`, `take`, `head`, `first`, `show`, `isEmpty`, `toLocalIterator` | ✅ |
| `printSchema`, `schema`, `columns`, `explain` | ✅ |
| Typed `Dataset[T]` (encoders for case classes) | 🚧 |

### Column & functions

| Feature | Status |
|---|---|
| Expression DSL (arithmetic, comparison, boolean, null, `isin`, `like`, `between`, `cast`, alias, sort order) | ✅ |
| `when`/`otherwise`, `lit`, `expr`, `col` | ✅ |
| Aggregate / math / string / date-time / collection functions | ✅ |
| Window functions (`Window`, `over`) | ✅ |
| `getItem`, `getField`, struct/array/map access | ✅ |

### Data sources

| Feature | Status |
|---|---|
| `read`/`write` parquet, json, csv, orc, text | ✅ |
| `option(s)`, `schema`, `mode`, `partitionBy` | ✅ |
| `saveAsTable`, `insertInto`, `table` | ✅ |
| Streaming (`readStream`/`writeStream`) | ⬜ |

### Types

| Feature | Status |
|---|---|
| Full `org.apache.spark.sql.types` hierarchy | ✅ |
| Protobuf ↔ `DataType` round-trip | ✅ |
| `StructType.fromDDL`, `DataTypes` factories | ✅ |

## Known limitations

- Typed `Dataset[T]` (arbitrary case-class encoders) is in progress; use `DataFrame`
  + `Row` for now.
- UDFs and ML (`spark.ml`) are not yet exposed.
- Structured Streaming is planned but not yet implemented.

!!! note
    This matrix evolves with each release. The
    [Scaladoc](https://hyukjinkwon.github.io/spark-connect-scala3/api/) is the
    authoritative list of available methods for a given version.
