# Data Types & Schemas

Types live in `org.apache.spark.sql.types` and mirror Apache Spark. They are used for
schemas, casts, and typed literals, and they round-trip through the Spark Connect
protobuf representation.

## The type hierarchy

| Scala type (`types.*`) | SQL | JVM value in a `Row` |
|------------------------|-----|----------------------|
| `BooleanType` | `BOOLEAN` | `Boolean` |
| `ByteType` | `TINYINT` | `Byte` |
| `ShortType` | `SMALLINT` | `Short` |
| `IntegerType` | `INT` | `Int` |
| `LongType` | `BIGINT` | `Long` |
| `FloatType` | `FLOAT` | `Float` |
| `DoubleType` | `DOUBLE` | `Double` |
| `DecimalType(p, s)` | `DECIMAL(p,s)` | `java.math.BigDecimal` |
| `StringType` | `STRING` | `String` |
| `BinaryType` | `BINARY` | `Array[Byte]` |
| `DateType` | `DATE` | `java.sql.Date` |
| `TimestampType` | `TIMESTAMP` | `java.sql.Timestamp` |
| `TimestampNTZType` | `TIMESTAMP_NTZ` | `java.time.LocalDateTime` |
| `ArrayType(elem, …)` | `ARRAY<…>` | `scala.Seq` |
| `MapType(k, v, …)` | `MAP<…>` | `scala.Map` |
| `StructType(fields)` | `STRUCT<…>` | `Row` |

Also available: `NullType`, `CharType(n)`, `VarcharType(n)`,
`YearMonthIntervalType`, `DayTimeIntervalType`, `VariantType`.

## Building a schema

```scala
import org.apache.spark.sql.types.*

val schema = StructType(Seq(
  StructField("id", LongType, nullable = false),
  StructField("name", StringType),
  StructField("tags", ArrayType(StringType)),
  StructField("score", DecimalType(10, 2))
))

// fluent style
val s2 = new StructType()
  .add("id", LongType, nullable = false)
  .add("name", StringType)
```

From a DDL string:

```scala
val s3 = StructType.fromDDL("id BIGINT, name STRING, tags ARRAY<STRING>")
```

## Inspecting a type

```scala
IntegerType.typeName        // "integer"
IntegerType.simpleString    // "int"
IntegerType.sql             // "INT"
ArrayType(IntegerType).sql  // "ARRAY<INT>"
schema.fieldNames           // Array("id", "name", "tags", "score")
schema("name").dataType     // StringType
```

## Java-style factories

For Java interop / familiarity, `DataTypes` exposes factory methods and singletons:

```scala
import org.apache.spark.sql.types.DataTypes

DataTypes.IntegerType
DataTypes.createDecimalType(10, 2)
DataTypes.createArrayType(DataTypes.StringType)
DataTypes.createStructField("id", DataTypes.LongType, false)
```

## Casting

```scala
$"value".cast(DoubleType)   // by DataType
$"value".cast("double")     // by type name
```
