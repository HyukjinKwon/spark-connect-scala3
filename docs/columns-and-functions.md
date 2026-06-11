# Columns and Functions

A `Column` is a symbolic reference to a value computed from the input rows.
Columns are combined with operators and the functions in
`org.apache.spark.sql.functions` to express transformations.

```scala
import org.apache.spark.sql.functions.*
```

## Referencing columns

```scala
col("name")          // a column by name
column("name")       // alias for col
lit(42)              // a literal value as a column
expr("price * qty")  // a SQL expression as a column
```

## Operators

`Column` overloads the usual operators. Comparison operators return boolean
columns you can use in `filter`.

```scala
// Arithmetic
col("price") + col("tax")
col("price") - 1
col("price") * col("qty")
col("total") / col("count")
col("id") % 3

// Comparison
col("age") === 30      // equals
col("age") =!= 30      // not equals
col("age") > 21
col("age") <= 65
col("age") <=> 30      // null-safe equals

// Boolean logic
(col("age") > 21).and(col("country") === "KR")
(col("vip") === true).or(col("spend") > 1000)
!col("active")
```

## Predicates

```scala
col("middle_name").isNull
col("email").isNotNull
col("status").isin("new", "active", "pending")
col("score").between(0, 100)
col("name").like("A%")
col("name").rlike("^A.*n$")
col("name").contains("li")
col("name").startsWith("Al")
col("name").endsWith("ce")
```

## Conditionals

`when` starts a conditional chain; `otherwise` supplies the default:

```scala
when(col("score") >= 90, "A")
  .when(col("score") >= 80, "B")
  .otherwise("C")
  .as("grade")
```

## Casting and aliasing

```scala
col("id").cast("string")
col("amount").cast(org.apache.spark.sql.types.DoubleType)
col("id").as("identifier")
col("id").alias("identifier")
```

## Sorting expressions

```scala
col("salary").desc
col("name").asc
```

## Complex types

```scala
col("tags").getItem(0)        // array element
col("address").getField("city")  // struct field
```

## The functions library

`org.apache.spark.sql.functions` provides a large catalog. A representative
selection by category:

### Aggregate

`count`, `countDistinct`, `sum`, `sumDistinct`, `avg` / `mean`, `min`, `max`,
`first`, `last`, `collect_list`, `collect_set`, `stddev`, `variance`, `corr`,
`covar_samp`, `skewness`, `kurtosis`, `approx_count_distinct`,
`percentile_approx`.

```scala
df.agg(
  count("*").as("rows"),
  sum("amount").as("total"),
  round(avg("amount"), 2).as("avg"),
  max("amount").as("max"))
```

### String

`concat`, `concat_ws`, `length`, `lower`, `upper`, `initcap`, `trim`, `ltrim`,
`rtrim`, `lpad`, `rpad`, `substring`, `split`, `regexp_replace`,
`regexp_extract`, `translate`, `reverse`, `format_string`.

```scala
df.select(
  upper(col("name")).as("upper"),
  split(col("csv"), ",").as("parts"),
  regexp_replace(col("phone"), "[^0-9]", "").as("digits"))
```

### Math

`abs`, `sqrt`, `pow`, `exp`, `log`, `log10`, `round`, `bround`, `ceil`, `floor`,
`signum`, `sin`, `cos`, `tan`, `greatest`, `least`.

### Date and time

`current_date`, `current_timestamp`, `date_add`, `date_sub`, `datediff`,
`months_between`, `date_format`, `year`, `month`, `dayofmonth`, `hour`,
`to_date`, `to_timestamp`, `unix_timestamp`, `from_unixtime`, `window`.

```scala
df.select(
  year(col("ts")).as("year"),
  date_format(col("ts"), "yyyy-MM-dd").as("day"),
  datediff(current_date(), col("ts")).as("age_days"))
```

### Array and map

`array`, `array_contains`, `array_distinct`, `array_sort`, `size`, `explode`,
`posexplode`, `flatten`, `slice`, `map`, `map_keys`, `map_values`.

```scala
df.select(explode(split(col("line"), " ")).as("word"))
```

### Conditional and null handling

`when`, `coalesce`, `nvl`, `nullif`, `greatest`, `least`, `isnull`, `isnan`.

### Window

`row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`,
`lead`. These are used with a `WindowSpec`; see
[the window section of the DataFrames guide](dataframes.md) and the example
below.

```scala
import org.apache.spark.sql.expressions.Window

val byDept = Window.partitionBy("dept").orderBy(col("salary").desc)

df.select(
  col("name"),
  rank().over(byDept).as("rank"),
  sum("salary").over(byDept).as("running_total"))
```

## Window specifications

`org.apache.spark.sql.expressions.Window` builds a `WindowSpec`:

```scala
import org.apache.spark.sql.expressions.Window

// Partition and order.
Window.partitionBy("dept").orderBy(col("salary").desc)

// Row frame: from the start of the partition to the current row.
Window.partitionBy("dept").orderBy("salary")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

// Range frame.
Window.partitionBy("dept").orderBy("salary")
  .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
```

The frame boundary constants are `Window.unboundedPreceding`,
`Window.unboundedFollowing`, and `Window.currentRow`.
