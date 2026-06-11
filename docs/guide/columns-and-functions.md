# Columns & Functions

A `Column` is an expression. You build columns from existing fields, literals, and
the functions in `org.apache.spark.sql.functions`, then use them in `select`,
`filter`, `withColumn`, aggregations, and joins.

## Referencing columns

```scala
import spark.implicits.*
import org.apache.spark.sql.functions.*

$"id"               // via the implicits interpolator
col("id")           // via functions.col
df("id")            // via the DataFrame (resolves against that frame)
expr("id + 1")      // a SQL expression string
```

## The expression DSL

```scala
$"price" * $"qty"                 // arithmetic: + - * / %
$"age" >= 18                      // comparison: === =!= < <= > >=
$"a" && $"b"                      // boolean: && || unary_!
$"name".isNull                    // null checks: isNull, isNotNull
$"status".isin("NEW", "OPEN")     // membership
$"name".like("A%")                // pattern match: like, rlike
$"score".between(0, 100)
$"value".cast("double")           // cast by type name
$"value".cast(DoubleType)         // cast by DataType
$"arr".getItem(0)                 // array / map element
$"struct".getField("x")           // struct field
$"id".as("identifier")            // alias
$"amount".desc                    // sort direction: asc, desc, asc_nulls_last, ...
```

## Conditionals

```scala
val label = when($"score" >= 90, "A")
  .when($"score" >= 80, "B")
  .otherwise("C")

df.withColumn("grade", label)
```

## Literals

```scala
lit(1)
lit("hello")
lit(3.14)
lit(Array(1, 2, 3))
```

## The `functions` object

`org.apache.spark.sql.functions` mirrors Apache Spark's static function set - over 400
aggregate, math, string, date/time, collection, conditional, and window functions. A
representative sample:

=== "Aggregate"

    ```scala
    count("*"), countDistinct($"x"), sum($"x"), avg($"x"),
    min($"x"), max($"x"), stddev($"x"), variance($"x"),
    collect_list($"x"), collect_set($"x"), first($"x"), last($"x")
    ```

=== "Math"

    ```scala
    abs($"x"), round($"x", 2), ceil($"x"), floor($"x"),
    sqrt($"x"), pow($"x", 2), exp($"x"), log($"x"),
    greatest($"a", $"b"), least($"a", $"b")
    ```

=== "String"

    ```scala
    concat($"a", $"b"), concat_ws("-", $"a", $"b"),
    lower($"s"), upper($"s"), trim($"s"), length($"s"),
    substring($"s", 1, 3), split($"s", ","), regexp_replace($"s", "a", "b"),
    lpad($"s", 5, "0"), format_string("%05d", $"n")
    ```

=== "Date/Time"

    ```scala
    current_date(), current_timestamp(), to_date($"s"), to_timestamp($"s"),
    date_add($"d", 7), datediff($"a", $"b"), year($"d"), month($"d"),
    date_format($"ts", "yyyy-MM-dd"), unix_timestamp($"s")
    ```

=== "Collections"

    ```scala
    array($"a", $"b"), array_contains($"arr", 1), size($"arr"),
    explode($"arr"), map($"k", $"v"), struct($"a", $"b"),
    from_json($"j", schema), to_json($"struct")
    ```

## Window functions

```scala
import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy($"dept").orderBy($"salary".desc)

df.select(
  $"name",
  $"dept",
  rank().over(w).as("rank"),
  sum($"salary").over(w).as("dept_total")
)
```

`Window` supports `partitionBy`, `orderBy`, `rowsBetween`, and `rangeBetween`. Use
`.over(spec)` on any window-capable function (`rank`, `dense_rank`, `row_number`,
`lead`, `lag`, plus aggregates).

!!! note "Full list"
    The [Scaladoc](https://hyukjinkwon.github.io/spark-connect-scala3/api/) documents
    every function and its signature.
