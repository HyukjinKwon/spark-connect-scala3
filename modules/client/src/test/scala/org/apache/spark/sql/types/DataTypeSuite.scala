/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

/**
 * Hermetic coverage for the [[DataType]] hierarchy: rendering (`typeName` / `simpleString` / `sql`
 * / `catalogString`), `defaultSize`, structural equality, the [[StructType]] container API, and
 * `treeString`. No server required.
 */
class DataTypeSuite extends munit.FunSuite {

  test("atomic rendering and default sizes") {
    assertEquals(BooleanType.simpleString, "boolean")
    assertEquals(BooleanType.defaultSize, 1)
    assertEquals(ByteType.simpleString, "tinyint")
    assertEquals(ShortType.simpleString, "smallint")
    assertEquals(IntegerType.simpleString, "int")
    assertEquals(LongType.simpleString, "bigint")
    assertEquals(FloatType.simpleString, "float")
    assertEquals(DoubleType.simpleString, "double")
    assertEquals(StringType.simpleString, "string")
    assertEquals(BinaryType.simpleString, "binary")
    assertEquals(DateType.simpleString, "date")
    assertEquals(TimestampType.simpleString, "timestamp")
    assertEquals(TimestampNTZType.simpleString, "timestamp_ntz")
    assertEquals(NullType.simpleString, "null")
    assertEquals(IntegerType.defaultSize, 4)
    assertEquals(LongType.defaultSize, 8)
    assertEquals(DoubleType.defaultSize, 8)
  }

  test("sql is the upper-cased simpleString; catalogString matches simpleString") {
    assertEquals(IntegerType.sql, "INT")
    assertEquals(IntegerType.catalogString, "int")
    assertEquals(StringType.sql, "STRING")
  }

  test("char and varchar carry their length") {
    assertEquals(CharType(10).simpleString, "char(10)")
    assertEquals(CharType(10).defaultSize, 10)
    assertEquals(VarcharType(255).simpleString, "varchar(255)")
    assertEquals(VarcharType(255).defaultSize, 255)
  }

  test("decimal renders precision and scale in typeName, simpleString and sql") {
    val d = DecimalType(10, 2)
    assertEquals(d.typeName, "decimal(10,2)")
    assertEquals(d.simpleString, "decimal(10,2)")
    assertEquals(d.sql, "DECIMAL(10,2)")
    assertEquals(DecimalType.DEFAULT, DecimalType(10, 0))
    assertEquals(DecimalType.SYSTEM_DEFAULT, DecimalType(38, 18))
    assertEquals(DecimalType().precision, 10)
    assertEquals(DecimalType(5, 0).defaultSize, 8)
    assertEquals(DecimalType(30, 0).defaultSize, 16)
  }

  test("year-month interval renders the actual field range") {
    import YearMonthIntervalType._
    assertEquals(YearMonthIntervalType(YEAR, MONTH).simpleString, "interval year to month")
    assertEquals(YearMonthIntervalType(YEAR, YEAR).simpleString, "interval year")
    assertEquals(YearMonthIntervalType(MONTH, MONTH).simpleString, "interval month")
    assertEquals(YearMonthIntervalType().simpleString, "interval year to month")
  }

  test("day-time interval renders the actual field range") {
    import DayTimeIntervalType._
    assertEquals(DayTimeIntervalType(DAY, SECOND).simpleString, "interval day to second")
    assertEquals(DayTimeIntervalType(DAY, DAY).simpleString, "interval day")
    assertEquals(DayTimeIntervalType(HOUR, MINUTE).simpleString, "interval hour to minute")
    assertEquals(DayTimeIntervalType(MINUTE, SECOND).simpleString, "interval minute to second")
    assertEquals(DayTimeIntervalType().simpleString, "interval day to second")
  }

  test("array and map rendering and default values") {
    assertEquals(ArrayType(IntegerType).containsNull, true)
    assertEquals(ArrayType(StringType, containsNull = false).simpleString, "array<string>")
    assertEquals(MapType(StringType, IntegerType).valueContainsNull, true)
    assertEquals(
      MapType(StringType, LongType, valueContainsNull = false).simpleString,
      "map<string,bigint>"
    )
    assertEquals(ArrayType(IntegerType).defaultSize, IntegerType.defaultSize)
  }

  test("structural equality ignores nullability of nested fields via sameType") {
    val a = StructType(Array(StructField("x", IntegerType, nullable = true)))
    val b = StructType(Array(StructField("x", IntegerType, nullable = false)))
    assert(a.sameType(b))
    assert(!a.sameType(StructType(Array(StructField("y", IntegerType)))))
    assert(ArrayType(IntegerType, containsNull = true).sameType(ArrayType(IntegerType, false)))
    assert(
      MapType(StringType, IntegerType, valueContainsNull = true)
        .sameType(MapType(StringType, IntegerType, valueContainsNull = false))
    )
    assert(!IntegerType.sameType(LongType))
  }

  test("StructType container API") {
    val s = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    assertEquals(s.length, 2)
    assertEquals(s.fieldNames.toSeq, Seq("a", "b"))
    assertEquals(s.fieldIndex("b"), 1)
    assertEquals(s("a").dataType, IntegerType: DataType)
    assertEquals(s(1).name, "b")
    assertEquals(s.iterator.size, 2)
    intercept[IllegalArgumentException](s("missing"))
    intercept[IllegalArgumentException](s.fieldIndex("missing"))
  }

  test("StructType.add overloads append fields") {
    val s = StructType()
      .add(StructField("a", IntegerType))
      .add("b", StringType)
      .add("c", LongType, nullable = false)
    assertEquals(s.fieldNames.toSeq, Seq("a", "b", "c"))
    assertEquals(s("c").nullable, false)
    assertEquals(s("b").nullable, true)
  }

  test("StructType equality is by content despite the backing Array") {
    val a = StructType(Array(StructField("x", IntegerType)))
    val b = StructType(Array(StructField("x", IntegerType)))
    assertEquals(a, b)
    assertEquals(a.hashCode(), b.hashCode())
    assertNotEquals(a, StructType(Array(StructField("y", IntegerType))))
  }

  test("StructField comment helpers round-trip through metadata") {
    val f = StructField("a", IntegerType).withComment("the id")
    assertEquals(f.getComment, Some("the id"))
    assertEquals(StructField("a", IntegerType).getComment, None)
    assertEquals(f.simpleString, "a:int")
  }

  test("treeString preserves decimal precision and nested structure") {
    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("amount", DecimalType(10, 2)),
        StructField(
          "owner",
          StructType(Array(StructField("name", StringType))),
          nullable = true
        ),
        StructField("items", ArrayType(StructType(Array(StructField("sku", StringType)))))
      )
    )
    val tree = schema.treeString
    assert(tree.startsWith("root\n"), tree)
    // treeString uses typeName (so LongType prints as `long`, matching Spark's printSchema), and
    // the decimal precision/scale must survive (the bug this regression-guards).
    assert(tree.contains("|-- id: long (nullable = false)"), tree)
    assert(tree.contains("|-- amount: decimal(10,2) (nullable = true)"), tree)
    assert(tree.contains("|-- name: string"), tree)
    assert(tree.contains("|-- sku: string"), tree)
  }
}
