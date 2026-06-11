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
 * Java/legacy-style factory mirroring `org.apache.spark.sql.types.DataTypes`. Provided so that code
 * written against Spark's `DataTypes.IntegerType` etc. compiles unchanged.
 */
object DataTypes:
  val NullType: DataType = org.apache.spark.sql.types.NullType
  val BooleanType: DataType = org.apache.spark.sql.types.BooleanType
  val ByteType: DataType = org.apache.spark.sql.types.ByteType
  val ShortType: DataType = org.apache.spark.sql.types.ShortType
  val IntegerType: DataType = org.apache.spark.sql.types.IntegerType
  val LongType: DataType = org.apache.spark.sql.types.LongType
  val FloatType: DataType = org.apache.spark.sql.types.FloatType
  val DoubleType: DataType = org.apache.spark.sql.types.DoubleType
  val StringType: DataType = org.apache.spark.sql.types.StringType
  val BinaryType: DataType = org.apache.spark.sql.types.BinaryType
  val DateType: DataType = org.apache.spark.sql.types.DateType
  val TimestampType: DataType = org.apache.spark.sql.types.TimestampType
  val TimestampNTZType: DataType = org.apache.spark.sql.types.TimestampNTZType
  val CalendarIntervalType: DataType = org.apache.spark.sql.types.CalendarIntervalType
  val VariantType: DataType = org.apache.spark.sql.types.VariantType

  def createDecimalType(precision: Int, scale: Int): DecimalType = DecimalType(precision, scale)
  def createDecimalType(): DecimalType = DecimalType.USER_DEFAULT
  def createArrayType(elementType: DataType): ArrayType = ArrayType(elementType, true)
  def createArrayType(elementType: DataType, containsNull: Boolean): ArrayType =
    ArrayType(elementType, containsNull)
  def createMapType(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType, valueType, true)
  def createMapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean): MapType =
    MapType(keyType, valueType, valueContainsNull)
  def createStructField(name: String, dataType: DataType, nullable: Boolean): StructField =
    StructField(name, dataType, nullable)
  def createStructType(fields: Array[StructField]): StructType = StructType(fields)
  def createStructType(fields: java.util.List[StructField]): StructType = StructType(fields)

/**
 * Minimal recursive-descent parser for Spark DDL type strings. Handles:
 *   - column lists: `a INT, b STRING, c ARRAY<INT>`
 *   - primitive types and aliases (int/integer, long/bigint, ...)
 *   - parameterised: `decimal(10,2)`, `char(10)`, `varchar(20)`
 *   - nested: `array<...>`, `map<...,...>`, `struct<a:int, b:string>`
 *
 * For anything it cannot parse it falls back to a single-field struct with the raw string so the
 * server can resolve it.
 */
private[types] object DDLParser:

  def parse(ddl: String): DataType =
    val trimmed = ddl.trim
    // A top-level comma not enclosed in <> or () means this is a column list -> StructType.
    if isColumnList(trimmed) then parseColumnList(trimmed)
    else parseType(trimmed)

  private def isColumnList(s: String): Boolean =
    var depth = 0
    var i = 0
    while i < s.length do
      s.charAt(i) match
        case '<' | '(' => depth += 1
        case '>' | ')' => depth -= 1
        case ',' if depth == 0 => return true
        case ':' if depth == 0 => return true // "name: type" style implies struct fields
        case _ =>
      i += 1
    false

  private def splitTopLevel(s: String, sep: Char): List[String] =
    val parts = scala.collection.mutable.ListBuffer.empty[String]
    val sb = new StringBuilder
    var depth = 0
    s.foreach {
      case c @ ('<' | '(') => depth += 1; sb.append(c)
      case c @ ('>' | ')') => depth -= 1; sb.append(c)
      case c if c == sep && depth == 0 =>
        parts += sb.toString; sb.clear()
      case c => sb.append(c)
    }
    if sb.nonEmpty then parts += sb.toString
    parts.toList.map(_.trim).filter(_.nonEmpty)

  private def parseColumnList(s: String): StructType =
    val fields = splitTopLevel(s, ',').map { col =>
      // "name: type" or "name type"
      val (name, typeStr) =
        val ci = topLevelIndexOf(col, ':')
        if ci >= 0 then (col.substring(0, ci).trim, col.substring(ci + 1).trim)
        else
          val sp = col.indexOf(' ')
          if sp >= 0 then (col.substring(0, sp).trim, col.substring(sp + 1).trim)
          else (col, "string")
      StructField(stripBackticks(name), parseType(typeStr))
    }
    StructType(fields.toArray)

  private def topLevelIndexOf(s: String, ch: Char): Int =
    var depth = 0
    var i = 0
    while i < s.length do
      s.charAt(i) match
        case '<' | '(' => depth += 1
        case '>' | ')' => depth -= 1
        case c if c == ch && depth == 0 => return i
        case _ =>
      i += 1
    -1

  private def stripBackticks(s: String): String =
    if s.startsWith("`") && s.endsWith("`") then s.substring(1, s.length - 1) else s

  private def parseType(raw: String): DataType =
    val s = raw.trim
    val lower = s.toLowerCase
    lower match
      case "void" | "null" => NullType
      case "boolean" | "bool" => BooleanType
      case "tinyint" | "byte" => ByteType
      case "smallint" | "short" => ShortType
      case "int" | "integer" => IntegerType
      case "bigint" | "long" => LongType
      case "float" | "real" => FloatType
      case "double" => DoubleType
      case "string" => StringType
      case "binary" => BinaryType
      case "date" => DateType
      case "timestamp" | "timestamp_ltz" => TimestampType
      case "timestamp_ntz" => TimestampNTZType
      case "interval" => CalendarIntervalType
      case "variant" => VariantType
      case _ if lower.startsWith("decimal") => parseParenType(s, lower, "decimal")
      case _ if lower.startsWith("char") => CharType(parseSingleInt(s))
      case _ if lower.startsWith("varchar") => VarcharType(parseSingleInt(s))
      case _ if lower.startsWith("array<") => ArrayType(parseType(inner(s)))
      case _ if lower.startsWith("map<") =>
        val parts = splitTopLevel(inner(s), ',')
        MapType(parseType(parts.head), parseType(parts(1)))
      case _ if lower.startsWith("struct<") => parseColumnList(inner(s))
      case _ =>
        // Unknown: keep the raw string in a single struct field so the server can parse it.
        StructType(Array(StructField("col", NullType)))

  private def inner(s: String): String =
    val a = s.indexOf('<')
    val b = s.lastIndexOf('>')
    s.substring(a + 1, b)

  private def parseSingleInt(s: String): Int =
    val a = s.indexOf('(')
    val b = s.indexOf(')')
    if a >= 0 && b > a then s.substring(a + 1, b).trim.toInt else 1

  private def parseParenType(s: String, lower: String, kind: String): DataType =
    val a = s.indexOf('(')
    if a < 0 then return DecimalType.USER_DEFAULT
    val b = s.indexOf(')')
    val params = s.substring(a + 1, b).split(',').map(_.trim)
    kind match
      case "decimal" =>
        if params.length == 2 then DecimalType(params(0).toInt, params(1).toInt)
        else if params.length == 1 then DecimalType(params(0).toInt, 0)
        else DecimalType.USER_DEFAULT
      case _ => DecimalType.USER_DEFAULT
