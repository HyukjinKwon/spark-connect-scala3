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

package org.apache.spark.sql

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.LiteralProtoUtils
import org.apache.spark.sql.types.DataType

// scalastyle:off
/**
 * Built-in functions for working with [[Column]]s, mirroring `org.apache.spark.sql.functions` in
 * Apache Spark. Import them with:
 *
 * {{{
 *   import org.apache.spark.sql.functions._
 * }}}
 *
 * The full catalog of Spark SQL functions is also available through [[expr]] (a SQL string) and
 * [[call_function]] (by name).
 */
object functions {

  private def fn(name: String, cols: Column*): Column = Column.fn(name, cols: _*)

  // ==========================================================================
  // Core constructors
  // ==========================================================================

  /** Reference a column by name. */
  def col(colName: String): Column = Column(colName)

  /** Reference a column by name (alias of [[col]]). */
  def column(colName: String): Column = Column(colName)

  /**
   * Create a literal column from the given value. Supported types include the Scala/Java
   * primitives, `String`, `BigDecimal`, dates/timestamps (`java.time.*`, `java.sql.*`),
   * `Array[Byte]`, and arrays of those. A `Column` is returned unchanged.
   */
  def lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => Column(s.name)
    case _ => Column(LiteralProtoUtils.toLiteralExpression(literal))
  }

  /** Create a strongly-typed literal column (alias of [[lit]]). */
  def typedLit[T](literal: T): Column = lit(literal)

  /** Parse a SQL expression string into a column. */
  def expr(expr: String): Column = Column(
    proto
      .Expression()
      .withExpressionString(proto.Expression.ExpressionString().withExpression(expr))
  )

  /** Call a SQL function by name with the given arguments. */
  def call_function(funcName: String, cols: Column*): Column = Column(
    proto
      .Expression()
      .withCallFunction(
        proto
          .CallFunction()
          .withFunctionName(funcName)
          .withArguments(cols.map(_.expr))
      )
  )

  // ==========================================================================
  // Sorting
  // ==========================================================================

  def asc(columnName: String): Column = Column(columnName).asc
  def asc_nulls_first(columnName: String): Column = Column(columnName).asc_nulls_first
  def asc_nulls_last(columnName: String): Column = Column(columnName).asc_nulls_last
  def desc(columnName: String): Column = Column(columnName).desc
  def desc_nulls_first(columnName: String): Column = Column(columnName).desc_nulls_first
  def desc_nulls_last(columnName: String): Column = Column(columnName).desc_nulls_last

  // ==========================================================================
  // Aggregate functions
  // ==========================================================================

  def approx_count_distinct(e: Column): Column = fn("approx_count_distinct", e)
  def approx_count_distinct(columnName: String): Column = approx_count_distinct(col(columnName))
  def approx_count_distinct(e: Column, rsd: Double): Column =
    fn("approx_count_distinct", e, lit(rsd))
  def avg(e: Column): Column = fn("avg", e)
  def avg(columnName: String): Column = avg(col(columnName))
  def mean(e: Column): Column = avg(e)
  def mean(columnName: String): Column = avg(columnName)
  def collect_list(e: Column): Column = fn("collect_list", e)
  def collect_list(columnName: String): Column = collect_list(col(columnName))
  def collect_set(e: Column): Column = fn("collect_set", e)
  def collect_set(columnName: String): Column = collect_set(col(columnName))
  def corr(column1: Column, column2: Column): Column = fn("corr", column1, column2)
  def corr(columnName1: String, columnName2: String): Column =
    corr(col(columnName1), col(columnName2))
  def count(e: Column): Column = fn("count", e)
  def count(columnName: String): Column = count(col(columnName))
  def countDistinct(expr: Column, exprs: Column*): Column =
    Column.fn("count", isDistinct = true, (expr +: exprs): _*)
  def countDistinct(columnName: String, columnNames: String*): Column =
    countDistinct(col(columnName), columnNames.map(col): _*)
  def count_distinct(expr: Column, exprs: Column*): Column = countDistinct(expr, exprs: _*)
  def covar_pop(column1: Column, column2: Column): Column = fn("covar_pop", column1, column2)
  def covar_pop(columnName1: String, columnName2: String): Column =
    covar_pop(col(columnName1), col(columnName2))
  def covar_samp(column1: Column, column2: Column): Column = fn("covar_samp", column1, column2)
  def covar_samp(columnName1: String, columnName2: String): Column =
    covar_samp(col(columnName1), col(columnName2))
  def first(e: Column, ignoreNulls: Boolean): Column = fn("first", e, lit(ignoreNulls))
  def first(e: Column): Column = first(e, ignoreNulls = false)
  def first(columnName: String): Column = first(col(columnName))
  def first(columnName: String, ignoreNulls: Boolean): Column =
    first(col(columnName), ignoreNulls)
  def first_value(e: Column): Column = fn("first_value", e)
  def first_value(e: Column, ignoreNulls: Column): Column = fn("first_value", e, ignoreNulls)
  def grouping(e: Column): Column = fn("grouping", e)
  def grouping(columnName: String): Column = grouping(col(columnName))
  def grouping_id(cols: Column*): Column = fn("grouping_id", cols: _*)
  def grouping_id(colName: String, colNames: String*): Column =
    grouping_id((colName +: colNames).map(col): _*)
  def kurtosis(e: Column): Column = fn("kurtosis", e)
  def kurtosis(columnName: String): Column = kurtosis(col(columnName))
  def last(e: Column, ignoreNulls: Boolean): Column = fn("last", e, lit(ignoreNulls))
  def last(e: Column): Column = last(e, ignoreNulls = false)
  def last(columnName: String): Column = last(col(columnName))
  def last(columnName: String, ignoreNulls: Boolean): Column = last(col(columnName), ignoreNulls)
  def last_value(e: Column): Column = fn("last_value", e)
  def last_value(e: Column, ignoreNulls: Column): Column = fn("last_value", e, ignoreNulls)
  def max(e: Column): Column = fn("max", e)
  def max(columnName: String): Column = max(col(columnName))
  def max_by(e: Column, ord: Column): Column = fn("max_by", e, ord)
  def median(e: Column): Column = fn("median", e)
  def min(e: Column): Column = fn("min", e)
  def min(columnName: String): Column = min(col(columnName))
  def min_by(e: Column, ord: Column): Column = fn("min_by", e, ord)
  def mode(e: Column): Column = fn("mode", e)
  def percentile(e: Column, percentage: Column): Column = fn("percentile", e, percentage)
  def percentile_approx(e: Column, percentage: Column, accuracy: Column): Column =
    fn("percentile_approx", e, percentage, accuracy)
  def product(e: Column): Column = fn("product", e)
  def skewness(e: Column): Column = fn("skewness", e)
  def skewness(columnName: String): Column = skewness(col(columnName))
  def std(e: Column): Column = fn("std", e)
  def stddev(e: Column): Column = fn("stddev", e)
  def stddev(columnName: String): Column = stddev(col(columnName))
  def stddev_samp(e: Column): Column = fn("stddev_samp", e)
  def stddev_samp(columnName: String): Column = stddev_samp(col(columnName))
  def stddev_pop(e: Column): Column = fn("stddev_pop", e)
  def stddev_pop(columnName: String): Column = stddev_pop(col(columnName))
  def sum(e: Column): Column = fn("sum", e)
  def sum(columnName: String): Column = sum(col(columnName))
  def sumDistinct(e: Column): Column = sum_distinct(e)
  def sum_distinct(e: Column): Column = Column.fn("sum", isDistinct = true, e)
  def variance(e: Column): Column = fn("variance", e)
  def variance(columnName: String): Column = variance(col(columnName))
  def var_samp(e: Column): Column = fn("var_samp", e)
  def var_samp(columnName: String): Column = var_samp(col(columnName))
  def var_pop(e: Column): Column = fn("var_pop", e)
  def var_pop(columnName: String): Column = var_pop(col(columnName))
  def count_if(e: Column): Column = fn("count_if", e)
  def any_value(e: Column): Column = fn("any_value", e)
  def any_value(e: Column, ignoreNulls: Column): Column = fn("any_value", e, ignoreNulls)
  def bool_and(e: Column): Column = fn("bool_and", e)
  def bool_or(e: Column): Column = fn("bool_or", e)
  def bit_and(e: Column): Column = fn("bit_and", e)
  def bit_or(e: Column): Column = fn("bit_or", e)
  def bit_xor(e: Column): Column = fn("bit_xor", e)

  // ==========================================================================
  // Window functions
  // ==========================================================================

  def cume_dist(): Column = fn("cume_dist")
  def dense_rank(): Column = fn("dense_rank")
  def lag(e: Column, offset: Int): Column = lag(e, offset, null)
  def lag(columnName: String, offset: Int): Column = lag(col(columnName), offset)
  def lag(e: Column, offset: Int, defaultValue: Any): Column =
    fn("lag", e, lit(offset), lit(defaultValue))
  def lag(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    fn("lag", e, lit(offset), lit(defaultValue), lit(ignoreNulls))
  def lead(e: Column, offset: Int): Column = lead(e, offset, null)
  def lead(columnName: String, offset: Int): Column = lead(col(columnName), offset)
  def lead(e: Column, offset: Int, defaultValue: Any): Column =
    fn("lead", e, lit(offset), lit(defaultValue))
  def lead(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    fn("lead", e, lit(offset), lit(defaultValue), lit(ignoreNulls))
  def nth_value(e: Column, offset: Int, ignoreNulls: Boolean): Column =
    fn("nth_value", e, lit(offset), lit(ignoreNulls))
  def nth_value(e: Column, offset: Int): Column = fn("nth_value", e, lit(offset))
  def ntile(n: Int): Column = fn("ntile", lit(n))
  def percent_rank(): Column = fn("percent_rank")
  def rank(): Column = fn("rank")
  def row_number(): Column = fn("row_number")

  // ==========================================================================
  // Non-aggregate / misc
  // ==========================================================================

  def array(cols: Column*): Column = fn("array", cols: _*)
  def array(colName: String, colNames: String*): Column =
    array((colName +: colNames).map(col): _*)
  def array_contains(column: Column, value: Any): Column =
    fn("array_contains", column, lit(value))
  def arrays_overlap(a1: Column, a2: Column): Column = fn("arrays_overlap", a1, a2)
  def array_append(column: Column, element: Any): Column =
    fn("array_append", column, lit(element))
  def array_prepend(column: Column, element: Any): Column =
    fn("array_prepend", column, lit(element))
  def slice(x: Column, start: Int, length: Int): Column =
    slice(x, lit(start), lit(length))
  def slice(x: Column, start: Column, length: Column): Column =
    fn("slice", x, start, length)
  def array_join(column: Column, delimiter: String, nullReplacement: String): Column =
    fn("array_join", column, lit(delimiter), lit(nullReplacement))
  def array_join(column: Column, delimiter: String): Column =
    fn("array_join", column, lit(delimiter))
  def concat(exprs: Column*): Column = fn("concat", exprs: _*)
  def array_position(column: Column, value: Any): Column =
    fn("array_position", column, lit(value))
  def element_at(column: Column, value: Any): Column = fn("element_at", column, lit(value))
  def get(column: Column, index: Column): Column = fn("get", column, index)
  def array_sort(e: Column): Column = fn("array_sort", e)
  def array_sort(e: Column, comparator: (Column, Column) => Column): Column =
    fn("array_sort", e, createLambda(comparator))
  def array_remove(column: Column, element: Any): Column =
    fn("array_remove", column, lit(element))
  def array_compact(column: Column): Column = fn("array_compact", column)
  def array_distinct(e: Column): Column = fn("array_distinct", e)
  def array_intersect(col1: Column, col2: Column): Column = fn("array_intersect", col1, col2)
  def array_union(col1: Column, col2: Column): Column = fn("array_union", col1, col2)
  def array_except(col1: Column, col2: Column): Column = fn("array_except", col1, col2)
  def array_insert(arr: Column, pos: Column, value: Column): Column =
    fn("array_insert", arr, pos, value)
  def array_max(e: Column): Column = fn("array_max", e)
  def array_min(e: Column): Column = fn("array_min", e)
  def array_size(e: Column): Column = fn("array_size", e)
  def array_repeat(e: Column, count: Int): Column = fn("array_repeat", e, lit(count))
  def array_repeat(e: Column, count: Column): Column = fn("array_repeat", e, count)
  def arrays_zip(e: Column*): Column = fn("arrays_zip", e: _*)
  def cardinality(e: Column): Column = fn("cardinality", e)
  def coalesce(e: Column*): Column = fn("coalesce", e: _*)
  def explode(e: Column): Column = fn("explode", e)
  def explode_outer(e: Column): Column = fn("explode_outer", e)
  def flatten(e: Column): Column = fn("flatten", e)
  def greatest(exprs: Column*): Column = fn("greatest", exprs: _*)
  def input_file_name(): Column = fn("input_file_name")
  def isnan(e: Column): Column = fn("isnan", e)
  def isnull(e: Column): Column = fn("isnull", e)
  def least(exprs: Column*): Column = fn("least", exprs: _*)
  def map(cols: Column*): Column = fn("map", cols: _*)
  def create_map(cols: Column*): Column = fn("map", cols: _*)
  def map_concat(cols: Column*): Column = fn("map_concat", cols: _*)
  def map_contains_key(column: Column, key: Any): Column =
    fn("map_contains_key", column, lit(key))
  def map_entries(e: Column): Column = fn("map_entries", e)
  def map_from_arrays(keys: Column, values: Column): Column = fn("map_from_arrays", keys, values)
  def map_from_entries(e: Column): Column = fn("map_from_entries", e)
  def map_keys(e: Column): Column = fn("map_keys", e)
  def map_values(e: Column): Column = fn("map_values", e)
  def monotonically_increasing_id(): Column = fn("monotonically_increasing_id")
  def nanvl(col1: Column, col2: Column): Column = fn("nanvl", col1, col2)
  def negate(e: Column): Column = -e
  def not(e: Column): Column = fn("not", e)
  def posexplode(e: Column): Column = fn("posexplode", e)
  def posexplode_outer(e: Column): Column = fn("posexplode_outer", e)
  def rand(seed: Long): Column = fn("rand", lit(seed))
  def rand(): Column = fn("rand")
  def randn(seed: Long): Column = fn("randn", lit(seed))
  def randn(): Column = fn("randn")
  def sequence(start: Column, stop: Column, step: Column): Column =
    fn("sequence", start, stop, step)
  def sequence(start: Column, stop: Column): Column = fn("sequence", start, stop)
  def shuffle(e: Column): Column = fn("shuffle", e)
  def size(e: Column): Column = fn("size", e)
  def sort_array(e: Column): Column = sort_array(e, asc = true)
  def sort_array(e: Column, asc: Boolean): Column = fn("sort_array", e, lit(asc))
  def spark_partition_id(): Column = fn("spark_partition_id")
  def struct(cols: Column*): Column = fn("struct", cols: _*)
  def struct(colName: String, colNames: String*): Column =
    struct((colName +: colNames).map(col): _*)
  def named_struct(cols: Column*): Column = fn("named_struct", cols: _*)
  def reverse(e: Column): Column = fn("reverse", e)

  /** Conditional: evaluate `value` when `condition` is true. Chain via [[Column.when]]. */
  def when(condition: Column, value: Any): Column = Column.fn("when", condition, lit(value))

  // ==========================================================================
  // Math functions
  // ==========================================================================

  def abs(e: Column): Column = fn("abs", e)
  def acos(e: Column): Column = fn("acos", e)
  def acosh(e: Column): Column = fn("acosh", e)
  def asin(e: Column): Column = fn("asin", e)
  def asinh(e: Column): Column = fn("asinh", e)
  def atan(e: Column): Column = fn("atan", e)
  def atan2(y: Column, x: Column): Column = fn("atan2", y, x)
  def atanh(e: Column): Column = fn("atanh", e)
  def bin(e: Column): Column = fn("bin", e)
  def cbrt(e: Column): Column = fn("cbrt", e)
  def ceil(e: Column): Column = fn("ceil", e)
  def ceil(e: Column, scale: Column): Column = fn("ceil", e, scale)
  def ceiling(e: Column): Column = fn("ceiling", e)
  def conv(num: Column, fromBase: Int, toBase: Int): Column =
    fn("conv", num, lit(fromBase), lit(toBase))
  def cos(e: Column): Column = fn("cos", e)
  def cosh(e: Column): Column = fn("cosh", e)
  def cot(e: Column): Column = fn("cot", e)
  def csc(e: Column): Column = fn("csc", e)
  def e(): Column = fn("e")
  def exp(e: Column): Column = fn("exp", e)
  def expm1(e: Column): Column = fn("expm1", e)
  def factorial(e: Column): Column = fn("factorial", e)
  def floor(e: Column): Column = fn("floor", e)
  def floor(e: Column, scale: Column): Column = fn("floor", e, scale)
  def hex(column: Column): Column = fn("hex", column)
  def unhex(column: Column): Column = fn("unhex", column)
  def hypot(l: Column, r: Column): Column = fn("hypot", l, r)
  def log(e: Column): Column = fn("ln", e)
  def log(base: Double, a: Column): Column = fn("log", lit(base), a)
  def ln(e: Column): Column = fn("ln", e)
  def log10(e: Column): Column = fn("log10", e)
  def log1p(e: Column): Column = fn("log1p", e)
  def log2(expr: Column): Column = fn("log2", expr)
  def negative(e: Column): Column = fn("negative", e)
  def pi(): Column = fn("pi")
  def positive(e: Column): Column = fn("positive", e)
  def pow(l: Column, r: Column): Column = fn("power", l, r)
  def pow(l: Column, r: Double): Column = pow(l, lit(r))
  def pow(l: Double, r: Column): Column = pow(lit(l), r)
  def power(l: Column, r: Column): Column = fn("power", l, r)
  def pmod(dividend: Column, divisor: Column): Column = fn("pmod", dividend, divisor)
  def rint(e: Column): Column = fn("rint", e)
  def round(e: Column): Column = round(e, 0)
  def round(e: Column, scale: Int): Column = fn("round", e, lit(scale))
  def bround(e: Column): Column = bround(e, 0)
  def bround(e: Column, scale: Int): Column = fn("bround", e, lit(scale))
  def sec(e: Column): Column = fn("sec", e)
  def shiftleft(e: Column, numBits: Int): Column = fn("shiftleft", e, lit(numBits))
  def shiftright(e: Column, numBits: Int): Column = fn("shiftright", e, lit(numBits))
  def shiftrightunsigned(e: Column, numBits: Int): Column =
    fn("shiftrightunsigned", e, lit(numBits))
  def signum(e: Column): Column = fn("signum", e)
  def sin(e: Column): Column = fn("sin", e)
  def sinh(e: Column): Column = fn("sinh", e)
  def sqrt(e: Column): Column = fn("sqrt", e)
  def tan(e: Column): Column = fn("tan", e)
  def tanh(e: Column): Column = fn("tanh", e)
  def toDegrees(e: Column): Column = degrees(e)
  def degrees(e: Column): Column = fn("degrees", e)
  def toRadians(e: Column): Column = radians(e)
  def radians(e: Column): Column = fn("radians", e)
  def try_add(left: Column, right: Column): Column = fn("try_add", left, right)
  def try_divide(left: Column, right: Column): Column = fn("try_divide", left, right)
  def try_multiply(left: Column, right: Column): Column = fn("try_multiply", left, right)
  def try_subtract(left: Column, right: Column): Column = fn("try_subtract", left, right)
  def try_sum(e: Column): Column = fn("try_sum", e)
  def try_avg(e: Column): Column = fn("try_avg", e)
  def width_bucket(v: Column, min: Column, max: Column, numBucket: Column): Column =
    fn("width_bucket", v, min, max, numBucket)

  // ==========================================================================
  // Hashing / misc
  // ==========================================================================

  def md5(e: Column): Column = fn("md5", e)
  def sha1(e: Column): Column = fn("sha1", e)
  def sha2(e: Column, numBits: Int): Column = fn("sha2", e, lit(numBits))
  def crc32(e: Column): Column = fn("crc32", e)
  def hash(cols: Column*): Column = fn("hash", cols: _*)
  def xxhash64(cols: Column*): Column = fn("xxhash64", cols: _*)
  def raise_error(c: Column): Column = fn("raise_error", c)
  def assert_true(c: Column): Column = fn("assert_true", c)
  def assert_true(c: Column, e: Column): Column = fn("assert_true", c, e)
  def bitwise_not(e: Column): Column = fn("~", e)
  def bit_count(e: Column): Column = fn("bit_count", e)
  def typeof(col: Column): Column = fn("typeof", col)
  def stack(cols: Column*): Column = fn("stack", cols: _*)
  def uuid(): Column = fn("uuid")
  def current_catalog(): Column = fn("current_catalog")
  def current_database(): Column = fn("current_database")
  def current_schema(): Column = fn("current_schema")
  def current_user(): Column = fn("current_user")
  def user(): Column = fn("user")
  def session_user(): Column = fn("session_user")
  def version(): Column = fn("version")
  def input_file_block_length(): Column = fn("input_file_block_length")
  def input_file_block_start(): Column = fn("input_file_block_start")
  def equal_null(col1: Column, col2: Column): Column = fn("equal_null", col1, col2)

  // ==========================================================================
  // Conditional / null handling
  // ==========================================================================

  def ifnull(col1: Column, col2: Column): Column = fn("ifnull", col1, col2)
  def nullif(col1: Column, col2: Column): Column = fn("nullif", col1, col2)
  def nvl(col1: Column, col2: Column): Column = fn("nvl", col1, col2)
  def nvl2(col1: Column, col2: Column, col3: Column): Column = fn("nvl2", col1, col2, col3)

  // ==========================================================================
  // String functions
  // ==========================================================================

  def ascii(e: Column): Column = fn("ascii", e)
  def base64(e: Column): Column = fn("base64", e)
  def bit_length(e: Column): Column = fn("bit_length", e)
  def btrim(str: Column): Column = fn("btrim", str)
  def btrim(str: Column, trim: Column): Column = fn("btrim", str, trim)
  def char(n: Column): Column = fn("char", n)
  def character_length(str: Column): Column = fn("character_length", str)
  def char_length(str: Column): Column = fn("char_length", str)
  def concat_ws(sep: String, exprs: Column*): Column =
    Column.fn("concat_ws", (lit(sep) +: exprs): _*)
  def contains(left: Column, right: Column): Column = fn("contains", left, right)
  def decode(value: Column, charset: String): Column = fn("decode", value, lit(charset))
  def encode(value: Column, charset: String): Column = fn("encode", value, lit(charset))
  def endswith(str: Column, suffix: Column): Column = fn("endswith", str, suffix)
  def startswith(str: Column, prefix: Column): Column = fn("startswith", str, prefix)
  def find_in_set(str: Column, strArray: Column): Column = fn("find_in_set", str, strArray)
  def format_number(x: Column, d: Int): Column = fn("format_number", x, lit(d))
  def format_string(format: String, arguments: Column*): Column =
    Column.fn("format_string", (lit(format) +: arguments): _*)
  def initcap(e: Column): Column = fn("initcap", e)
  def instr(str: Column, substring: String): Column = fn("instr", str, lit(substring))
  def lcase(str: Column): Column = fn("lcase", str)
  def length(e: Column): Column = fn("length", e)
  def len(e: Column): Column = fn("len", e)
  def levenshtein(l: Column, r: Column): Column = fn("levenshtein", l, r)
  def levenshtein(l: Column, r: Column, threshold: Int): Column =
    fn("levenshtein", l, r, lit(threshold))
  def locate(substr: String, str: Column): Column = fn("locate", lit(substr), str)
  def locate(substr: String, str: Column, pos: Int): Column =
    fn("locate", lit(substr), str, lit(pos))
  def lower(e: Column): Column = fn("lower", e)
  def lpad(str: Column, len: Int, pad: String): Column = fn("lpad", str, lit(len), lit(pad))
  def lpad(str: Column, len: Column, pad: Column): Column = fn("lpad", str, len, pad)
  def ltrim(e: Column): Column = fn("ltrim", e)
  def ltrim(e: Column, trimString: String): Column = fn("ltrim", lit(trimString), e)
  def mask(input: Column): Column = fn("mask", input)
  def octet_length(e: Column): Column = fn("octet_length", e)
  def overlay(src: Column, replace: Column, pos: Column, len: Column): Column =
    fn("overlay", src, replace, pos, len)
  def overlay(src: Column, replace: Column, pos: Column): Column =
    fn("overlay", src, replace, pos)
  def position(substr: Column, str: Column): Column = fn("position", substr, str)
  def printf(format: Column, arguments: Column*): Column =
    Column.fn("printf", (format +: arguments): _*)
  def regexp(str: Column, regexp: Column): Column = fn("regexp", str, regexp)
  def regexp_count(str: Column, regexp: Column): Column = fn("regexp_count", str, regexp)
  def regexp_extract(e: Column, exp: String, groupIdx: Int): Column =
    fn("regexp_extract", e, lit(exp), lit(groupIdx))
  def regexp_extract_all(str: Column, regexp: Column): Column =
    fn("regexp_extract_all", str, regexp)
  def regexp_extract_all(str: Column, regexp: Column, idx: Column): Column =
    fn("regexp_extract_all", str, regexp, idx)
  def regexp_replace(e: Column, pattern: String, replacement: String): Column =
    fn("regexp_replace", e, lit(pattern), lit(replacement))
  def regexp_replace(e: Column, pattern: Column, replacement: Column): Column =
    fn("regexp_replace", e, pattern, replacement)
  def regexp_like(str: Column, regexp: Column): Column = fn("regexp_like", str, regexp)
  def regexp_instr(str: Column, regexp: Column): Column = fn("regexp_instr", str, regexp)
  def regexp_substr(str: Column, regexp: Column): Column = fn("regexp_substr", str, regexp)
  def repeat(str: Column, n: Int): Column = fn("repeat", str, lit(n))
  def replace(src: Column, search: Column): Column = fn("replace", src, search)
  def replace(src: Column, search: Column, replace: Column): Column =
    fn("replace", src, search, replace)
  def rpad(str: Column, len: Int, pad: String): Column = fn("rpad", str, lit(len), lit(pad))
  def rpad(str: Column, len: Column, pad: Column): Column = fn("rpad", str, len, pad)
  def rtrim(e: Column): Column = fn("rtrim", e)
  def rtrim(e: Column, trimString: String): Column = fn("rtrim", lit(trimString), e)
  def sentences(string: Column): Column = fn("sentences", string)
  def sentences(string: Column, language: Column, country: Column): Column =
    fn("sentences", string, language, country)
  def soundex(e: Column): Column = fn("soundex", e)
  def split(str: Column, pattern: String): Column = fn("split", str, lit(pattern))
  def split(str: Column, pattern: String, limit: Int): Column =
    fn("split", str, lit(pattern), lit(limit))
  def split_part(str: Column, delimiter: Column, partNum: Column): Column =
    fn("split_part", str, delimiter, partNum)
  def substring(str: Column, pos: Int, len: Int): Column =
    fn("substring", str, lit(pos), lit(len))
  def substring(str: Column, pos: Column, len: Column): Column = fn("substring", str, pos, len)
  def substring_index(str: Column, delim: String, count: Int): Column =
    fn("substring_index", str, lit(delim), lit(count))
  def translate(src: Column, matchingString: String, replaceString: String): Column =
    fn("translate", src, lit(matchingString), lit(replaceString))
  def trim(e: Column): Column = fn("trim", e)
  def trim(e: Column, trimString: String): Column = fn("trim", lit(trimString), e)
  def ucase(str: Column): Column = fn("ucase", str)
  def unbase64(e: Column): Column = fn("unbase64", e)
  def upper(e: Column): Column = fn("upper", e)
  def to_binary(e: Column): Column = fn("to_binary", e)
  def to_binary(e: Column, format: Column): Column = fn("to_binary", e, format)
  def to_char(e: Column, format: Column): Column = fn("to_char", e, format)
  def to_number(e: Column, format: Column): Column = fn("to_number", e, format)
  def url_decode(str: Column): Column = fn("url_decode", str)
  def url_encode(str: Column): Column = fn("url_encode", str)

  // ==========================================================================
  // Date / time functions
  // ==========================================================================

  def add_months(startDate: Column, numMonths: Int): Column =
    add_months(startDate, lit(numMonths))
  def add_months(startDate: Column, numMonths: Column): Column =
    fn("add_months", startDate, numMonths)
  def curdate(): Column = fn("curdate")
  def current_date(): Column = fn("current_date")
  def current_timestamp(): Column = fn("current_timestamp")
  def current_timezone(): Column = fn("current_timezone")
  def now(): Column = fn("now")
  def date_add(start: Column, days: Int): Column = date_add(start, lit(days))
  def date_add(start: Column, days: Column): Column = fn("date_add", start, days)
  def dateadd(start: Column, days: Column): Column = fn("dateadd", start, days)
  def date_sub(start: Column, days: Int): Column = date_sub(start, lit(days))
  def date_sub(start: Column, days: Column): Column = fn("date_sub", start, days)
  def date_diff(end: Column, start: Column): Column = fn("date_diff", end, start)
  def datediff(end: Column, start: Column): Column = fn("datediff", end, start)
  def date_format(dateExpr: Column, format: String): Column =
    fn("date_format", dateExpr, lit(format))
  def date_trunc(format: String, timestamp: Column): Column =
    fn("date_trunc", lit(format), timestamp)
  def dayofmonth(e: Column): Column = fn("dayofmonth", e)
  def day(e: Column): Column = fn("day", e)
  def dayofweek(e: Column): Column = fn("dayofweek", e)
  def dayofyear(e: Column): Column = fn("dayofyear", e)
  def extract(field: Column, source: Column): Column = fn("extract", field, source)
  def from_unixtime(ut: Column): Column = fn("from_unixtime", ut)
  def from_unixtime(ut: Column, f: String): Column = fn("from_unixtime", ut, lit(f))
  def from_utc_timestamp(ts: Column, tz: String): Column =
    fn("from_utc_timestamp", ts, lit(tz))
  def hour(e: Column): Column = fn("hour", e)
  def last_day(e: Column): Column = fn("last_day", e)
  def make_date(year: Column, month: Column, day: Column): Column =
    fn("make_date", year, month, day)
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    fn("make_timestamp", years, months, days, hours, mins, secs)
  def minute(e: Column): Column = fn("minute", e)
  def month(e: Column): Column = fn("month", e)
  def months_between(end: Column, start: Column): Column = fn("months_between", end, start)
  def months_between(end: Column, start: Column, roundOff: Boolean): Column =
    fn("months_between", end, start, lit(roundOff))
  def next_day(date: Column, dayOfWeek: String): Column = fn("next_day", date, lit(dayOfWeek))
  def quarter(e: Column): Column = fn("quarter", e)
  def second(e: Column): Column = fn("second", e)
  def to_date(e: Column): Column = fn("to_date", e)
  def to_date(e: Column, fmt: String): Column = fn("to_date", e, lit(fmt))
  def to_timestamp(s: Column): Column = fn("to_timestamp", s)
  def to_timestamp(s: Column, fmt: String): Column = fn("to_timestamp", s, lit(fmt))
  def to_unix_timestamp(e: Column): Column = fn("to_unix_timestamp", e)
  def to_utc_timestamp(ts: Column, tz: String): Column = fn("to_utc_timestamp", ts, lit(tz))
  def trunc(date: Column, format: String): Column = fn("trunc", date, lit(format))
  def unix_date(e: Column): Column = fn("unix_date", e)
  def unix_micros(e: Column): Column = fn("unix_micros", e)
  def unix_millis(e: Column): Column = fn("unix_millis", e)
  def unix_seconds(e: Column): Column = fn("unix_seconds", e)
  def unix_timestamp(): Column = fn("unix_timestamp")
  def unix_timestamp(s: Column): Column = fn("unix_timestamp", s)
  def unix_timestamp(s: Column, p: String): Column = fn("unix_timestamp", s, lit(p))
  def weekday(e: Column): Column = fn("weekday", e)
  def weekofyear(e: Column): Column = fn("weekofyear", e)
  def year(e: Column): Column = fn("year", e)
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column =
    fn("window", timeColumn, lit(windowDuration), lit(slideDuration))
  def window(timeColumn: Column, windowDuration: String): Column =
    fn("window", timeColumn, lit(windowDuration))
  def session_window(timeColumn: Column, gapDuration: String): Column =
    fn("session_window", timeColumn, lit(gapDuration))
  def timestamp_seconds(e: Column): Column = fn("timestamp_seconds", e)
  def timestamp_millis(e: Column): Column = fn("timestamp_millis", e)
  def timestamp_micros(e: Column): Column = fn("timestamp_micros", e)

  // ==========================================================================
  // JSON / CSV / Variant
  // ==========================================================================

  def get_json_object(e: Column, path: String): Column = fn("get_json_object", e, lit(path))
  def json_tuple(json: Column, fields: String*): Column =
    Column.fn("json_tuple", (json +: fields.map(lit)): _*)
  def from_json(e: Column, schema: Column): Column = fn("from_json", e, schema)
  def from_json(e: Column, schema: DataType): Column =
    fn("from_json", e, lit(schema.json))
  def to_json(e: Column): Column = fn("to_json", e)
  def schema_of_json(json: Column): Column = fn("schema_of_json", json)
  def schema_of_json(json: String): Column = fn("schema_of_json", lit(json))
  def json_array_length(e: Column): Column = fn("json_array_length", e)
  def json_object_keys(e: Column): Column = fn("json_object_keys", e)
  def from_csv(e: Column, schema: Column, options: Map[String, String]): Column =
    fn("from_csv", e, schema)
  def to_csv(e: Column): Column = fn("to_csv", e)
  def schema_of_csv(csv: Column): Column = fn("schema_of_csv", csv)
  def parse_json(e: Column): Column = fn("parse_json", e)

  // ==========================================================================
  // Higher-order (lambda) functions
  // ==========================================================================

  def transform(column: Column, f: Column => Column): Column =
    fn("transform", column, createLambda(f))
  def transform(column: Column, f: (Column, Column) => Column): Column =
    fn("transform", column, createLambda(f))
  def exists(column: Column, f: Column => Column): Column =
    fn("exists", column, createLambda(f))
  def forall(column: Column, f: Column => Column): Column =
    fn("forall", column, createLambda(f))
  def filter(column: Column, f: Column => Column): Column =
    fn("filter", column, createLambda(f))
  def filter(column: Column, f: (Column, Column) => Column): Column =
    fn("filter", column, createLambda(f))
  def aggregate(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    fn("aggregate", expr, initialValue, createLambda(merge), createLambda(finish))
  def aggregate(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    aggregate(expr, initialValue, merge, c => c)
  def reduce(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    fn("reduce", expr, initialValue, createLambda(merge), createLambda(finish))
  def reduce(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    reduce(expr, initialValue, merge, c => c)
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column =
    fn("zip_with", left, right, createLambda(f))
  def transform_keys(expr: Column, f: (Column, Column) => Column): Column =
    fn("transform_keys", expr, createLambda(f))
  def transform_values(expr: Column, f: (Column, Column) => Column): Column =
    fn("transform_values", expr, createLambda(f))
  def map_filter(expr: Column, f: (Column, Column) => Column): Column =
    fn("map_filter", expr, createLambda(f))
  def map_zip_with(left: Column, right: Column, f: (Column, Column, Column) => Column): Column =
    fn("map_zip_with", left, right, createLambda(f))

  // --- lambda helpers --------------------------------------------------------

  private def namedLambdaVar(name: String): proto.Expression.UnresolvedNamedLambdaVariable =
    proto.Expression.UnresolvedNamedLambdaVariable().addNameParts(name)

  private def varColumn(v: proto.Expression.UnresolvedNamedLambdaVariable): Column =
    Column(proto.Expression().withUnresolvedNamedLambdaVariable(v))

  private def lambda(
      args: Seq[proto.Expression.UnresolvedNamedLambdaVariable],
      body: Column
  ): Column =
    Column(
      proto
        .Expression()
        .withLambdaFunction(
          proto.Expression
            .LambdaFunction()
            .withFunction(body.expr)
            .withArguments(args)
        )
    )

  private def createLambda(f: Column => Column): Column = {
    val x = namedLambdaVar("x")
    lambda(Seq(x), f(varColumn(x)))
  }

  private def createLambda(f: (Column, Column) => Column): Column = {
    val x = namedLambdaVar("x")
    val y = namedLambdaVar("y")
    lambda(Seq(x, y), f(varColumn(x), varColumn(y)))
  }

  private def createLambda(f: (Column, Column, Column) => Column): Column = {
    val x = namedLambdaVar("x")
    val y = namedLambdaVar("y")
    val z = namedLambdaVar("z")
    lambda(Seq(x, y, z), f(varColumn(x), varColumn(y), varColumn(z)))
  }
}
// scalastyle:on
