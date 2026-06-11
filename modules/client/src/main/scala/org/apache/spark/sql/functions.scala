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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.connect.proto

/**
 * Built-in functions for working with [[Column]]s, mirroring `org.apache.spark.sql.functions`.
 *
 * {{{
 *   import org.apache.spark.sql.functions._
 *   df.select(col("id"), upper(col("name")), (col("x") + 1).as("x1"))
 *   df.groupBy("dept").agg(avg("salary"), count(lit(1)))
 * }}}
 *
 * This object exposes a comprehensive subset of Spark's function library. Any Spark function not
 * listed here can still be invoked by name via [[callUDF]] / [[expr]].
 *
 * Following Spark's convention, a `String` argument denotes a *column name* for most functions
 * (e.g. `sum("salary")` aggregates the `salary` column), while functions whose parameters are
 * genuinely literal (regex patterns, date formats, JSON paths, ...) treat their `String` arguments
 * as literal values.
 */
object functions {

  // -- Core column constructors ----------------------------------------------

  /** Returns a [[Column]] based on the given column name. */
  def col(colName: String): Column = Column.fromName(colName)

  /** Returns a [[Column]] based on the given column name. Alias of [[col]]. */
  def column(colName: String): Column = Column.fromName(colName)

  /** Creates a [[Column]] of literal value. */
  def lit(literal: Any): Column = Column.lit(literal)

  /** Parses the expression string into the column it represents. */
  def expr(expr: String): Column = Dataset.exprColumn(expr)

  /** Calls a Spark function by name with the given columns as arguments. */
  def callUDF(funcName: String, cols: Column*): Column = Column.fn(funcName, cols*)
  def call_function(funcName: String, cols: Column*): Column = Column.fn(funcName, cols*)

  // -- Sorting ----------------------------------------------------------------

  def asc(columnName: String): Column = Column.fromName(columnName).asc
  def asc_nulls_first(columnName: String): Column = Column.fromName(columnName).asc_nulls_first
  def asc_nulls_last(columnName: String): Column = Column.fromName(columnName).asc_nulls_last
  def desc(columnName: String): Column = Column.fromName(columnName).desc
  def desc_nulls_first(columnName: String): Column = Column.fromName(columnName).desc_nulls_first
  def desc_nulls_last(columnName: String): Column = Column.fromName(columnName).desc_nulls_last

  // -- Aggregate functions ----------------------------------------------------

  def count(e: Column): Column = Column.fn("count", e)
  def count(columnName: String): Column = count(col(columnName))
  def countDistinct(expr: Column, exprs: Column*): Column =
    Column.fnInternal("count", isDistinct = true, (expr +: exprs)*)
  def countDistinct(columnName: String, columnNames: String*): Column =
    countDistinct(col(columnName), columnNames.map(col)*)
  def count_distinct(expr: Column, exprs: Column*): Column = countDistinct(expr, exprs*)
  def sum(e: Column): Column = Column.fn("sum", e)
  def sum(columnName: String): Column = sum(col(columnName))
  def sumDistinct(e: Column): Column = Column.fnInternal("sum", isDistinct = true, e)
  def sumDistinct(columnName: String): Column = sumDistinct(col(columnName))
  def sum_distinct(e: Column): Column = Column.fnInternal("sum", isDistinct = true, e)
  def avg(e: Column): Column = Column.fn("avg", e)
  def avg(columnName: String): Column = avg(col(columnName))
  def mean(e: Column): Column = avg(e)
  def mean(columnName: String): Column = avg(col(columnName))
  def min(e: Column): Column = Column.fn("min", e)
  def min(columnName: String): Column = min(col(columnName))
  def max(e: Column): Column = Column.fn("max", e)
  def max(columnName: String): Column = max(col(columnName))
  def min_by(e: Column, ord: Column): Column = Column.fn("min_by", e, ord)
  def max_by(e: Column, ord: Column): Column = Column.fn("max_by", e, ord)
  def first(e: Column): Column = Column.fn("first", e)
  def first(columnName: String): Column = first(col(columnName))
  def first(e: Column, ignoreNulls: Boolean): Column = Column.fn("first", e, lit(ignoreNulls))
  def first_value(e: Column): Column = Column.fn("first_value", e)
  def first_value(e: Column, ignoreNulls: Column): Column =
    Column.fn("first_value", e, ignoreNulls)
  def last(e: Column): Column = Column.fn("last", e)
  def last(columnName: String): Column = last(col(columnName))
  def last(e: Column, ignoreNulls: Boolean): Column = Column.fn("last", e, lit(ignoreNulls))
  def last_value(e: Column): Column = Column.fn("last_value", e)
  def last_value(e: Column, ignoreNulls: Column): Column = Column.fn("last_value", e, ignoreNulls)
  def stddev(e: Column): Column = Column.fn("stddev", e)
  def stddev(columnName: String): Column = stddev(col(columnName))
  def stddev_samp(e: Column): Column = Column.fn("stddev_samp", e)
  def stddev_samp(columnName: String): Column = stddev_samp(col(columnName))
  def stddev_pop(e: Column): Column = Column.fn("stddev_pop", e)
  def stddev_pop(columnName: String): Column = stddev_pop(col(columnName))
  def variance(e: Column): Column = Column.fn("variance", e)
  def variance(columnName: String): Column = variance(col(columnName))
  def var_samp(e: Column): Column = Column.fn("var_samp", e)
  def var_samp(columnName: String): Column = var_samp(col(columnName))
  def var_pop(e: Column): Column = Column.fn("var_pop", e)
  def var_pop(columnName: String): Column = var_pop(col(columnName))
  def skewness(e: Column): Column = Column.fn("skewness", e)
  def skewness(columnName: String): Column = skewness(col(columnName))
  def kurtosis(e: Column): Column = Column.fn("kurtosis", e)
  def kurtosis(columnName: String): Column = kurtosis(col(columnName))
  def corr(column1: Column, column2: Column): Column = Column.fn("corr", column1, column2)
  def corr(columnName1: String, columnName2: String): Column =
    corr(col(columnName1), col(columnName2))
  def covar_samp(column1: Column, column2: Column): Column =
    Column.fn("covar_samp", column1, column2)
  def covar_samp(columnName1: String, columnName2: String): Column =
    covar_samp(col(columnName1), col(columnName2))
  def covar_pop(column1: Column, column2: Column): Column =
    Column.fn("covar_pop", column1, column2)
  def covar_pop(columnName1: String, columnName2: String): Column =
    covar_pop(col(columnName1), col(columnName2))
  def collect_list(e: Column): Column = Column.fn("collect_list", e)
  def collect_list(columnName: String): Column = collect_list(col(columnName))
  def collect_set(e: Column): Column = Column.fn("collect_set", e)
  def collect_set(columnName: String): Column = collect_set(col(columnName))
  def approx_count_distinct(e: Column): Column = Column.fn("approx_count_distinct", e)
  def approx_count_distinct(columnName: String): Column =
    approx_count_distinct(col(columnName))
  def approx_count_distinct(e: Column, rsd: Double): Column =
    Column.fn("approx_count_distinct", e, lit(rsd))
  def approx_count_distinct(columnName: String, rsd: Double): Column =
    approx_count_distinct(col(columnName), rsd)
  def count_if(e: Column): Column = Column.fn("count_if", e)
  def grouping(e: Column): Column = Column.fn("grouping", e)
  def grouping(columnName: String): Column = grouping(col(columnName))
  def grouping_id(cols: Column*): Column = Column.fn("grouping_id", cols*)
  def grouping_id(colName: String, colNames: String*): Column =
    grouping_id((colName +: colNames).map(col)*)
  def product(e: Column): Column = Column.fn("product", e)
  def any(e: Column): Column = Column.fn("any", e)
  def some(e: Column): Column = Column.fn("some", e)
  def every(e: Column): Column = Column.fn("every", e)
  def bool_and(e: Column): Column = Column.fn("bool_and", e)
  def bool_or(e: Column): Column = Column.fn("bool_or", e)
  def bit_and(e: Column): Column = Column.fn("bit_and", e)
  def bit_or(e: Column): Column = Column.fn("bit_or", e)
  def bit_xor(e: Column): Column = Column.fn("bit_xor", e)
  def any_value(e: Column): Column = Column.fn("any_value", e)
  def any_value(e: Column, ignoreNulls: Column): Column = Column.fn("any_value", e, ignoreNulls)
  def median(e: Column): Column = Column.fn("median", e)
  def mode(e: Column): Column = Column.fn("mode", e)
  def percentile_approx(e: Column, percentage: Column, accuracy: Column): Column =
    Column.fn("percentile_approx", e, percentage, accuracy)
  def percentile(e: Column, percentage: Column): Column =
    Column.fn("percentile", e, percentage)
  def percentile(e: Column, percentage: Column, frequency: Column): Column =
    Column.fn("percentile", e, percentage, frequency)

  // -- Conditional / null handling --------------------------------------------

  /** Evaluates a list of conditions and returns one of multiple possible result expressions. */
  def when(condition: Column, value: Any): Column =
    Column.fn("when", condition, Column.lit(value))

  def coalesce(e: Column*): Column = Column.fn("coalesce", e*)
  def isnull(e: Column): Column = Column.fn("isnull", e)
  def isnan(e: Column): Column = Column.fn("isnan", e)
  def nanvl(col1: Column, col2: Column): Column = Column.fn("nanvl", col1, col2)
  def nvl(col1: Column, col2: Column): Column = Column.fn("nvl", col1, col2)
  def nvl2(col1: Column, col2: Column, col3: Column): Column =
    Column.fn("nvl2", col1, col2, col3)
  def ifnull(col1: Column, col2: Column): Column = Column.fn("ifnull", col1, col2)
  def nullif(col1: Column, col2: Column): Column = Column.fn("nullif", col1, col2)
  def greatest(exprs: Column*): Column = Column.fn("greatest", exprs*)
  def least(exprs: Column*): Column = Column.fn("least", exprs*)

  // -- Math --------------------------------------------------------------------

  def abs(e: Column): Column = Column.fn("abs", e)
  def sqrt(e: Column): Column = Column.fn("sqrt", e)
  def sqrt(colName: String): Column = sqrt(col(colName))
  def cbrt(e: Column): Column = Column.fn("cbrt", e)
  def cbrt(columnName: String): Column = cbrt(col(columnName))
  def ceil(e: Column): Column = Column.fn("ceil", e)
  def ceil(columnName: String): Column = ceil(col(columnName))
  def ceil(e: Column, scale: Column): Column = Column.fn("ceil", e, scale)
  def ceiling(e: Column): Column = Column.fn("ceiling", e)
  def floor(e: Column): Column = Column.fn("floor", e)
  def floor(columnName: String): Column = floor(col(columnName))
  def floor(e: Column, scale: Column): Column = Column.fn("floor", e, scale)
  def round(e: Column): Column = Column.fn("round", e)
  def round(e: Column, scale: Int): Column = Column.fn("round", e, lit(scale))
  def bround(e: Column): Column = Column.fn("bround", e)
  def bround(e: Column, scale: Int): Column = Column.fn("bround", e, lit(scale))
  def exp(e: Column): Column = Column.fn("exp", e)
  def exp(columnName: String): Column = exp(col(columnName))
  def expm1(e: Column): Column = Column.fn("expm1", e)
  def expm1(columnName: String): Column = expm1(col(columnName))
  def log(e: Column): Column = Column.fn("ln", e)
  def log(columnName: String): Column = log(col(columnName))
  def log(base: Double, e: Column): Column = Column.fn("log", lit(base), e)
  def log(base: Double, columnName: String): Column = log(base, col(columnName))
  def ln(e: Column): Column = Column.fn("ln", e)
  def log2(e: Column): Column = Column.fn("log2", e)
  def log2(columnName: String): Column = log2(col(columnName))
  def log10(e: Column): Column = Column.fn("log10", e)
  def log10(columnName: String): Column = log10(col(columnName))
  def log1p(e: Column): Column = Column.fn("log1p", e)
  def log1p(columnName: String): Column = log1p(col(columnName))
  def pow(l: Column, r: Column): Column = Column.fn("power", l, r)
  def pow(l: Column, r: Double): Column = Column.fn("power", l, lit(r))
  def pow(l: Double, r: Column): Column = Column.fn("power", lit(l), r)
  def pow(l: Column, rightName: String): Column = pow(l, col(rightName))
  def pow(leftName: String, r: Column): Column = pow(col(leftName), r)
  def pow(leftName: String, rightName: String): Column = pow(col(leftName), col(rightName))
  def power(l: Column, r: Column): Column = Column.fn("power", l, r)
  def sin(e: Column): Column = Column.fn("sin", e)
  def sin(columnName: String): Column = sin(col(columnName))
  def cos(e: Column): Column = Column.fn("cos", e)
  def cos(columnName: String): Column = cos(col(columnName))
  def tan(e: Column): Column = Column.fn("tan", e)
  def tan(columnName: String): Column = tan(col(columnName))
  def asin(e: Column): Column = Column.fn("asin", e)
  def asin(columnName: String): Column = asin(col(columnName))
  def acos(e: Column): Column = Column.fn("acos", e)
  def acos(columnName: String): Column = acos(col(columnName))
  def atan(e: Column): Column = Column.fn("atan", e)
  def atan(columnName: String): Column = atan(col(columnName))
  def atan2(y: Column, x: Column): Column = Column.fn("atan2", y, x)
  def atan2(y: Column, xName: String): Column = atan2(y, col(xName))
  def atan2(yName: String, x: Column): Column = atan2(col(yName), x)
  def atan2(yName: String, xName: String): Column = atan2(col(yName), col(xName))
  def atan2(y: Column, xValue: Double): Column = atan2(y, lit(xValue))
  def atan2(yValue: Double, x: Column): Column = atan2(lit(yValue), x)
  def sinh(e: Column): Column = Column.fn("sinh", e)
  def sinh(columnName: String): Column = sinh(col(columnName))
  def cosh(e: Column): Column = Column.fn("cosh", e)
  def cosh(columnName: String): Column = cosh(col(columnName))
  def tanh(e: Column): Column = Column.fn("tanh", e)
  def tanh(columnName: String): Column = tanh(col(columnName))
  def asinh(e: Column): Column = Column.fn("asinh", e)
  def acosh(e: Column): Column = Column.fn("acosh", e)
  def atanh(e: Column): Column = Column.fn("atanh", e)
  def cot(e: Column): Column = Column.fn("cot", e)
  def csc(e: Column): Column = Column.fn("csc", e)
  def sec(e: Column): Column = Column.fn("sec", e)
  def toDegrees(e: Column): Column = Column.fn("degrees", e)
  def degrees(e: Column): Column = Column.fn("degrees", e)
  def degrees(columnName: String): Column = degrees(col(columnName))
  def toRadians(e: Column): Column = Column.fn("radians", e)
  def radians(e: Column): Column = Column.fn("radians", e)
  def radians(columnName: String): Column = radians(col(columnName))
  def signum(e: Column): Column = Column.fn("signum", e)
  def signum(columnName: String): Column = signum(col(columnName))
  def factorial(e: Column): Column = Column.fn("factorial", e)
  def hypot(l: Column, r: Column): Column = Column.fn("hypot", l, r)
  def hypot(l: Column, rightName: String): Column = hypot(l, col(rightName))
  def hypot(leftName: String, r: Column): Column = hypot(col(leftName), r)
  def hypot(leftName: String, rightName: String): Column = hypot(col(leftName), col(rightName))
  def hypot(l: Column, r: Double): Column = hypot(l, lit(r))
  def hypot(l: Double, r: Column): Column = hypot(lit(l), r)
  def rint(e: Column): Column = Column.fn("rint", e)
  def rint(columnName: String): Column = rint(col(columnName))
  def pmod(dividend: Column, divisor: Column): Column = Column.fn("pmod", dividend, divisor)
  def hex(column: Column): Column = Column.fn("hex", column)
  def unhex(column: Column): Column = Column.fn("unhex", column)
  def conv(num: Column, fromBase: Int, toBase: Int): Column =
    Column.fn("conv", num, lit(fromBase), lit(toBase))
  def shiftleft(e: Column, numBits: Int): Column = Column.fn("shiftleft", e, lit(numBits))
  def shiftright(e: Column, numBits: Int): Column = Column.fn("shiftright", e, lit(numBits))
  def shiftrightunsigned(e: Column, numBits: Int): Column =
    Column.fn("shiftrightunsigned", e, lit(numBits))
  def bin(e: Column): Column = Column.fn("bin", e)
  def bin(columnName: String): Column = bin(col(columnName))
  def negate(e: Column): Column = Column.fn("negative", e)
  def negative(e: Column): Column = Column.fn("negative", e)
  def positive(e: Column): Column = Column.fn("positive", e)
  def bitwise_not(e: Column): Column = Column.fn("bitwise_not", e)
  def bitwiseNOT(e: Column): Column = Column.fn("bitwise_not", e)
  def bit_count(e: Column): Column = Column.fn("bit_count", e)

  // -- String ------------------------------------------------------------------

  def upper(e: Column): Column = Column.fn("upper", e)
  def lower(e: Column): Column = Column.fn("lower", e)
  def ucase(e: Column): Column = Column.fn("ucase", e)
  def lcase(e: Column): Column = Column.fn("lcase", e)
  def length(e: Column): Column = Column.fn("length", e)
  def char_length(e: Column): Column = Column.fn("char_length", e)
  def character_length(e: Column): Column = Column.fn("character_length", e)
  def octet_length(e: Column): Column = Column.fn("octet_length", e)
  def bit_length(e: Column): Column = Column.fn("bit_length", e)
  def trim(e: Column): Column = Column.fn("trim", e)
  def trim(e: Column, trimString: String): Column = Column.fn("trim", lit(trimString), e)
  def ltrim(e: Column): Column = Column.fn("ltrim", e)
  def ltrim(e: Column, trimString: String): Column = Column.fn("ltrim", lit(trimString), e)
  def rtrim(e: Column): Column = Column.fn("rtrim", e)
  def rtrim(e: Column, trimString: String): Column = Column.fn("rtrim", lit(trimString), e)
  def btrim(str: Column): Column = Column.fn("btrim", str)
  def btrim(str: Column, trim: Column): Column = Column.fn("btrim", str, trim)
  def lpad(str: Column, len: Int, pad: String): Column =
    Column.fn("lpad", str, lit(len), lit(pad))
  def rpad(str: Column, len: Int, pad: String): Column =
    Column.fn("rpad", str, lit(len), lit(pad))
  def concat(exprs: Column*): Column = Column.fn("concat", exprs*)
  def concat_ws(sep: String, exprs: Column*): Column =
    Column.fn("concat_ws", (lit(sep) +: exprs)*)
  def substring(str: Column, pos: Int, len: Int): Column =
    Column.fn("substring", str, lit(pos), lit(len))
  def substring(str: Column, pos: Column, len: Column): Column =
    Column.fn("substring", str, pos, len)
  def substring_index(str: Column, delim: String, count: Int): Column =
    Column.fn("substring_index", str, lit(delim), lit(count))
  def split(str: Column, pattern: String): Column = Column.fn("split", str, lit(pattern))
  def split(str: Column, pattern: String, limit: Int): Column =
    Column.fn("split", str, lit(pattern), lit(limit))
  def split_part(str: Column, delimiter: Column, partNum: Column): Column =
    Column.fn("split_part", str, delimiter, partNum)
  def regexp_replace(e: Column, pattern: String, replacement: String): Column =
    Column.fn("regexp_replace", e, lit(pattern), lit(replacement))
  def regexp_replace(e: Column, pattern: Column, replacement: Column): Column =
    Column.fn("regexp_replace", e, pattern, replacement)
  def regexp_extract(e: Column, exp: String, groupIdx: Int): Column =
    Column.fn("regexp_extract", e, lit(exp), lit(groupIdx))
  def regexp_extract_all(e: Column, exp: Column): Column =
    Column.fn("regexp_extract_all", e, exp)
  def regexp_extract_all(e: Column, exp: Column, groupIdx: Column): Column =
    Column.fn("regexp_extract_all", e, exp, groupIdx)
  def regexp_like(e: Column, pattern: Column): Column = Column.fn("regexp_like", e, pattern)
  def regexp_count(e: Column, pattern: Column): Column = Column.fn("regexp_count", e, pattern)
  def regexp_substr(e: Column, pattern: Column): Column = Column.fn("regexp_substr", e, pattern)
  def regexp_instr(e: Column, pattern: Column): Column = Column.fn("regexp_instr", e, pattern)
  def translate(src: Column, matchingString: String, replaceString: String): Column =
    Column.fn("translate", src, lit(matchingString), lit(replaceString))
  def initcap(e: Column): Column = Column.fn("initcap", e)
  def instr(str: Column, substring: String): Column = Column.fn("instr", str, lit(substring))
  def locate(substr: String, str: Column): Column = Column.fn("locate", lit(substr), str)
  def locate(substr: String, str: Column, pos: Int): Column =
    Column.fn("locate", lit(substr), str, lit(pos))
  def repeat(str: Column, n: Int): Column = Column.fn("repeat", str, lit(n))
  def reverse(e: Column): Column = Column.fn("reverse", e)
  def ascii(e: Column): Column = Column.fn("ascii", e)
  def base64(e: Column): Column = Column.fn("base64", e)
  def unbase64(e: Column): Column = Column.fn("unbase64", e)
  def encode(value: Column, charset: String): Column = Column.fn("encode", value, lit(charset))
  def decode(value: Column, charset: String): Column = Column.fn("decode", value, lit(charset))
  def format_number(x: Column, d: Int): Column = Column.fn("format_number", x, lit(d))
  def format_string(format: String, arguments: Column*): Column =
    Column.fn("format_string", (lit(format) +: arguments)*)
  def soundex(e: Column): Column = Column.fn("soundex", e)
  def levenshtein(l: Column, r: Column): Column = Column.fn("levenshtein", l, r)
  def levenshtein(l: Column, r: Column, threshold: Int): Column =
    Column.fn("levenshtein", l, r, lit(threshold))
  def overlay(src: Column, replace: Column, pos: Column): Column =
    Column.fn("overlay", src, replace, pos)
  def overlay(src: Column, replace: Column, pos: Column, len: Column): Column =
    Column.fn("overlay", src, replace, pos, len)
  def sentences(string: Column): Column = Column.fn("sentences", string)
  def sentences(string: Column, language: Column, country: Column): Column =
    Column.fn("sentences", string, language, country)
  def contains(left: Column, right: Column): Column = Column.fn("contains", left, right)
  def startswith(str: Column, prefix: Column): Column = Column.fn("startswith", str, prefix)
  def endswith(str: Column, suffix: Column): Column = Column.fn("endswith", str, suffix)
  def replace(src: Column, search: Column): Column = Column.fn("replace", src, search)
  def replace(src: Column, search: Column, replace: Column): Column =
    Column.fn("replace", src, search, replace)
  def to_varchar(e: Column, format: Column): Column = Column.fn("to_varchar", e, format)
  def to_char(e: Column, format: Column): Column = Column.fn("to_char", e, format)
  def to_number(e: Column, format: Column): Column = Column.fn("to_number", e, format)

  // -- Hashing -----------------------------------------------------------------

  def md5(e: Column): Column = Column.fn("md5", e)
  def sha1(e: Column): Column = Column.fn("sha1", e)
  def sha2(e: Column, numBits: Int): Column = Column.fn("sha2", e, lit(numBits))
  def crc32(e: Column): Column = Column.fn("crc32", e)
  def hash(cols: Column*): Column = Column.fn("hash", cols*)
  def xxhash64(cols: Column*): Column = Column.fn("xxhash64", cols*)

  // -- Date / time -------------------------------------------------------------

  def current_date(): Column = Column.fn("current_date")
  def current_timestamp(): Column = Column.fn("current_timestamp")
  def now(): Column = Column.fn("now")
  def current_timezone(): Column = Column.fn("current_timezone")
  def localtimestamp(): Column = Column.fn("localtimestamp")
  def year(e: Column): Column = Column.fn("year", e)
  def quarter(e: Column): Column = Column.fn("quarter", e)
  def month(e: Column): Column = Column.fn("month", e)
  def dayofmonth(e: Column): Column = Column.fn("dayofmonth", e)
  def day(e: Column): Column = Column.fn("day", e)
  def dayofweek(e: Column): Column = Column.fn("dayofweek", e)
  def dayofyear(e: Column): Column = Column.fn("dayofyear", e)
  def weekofyear(e: Column): Column = Column.fn("weekofyear", e)
  def weekday(e: Column): Column = Column.fn("weekday", e)
  def hour(e: Column): Column = Column.fn("hour", e)
  def minute(e: Column): Column = Column.fn("minute", e)
  def second(e: Column): Column = Column.fn("second", e)
  def date_add(start: Column, days: Int): Column = Column.fn("date_add", start, lit(days))
  def date_add(start: Column, days: Column): Column = Column.fn("date_add", start, days)
  def date_sub(start: Column, days: Int): Column = Column.fn("date_sub", start, lit(days))
  def date_sub(start: Column, days: Column): Column = Column.fn("date_sub", start, days)
  def datediff(end: Column, start: Column): Column = Column.fn("datediff", end, start)
  def date_diff(end: Column, start: Column): Column = Column.fn("date_diff", end, start)
  def months_between(end: Column, start: Column): Column =
    Column.fn("months_between", end, start)
  def months_between(end: Column, start: Column, roundOff: Boolean): Column =
    Column.fn("months_between", end, start, lit(roundOff))
  def add_months(start: Column, numMonths: Int): Column =
    Column.fn("add_months", start, lit(numMonths))
  def add_months(start: Column, numMonths: Column): Column =
    Column.fn("add_months", start, numMonths)
  def last_day(e: Column): Column = Column.fn("last_day", e)
  def next_day(date: Column, dayOfWeek: String): Column =
    Column.fn("next_day", date, lit(dayOfWeek))
  def next_day(date: Column, dayOfWeek: Column): Column = Column.fn("next_day", date, dayOfWeek)
  def trunc(date: Column, format: String): Column = Column.fn("trunc", date, lit(format))
  def date_trunc(format: String, timestamp: Column): Column =
    Column.fn("date_trunc", lit(format), timestamp)
  def from_unixtime(ut: Column): Column = Column.fn("from_unixtime", ut)
  def from_unixtime(ut: Column, f: String): Column = Column.fn("from_unixtime", ut, lit(f))
  def unix_timestamp(): Column = Column.fn("unix_timestamp")
  def unix_timestamp(s: Column): Column = Column.fn("unix_timestamp", s)
  def unix_timestamp(s: Column, p: String): Column = Column.fn("unix_timestamp", s, lit(p))
  def to_date(e: Column): Column = Column.fn("to_date", e)
  def to_date(e: Column, fmt: String): Column = Column.fn("to_date", e, lit(fmt))
  def to_timestamp(e: Column): Column = Column.fn("to_timestamp", e)
  def to_timestamp(e: Column, fmt: String): Column = Column.fn("to_timestamp", e, lit(fmt))
  def date_format(dateExpr: Column, format: String): Column =
    Column.fn("date_format", dateExpr, lit(format))
  def from_utc_timestamp(ts: Column, tz: String): Column =
    Column.fn("from_utc_timestamp", ts, lit(tz))
  def from_utc_timestamp(ts: Column, tz: Column): Column =
    Column.fn("from_utc_timestamp", ts, tz)
  def to_utc_timestamp(ts: Column, tz: String): Column =
    Column.fn("to_utc_timestamp", ts, lit(tz))
  def to_utc_timestamp(ts: Column, tz: Column): Column = Column.fn("to_utc_timestamp", ts, tz)
  def window(timeColumn: Column, windowDuration: String): Column =
    Column.fn("window", timeColumn, lit(windowDuration))
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column =
    Column.fn("window", timeColumn, lit(windowDuration), lit(slideDuration))
  def window(
      timeColumn: Column,
      windowDuration: String,
      slideDuration: String,
      startTime: String
  ): Column =
    Column.fn("window", timeColumn, lit(windowDuration), lit(slideDuration), lit(startTime))
  def session_window(timeColumn: Column, gapDuration: String): Column =
    Column.fn("session_window", timeColumn, lit(gapDuration))
  def make_date(year: Column, month: Column, day: Column): Column =
    Column.fn("make_date", year, month, day)
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("make_timestamp", years, months, days, hours, mins, secs)
  def extract(field: Column, source: Column): Column = Column.fn("extract", field, source)
  def date_part(field: Column, source: Column): Column = Column.fn("date_part", field, source)
  def datepart(field: Column, source: Column): Column = Column.fn("datepart", field, source)
  def timestamp_seconds(e: Column): Column = Column.fn("timestamp_seconds", e)
  def timestamp_millis(e: Column): Column = Column.fn("timestamp_millis", e)
  def timestamp_micros(e: Column): Column = Column.fn("timestamp_micros", e)
  def unix_date(e: Column): Column = Column.fn("unix_date", e)
  def unix_seconds(e: Column): Column = Column.fn("unix_seconds", e)
  def unix_millis(e: Column): Column = Column.fn("unix_millis", e)
  def unix_micros(e: Column): Column = Column.fn("unix_micros", e)
  def date_from_unix_date(e: Column): Column = Column.fn("date_from_unix_date", e)

  // -- Collections: arrays -----------------------------------------------------

  def array(cols: Column*): Column = Column.fn("array", cols*)
  def array(colName: String, colNames: String*): Column =
    array((colName +: colNames).map(col)*)
  def array_contains(column: Column, value: Any): Column =
    Column.fn("array_contains", column, lit(value))
  def arrays_overlap(a1: Column, a2: Column): Column = Column.fn("arrays_overlap", a1, a2)
  def array_join(column: Column, delimiter: String): Column =
    Column.fn("array_join", column, lit(delimiter))
  def array_join(column: Column, delimiter: String, nullReplacement: String): Column =
    Column.fn("array_join", column, lit(delimiter), lit(nullReplacement))
  def array_position(column: Column, value: Any): Column =
    Column.fn("array_position", column, lit(value))
  def array_remove(column: Column, element: Any): Column =
    Column.fn("array_remove", column, lit(element))
  def array_distinct(e: Column): Column = Column.fn("array_distinct", e)
  def array_intersect(col1: Column, col2: Column): Column =
    Column.fn("array_intersect", col1, col2)
  def array_union(col1: Column, col2: Column): Column = Column.fn("array_union", col1, col2)
  def array_except(col1: Column, col2: Column): Column = Column.fn("array_except", col1, col2)
  def array_max(e: Column): Column = Column.fn("array_max", e)
  def array_min(e: Column): Column = Column.fn("array_min", e)
  def array_repeat(e: Column, count: Int): Column = Column.fn("array_repeat", e, lit(count))
  def array_repeat(e: Column, count: Column): Column = Column.fn("array_repeat", e, count)
  def array_sort(e: Column): Column = Column.fn("array_sort", e)
  def array_append(column: Column, element: Any): Column =
    Column.fn("array_append", column, lit(element))
  def array_prepend(column: Column, element: Any): Column =
    Column.fn("array_prepend", column, lit(element))
  def array_insert(arr: Column, pos: Column, value: Column): Column =
    Column.fn("array_insert", arr, pos, value)
  def array_compact(column: Column): Column = Column.fn("array_compact", column)
  def slice(x: Column, start: Int, length: Int): Column =
    Column.fn("slice", x, lit(start), lit(length))
  def slice(x: Column, start: Column, length: Column): Column =
    Column.fn("slice", x, start, length)
  def size(e: Column): Column = Column.fn("size", e)
  def cardinality(e: Column): Column = Column.fn("cardinality", e)
  def sort_array(e: Column): Column = Column.fn("sort_array", e, lit(true))
  def sort_array(e: Column, asc: Boolean): Column = Column.fn("sort_array", e, lit(asc))
  def element_at(column: Column, value: Any): Column =
    Column.fn("element_at", column, lit(value))
  def get(column: Column, index: Column): Column = Column.fn("get", column, index)
  def explode(e: Column): Column = Column.fn("explode", e)
  def explode_outer(e: Column): Column = Column.fn("explode_outer", e)
  def posexplode(e: Column): Column = Column.fn("posexplode", e)
  def posexplode_outer(e: Column): Column = Column.fn("posexplode_outer", e)
  def inline(e: Column): Column = Column.fn("inline", e)
  def inline_outer(e: Column): Column = Column.fn("inline_outer", e)
  def flatten(e: Column): Column = Column.fn("flatten", e)
  def sequence(start: Column, stop: Column): Column = Column.fn("sequence", start, stop)
  def sequence(start: Column, stop: Column, step: Column): Column =
    Column.fn("sequence", start, stop, step)
  def shuffle(e: Column): Column = Column.fn("shuffle", e)
  def arrays_zip(e: Column*): Column = Column.fn("arrays_zip", e*)

  // -- Collections: structs ----------------------------------------------------

  def struct(cols: Column*): Column = Column.fn("struct", cols*)
  def struct(colName: String, colNames: String*): Column =
    struct((colName +: colNames).map(col)*)
  def named_struct(cols: Column*): Column = Column.fn("named_struct", cols*)

  // -- Collections: maps -------------------------------------------------------

  def map(cols: Column*): Column = Column.fn("map", cols*)
  def map_keys(e: Column): Column = Column.fn("map_keys", e)
  def map_values(e: Column): Column = Column.fn("map_values", e)
  def map_entries(e: Column): Column = Column.fn("map_entries", e)
  def map_from_entries(e: Column): Column = Column.fn("map_from_entries", e)
  def map_from_arrays(keys: Column, values: Column): Column =
    Column.fn("map_from_arrays", keys, values)
  def map_concat(cols: Column*): Column = Column.fn("map_concat", cols*)
  def map_contains_key(column: Column, key: Any): Column =
    Column.fn("map_contains_key", column, lit(key))

  // -- JSON / CSV --------------------------------------------------------------

  def get_json_object(e: Column, path: String): Column =
    Column.fn("get_json_object", e, lit(path))
  def json_tuple(json: Column, fields: String*): Column =
    Column.fn("json_tuple", (json +: fields.map(lit))*)
  def from_json(e: Column, schema: String): Column = Column.fn("from_json", e, lit(schema))
  def from_json(e: Column, schema: Column): Column = Column.fn("from_json", e, schema)
  def to_json(e: Column): Column = Column.fn("to_json", e)
  def schema_of_json(json: String): Column = Column.fn("schema_of_json", lit(json))
  def schema_of_json(json: Column): Column = Column.fn("schema_of_json", json)
  def from_csv(e: Column, schema: Column): Column = Column.fn("from_csv", e, schema)
  def to_csv(e: Column): Column = Column.fn("to_csv", e)
  def schema_of_csv(csv: String): Column = Column.fn("schema_of_csv", lit(csv))
  def schema_of_csv(csv: Column): Column = Column.fn("schema_of_csv", csv)

  // -- Misc / metadata ---------------------------------------------------------

  def monotonically_increasing_id(): Column = Column.fn("monotonically_increasing_id")
  def spark_partition_id(): Column = Column.fn("spark_partition_id")
  def input_file_name(): Column = Column.fn("input_file_name")
  def input_file_block_start(): Column = Column.fn("input_file_block_start")
  def input_file_block_length(): Column = Column.fn("input_file_block_length")
  def current_user(): Column = Column.fn("current_user")
  def current_catalog(): Column = Column.fn("current_catalog")
  def current_database(): Column = Column.fn("current_database")
  def current_schema(): Column = Column.fn("current_schema")
  def version(): Column = Column.fn("version")
  def uuid(): Column = Column.fn("uuid")
  def typeof(e: Column): Column = Column.fn("typeof", e)
  def rand(): Column = Column.fn("rand")
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))
  def randn(): Column = Column.fn("randn")
  def randn(seed: Long): Column = Column.fn("randn", lit(seed))

  // -- Window functions --------------------------------------------------------

  def row_number(): Column = Column.fn("row_number")
  def rank(): Column = Column.fn("rank")
  def dense_rank(): Column = Column.fn("dense_rank")
  def percent_rank(): Column = Column.fn("percent_rank")
  def cume_dist(): Column = Column.fn("cume_dist")
  def ntile(n: Int): Column = Column.fn("ntile", lit(n))
  def lag(e: Column, offset: Int): Column = Column.fn("lag", e, lit(offset))
  def lag(columnName: String, offset: Int): Column = lag(col(columnName), offset)
  def lag(e: Column, offset: Int, defaultValue: Any): Column =
    Column.fn("lag", e, lit(offset), lit(defaultValue))
  def lead(e: Column, offset: Int): Column = Column.fn("lead", e, lit(offset))
  def lead(columnName: String, offset: Int): Column = lead(col(columnName), offset)
  def lead(e: Column, offset: Int, defaultValue: Any): Column =
    Column.fn("lead", e, lit(offset), lit(defaultValue))
  def nth_value(e: Column, offset: Int): Column = Column.fn("nth_value", e, lit(offset))
  def nth_value(e: Column, offset: Int, ignoreNulls: Boolean): Column =
    Column.fn("nth_value", e, lit(offset), lit(ignoreNulls))

  // -- Higher-order (lambda) functions ----------------------------------------

  private val lambdaCounter = new AtomicLong(0L)

  /** Builds an `Expression.LambdaFunction` from a Scala function over fresh lambda variables. */
  private def createLambda(numArgs: Int, f: Seq[Column] => Column): Column = {
    val id = lambdaCounter.getAndIncrement()
    val vars = (0 until numArgs).map { i =>
      proto.Expression.UnresolvedNamedLambdaVariable(nameParts = Seq(s"x_${id}_$i"))
    }
    val args = vars.map(v =>
      new Column(
        proto.Expression(exprType = proto.Expression.ExprType.UnresolvedNamedLambdaVariable(v))
      )
    )
    val body = f(args)
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.LambdaFunction(
          proto.Expression.LambdaFunction(function = Some(body.expr), arguments = vars)
        )
      )
    )
  }

  /** Transforms elements of an array using the given function. */
  def transform(column: Column, f: Column => Column): Column =
    Column.fn("transform", column, createLambda(1, cs => f(cs(0))))

  /** Transforms elements of an array using the (element, index) function. */
  def transform(column: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform", column, createLambda(2, cs => f(cs(0), cs(1))))

  /** True if the predicate holds for any element of the array. */
  def exists(column: Column, f: Column => Column): Column =
    Column.fn("exists", column, createLambda(1, cs => f(cs(0))))

  /** True if the predicate holds for every element of the array. */
  def forall(column: Column, f: Column => Column): Column =
    Column.fn("forall", column, createLambda(1, cs => f(cs(0))))

  /** Filters an array keeping elements for which the predicate holds. */
  def filter(column: Column, f: Column => Column): Column =
    Column.fn("filter", column, createLambda(1, cs => f(cs(0))))

  /** Filters an array using the (element, index) predicate. */
  def filter(column: Column, f: (Column, Column) => Column): Column =
    Column.fn("filter", column, createLambda(2, cs => f(cs(0), cs(1))))

  /** Applies a binary operator to an initial state and all array elements, then a finish step. */
  def aggregate(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    Column.fn(
      "aggregate",
      expr,
      initialValue,
      createLambda(2, cs => merge(cs(0), cs(1))),
      createLambda(1, cs => finish(cs(0)))
    )

  def aggregate(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    aggregate(expr, initialValue, merge, identity)

  /** Merges two arrays element-wise using the given function. */
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column =
    Column.fn("zip_with", left, right, createLambda(2, cs => f(cs(0), cs(1))))

  /** Applies a function to every (key, value) entry of a map and returns transformed keys. */
  def transform_keys(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform_keys", expr, createLambda(2, cs => f(cs(0), cs(1))))

  /** Applies a function to every (key, value) entry of a map and returns transformed values. */
  def transform_values(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("transform_values", expr, createLambda(2, cs => f(cs(0), cs(1))))

  /** Filters a map keeping entries for which the (key, value) predicate holds. */
  def map_filter(expr: Column, f: (Column, Column) => Column): Column =
    Column.fn("map_filter", expr, createLambda(2, cs => f(cs(0), cs(1))))

  /** Merges two maps by key using the (key, value1, value2) function. */
  def map_zip_with(left: Column, right: Column, f: (Column, Column, Column) => Column): Column =
    Column.fn("map_zip_with", left, right, createLambda(3, cs => f(cs(0), cs(1), cs(2))))

  /** Marks a DataFrame as small enough for a broadcast join. */
  def broadcast(df: Dataset): Dataset = df.hint("broadcast")
}
