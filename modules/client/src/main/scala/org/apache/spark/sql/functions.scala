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

/**
 * Built-in functions for working with [[Column]]s, mirroring `org.apache.spark.sql.functions`.
 *
 * {{{
 *   import org.apache.spark.sql.functions._
 *   df.select(col("id"), upper(col("name")), (col("x") + 1).as("x1"))
 *   df.groupBy("dept").agg(avg("salary"), count(lit(1)))
 * }}}
 *
 * This object exposes a curated, growing subset of Spark's function library. Any Spark function
 * not listed here can still be invoked by name via [[callUDF]] / [[expr]].
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
  def count_distinct(expr: Column, exprs: Column*): Column = countDistinct(expr, exprs*)
  def sum(e: Column): Column = Column.fn("sum", e)
  def sum(columnName: String): Column = sum(col(columnName))
  def sumDistinct(e: Column): Column = Column.fnInternal("sum", isDistinct = true, e)
  def avg(e: Column): Column = Column.fn("avg", e)
  def avg(columnName: String): Column = avg(col(columnName))
  def mean(e: Column): Column = avg(e)
  def min(e: Column): Column = Column.fn("min", e)
  def min(columnName: String): Column = min(col(columnName))
  def max(e: Column): Column = Column.fn("max", e)
  def max(columnName: String): Column = max(col(columnName))
  def first(e: Column): Column = Column.fn("first", e)
  def last(e: Column): Column = Column.fn("last", e)
  def stddev(e: Column): Column = Column.fn("stddev", e)
  def variance(e: Column): Column = Column.fn("variance", e)
  def collect_list(e: Column): Column = Column.fn("collect_list", e)
  def collect_set(e: Column): Column = Column.fn("collect_set", e)
  def approx_count_distinct(e: Column): Column = Column.fn("approx_count_distinct", e)

  // -- Conditional ------------------------------------------------------------

  /** Evaluates a list of conditions and returns one of multiple possible result expressions. */
  def when(condition: Column, value: Any): Column =
    Column.fn("when", condition, Column.lit(value))

  def coalesce(e: Column*): Column = Column.fn("coalesce", e*)
  def isnull(e: Column): Column = Column.fn("isnull", e)
  def isnan(e: Column): Column = Column.fn("isnan", e)
  def nvl(col1: Column, col2: Column): Column = Column.fn("nvl", col1, col2)

  // -- Math --------------------------------------------------------------------

  def abs(e: Column): Column = Column.fn("abs", e)
  def sqrt(e: Column): Column = Column.fn("sqrt", e)
  def ceil(e: Column): Column = Column.fn("ceil", e)
  def floor(e: Column): Column = Column.fn("floor", e)
  def round(e: Column): Column = Column.fn("round", e)
  def round(e: Column, scale: Int): Column = Column.fn("round", e, lit(scale))
  def exp(e: Column): Column = Column.fn("exp", e)
  def log(e: Column): Column = Column.fn("ln", e)
  def pow(l: Column, r: Column): Column = Column.fn("power", l, r)
  def greatest(exprs: Column*): Column = Column.fn("greatest", exprs*)
  def least(exprs: Column*): Column = Column.fn("least", exprs*)
  def rand(): Column = Column.fn("rand")
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))
  def monotonically_increasing_id(): Column = Column.fn("monotonically_increasing_id")

  // -- String ------------------------------------------------------------------

  def upper(e: Column): Column = Column.fn("upper", e)
  def lower(e: Column): Column = Column.fn("lower", e)
  def length(e: Column): Column = Column.fn("length", e)
  def trim(e: Column): Column = Column.fn("trim", e)
  def ltrim(e: Column): Column = Column.fn("ltrim", e)
  def rtrim(e: Column): Column = Column.fn("rtrim", e)
  def concat(exprs: Column*): Column = Column.fn("concat", exprs*)
  def concat_ws(sep: String, exprs: Column*): Column =
    Column.fn("concat_ws", (lit(sep) +: exprs)*)
  def substring(str: Column, pos: Int, len: Int): Column =
    Column.fn("substring", str, lit(pos), lit(len))
  def split(str: Column, pattern: String): Column = Column.fn("split", str, lit(pattern))
  def regexp_replace(e: Column, pattern: String, replacement: String): Column =
    Column.fn("regexp_replace", e, lit(pattern), lit(replacement))
  def lpad(str: Column, len: Int, pad: String): Column =
    Column.fn("lpad", str, lit(len), lit(pad))
  def rpad(str: Column, len: Int, pad: String): Column =
    Column.fn("rpad", str, lit(len), lit(pad))
  def format_string(format: String, arguments: Column*): Column =
    Column.fn("format_string", (lit(format) +: arguments)*)

  // -- Date / time -------------------------------------------------------------

  def current_date(): Column = Column.fn("current_date")
  def current_timestamp(): Column = Column.fn("current_timestamp")
  def year(e: Column): Column = Column.fn("year", e)
  def month(e: Column): Column = Column.fn("month", e)
  def dayofmonth(e: Column): Column = Column.fn("dayofmonth", e)
  def hour(e: Column): Column = Column.fn("hour", e)
  def to_date(e: Column): Column = Column.fn("to_date", e)
  def to_timestamp(e: Column): Column = Column.fn("to_timestamp", e)
  def date_add(start: Column, days: Int): Column = Column.fn("date_add", start, lit(days))
  def datediff(end: Column, start: Column): Column = Column.fn("datediff", end, start)

  // -- Collections -------------------------------------------------------------

  def array(cols: Column*): Column = Column.fn("array", cols*)
  def array_contains(column: Column, value: Any): Column =
    Column.fn("array_contains", column, lit(value))
  def explode(e: Column): Column = Column.fn("explode", e)
  def size(e: Column): Column = Column.fn("size", e)
  def struct(cols: Column*): Column = Column.fn("struct", cols*)
  def map(cols: Column*): Column = Column.fn("map", cols*)

  // -- Window functions --------------------------------------------------------

  def row_number(): Column = Column.fn("row_number")
  def rank(): Column = Column.fn("rank")
  def dense_rank(): Column = Column.fn("dense_rank")
  def percent_rank(): Column = Column.fn("percent_rank")
  def cume_dist(): Column = Column.fn("cume_dist")
  def ntile(n: Int): Column = Column.fn("ntile", lit(n))
  def lag(e: Column, offset: Int): Column = Column.fn("lag", e, lit(offset))
  def lead(e: Column, offset: Int): Column = Column.fn("lead", e, lit(offset))
}
