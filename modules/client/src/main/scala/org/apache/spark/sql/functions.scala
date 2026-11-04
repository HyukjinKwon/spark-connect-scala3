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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.{DataType, StructType}

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
  @deprecated("Use call_udf", "3.2.0")
  def callUDF(funcName: String, cols: Column*): Column = call_function(funcName, cols*)

  /**
   * Calls a SQL function by name, which may be a built-in function, a temporary function, or a
   * persistent (possibly qualified, e.g. `my_db.my_fn`) function.
   */
  def call_function(funcName: String, cols: Column*): Column =
    Column.function(funcName, cols, isUserDefinedFunction = true)

  /**
   * Creates a [[Column]] of literal value whose Spark type comes from the value's [[Encoder]], so
   * that parameterized types (`Seq`, `Map`, `Option`, case classes and tuples) keep their full
   * type, including for empty collections and `None`.
   */
  def typedLit[T](literal: T)(using enc: Encoder[T]): Column = typedlit(literal)

  /**
   * Creates a [[Column]] of literal value whose Spark type comes from the value's [[Encoder]]. An
   * alias of [[typedLit]].
   */
  def typedlit[T](literal: T)(using enc: Encoder[T]): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ =>
      Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.Literal(
            Column.typedLiteralProto(enc.toValue(literal), enc.dataType)
          )
        )
      )
  }

  /** Call an user-defined function. */
  @scala.annotation.varargs
  def call_udf(udfName: String, cols: Column*): Column = call_function(udfName, cols*)

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
  def first(e: Column): Column = first(e, ignoreNulls = false)
  def first(columnName: String): Column = first(col(columnName))
  def first(e: Column, ignoreNulls: Boolean): Column = Column.fn("first", e, lit(ignoreNulls))
  def first_value(e: Column): Column = Column.fn("first_value", e)
  def first_value(e: Column, ignoreNulls: Column): Column =
    Column.fn("first_value", e, ignoreNulls)
  def last(e: Column): Column = last(e, ignoreNulls = false)
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
  def product(e: Column): Column = Column.internalFn("product", e)
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

  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column): Column = approx_count_distinct(e)

  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String): Column = approx_count_distinct(columnName)

  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(e: Column, rsd: Double): Column = approx_count_distinct(e, rsd)

  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(columnName: String, rsd: Double): Column =
    approx_count_distinct(Column(columnName), rsd)

  /**
   * Aggregate function: returns the distinct union of the elements of an array-typed column across
   * rows.
   */
  def collect_union(e: Column): Column = Column.fn("collect_union", e)

  /**
   * Aggregate function: returns the distinct union of the elements of an array-typed column across
   * rows.
   */
  def collect_union(columnName: String): Column = collect_union(Column(columnName))

  /** Returns a count-min sketch of a column with the given esp, confidence and seed. */
  def count_min_sketch(e: Column, eps: Column, confidence: Column, seed: Column): Column =
    Column.fn("count_min_sketch", e, eps, confidence, seed)

  /** Returns a count-min sketch of a column with the given esp, confidence and seed. */
  def count_min_sketch(e: Column, eps: Column, confidence: Column): Column =
    count_min_sketch(e, eps, confidence, lit(randomSeed()))

  /** Aggregate function: returns the first value of a column in a group. */
  def first(columnName: String, ignoreNulls: Boolean): Column =
    first(Column(columnName), ignoreNulls)

  /** Aggregate function: returns the last value of the column in a group. */
  def last(columnName: String, ignoreNulls: Boolean): Column =
    last(Column(columnName), ignoreNulls)

  /** Aggregate function: returns the most frequent value in a group. */
  def mode(e: Column, deterministic: Boolean): Column = Column.fn("mode", e, lit(deterministic))

  /** Aggregate function: returns an array of values associated with the top `k` values of `ord`. */
  def max_by(e: Column, ord: Column, k: Int): Column = Column.fn("max_by", e, ord, lit(k))

  /** Aggregate function: returns an array of values associated with the top `k` values of `ord`. */
  def max_by(e: Column, ord: Column, k: Column): Column = Column.fn("max_by", e, ord, k)

  /**
   * Aggregate function: returns an array of values associated with the bottom `k` values of `ord`.
   */
  def min_by(e: Column, ord: Column, k: Int): Column = Column.fn("min_by", e, ord, lit(k))

  /**
   * Aggregate function: returns an array of values associated with the bottom `k` values of `ord`.
   */
  def min_by(e: Column, ord: Column, k: Column): Column = Column.fn("min_by", e, ord, k)

  /**
   * Aggregate function: returns the approximate `percentile` of the numeric column `col` which is
   * the smallest value in the ordered `col` values (sorted from least to greatest) such that no
   * more than `percentage` of `col` values is less than the value or equal to that value.
   */
  def approx_percentile(e: Column, percentage: Column, accuracy: Column): Column =
    Column.fn("approx_percentile", e, percentage, accuracy)

  /** Aggregate function: alias for `stddev_samp`. */
  def std(e: Column): Column = Column.fn("std", e)

  /** Aggregate function: returns the concatenation of non-null input values. */
  def listagg(e: Column): Column = Column.fn("listagg", e)

  /**
   * Aggregate function: returns the concatenation of non-null input values, separated by the
   * delimiter.
   */
  def listagg(e: Column, delimiter: Column): Column = Column.fn("listagg", e, delimiter)

  /** Aggregate function: returns the concatenation of distinct non-null input values. */
  def listagg_distinct(e: Column): Column = Column.fn("listagg", isDistinct = true, e)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values, separated by
   * the delimiter.
   */
  def listagg_distinct(e: Column, delimiter: Column): Column =
    Column.fn("listagg", isDistinct = true, e, delimiter)

  /** Aggregate function: returns the concatenation of non-null input values. */
  def string_agg(e: Column): Column = Column.fn("string_agg", e)

  /**
   * Aggregate function: returns the concatenation of non-null input values, separated by the
   * delimiter.
   */
  def string_agg(e: Column, delimiter: Column): Column = Column.fn("string_agg", e, delimiter)

  /** Aggregate function: returns the concatenation of distinct non-null input values. */
  def string_agg_distinct(e: Column): Column = Column.fn("string_agg", isDistinct = true, e)

  /**
   * Aggregate function: returns the concatenation of distinct non-null input values, separated by
   * the delimiter.
   */
  def string_agg_distinct(e: Column, delimiter: Column): Column =
    Column.fn("string_agg", isDistinct = true, e, delimiter)

  /**
   * Aggregate function: returns the average of the independent variable for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_avgx(y: Column, x: Column): Column = Column.fn("regr_avgx", y, x)

  /**
   * Aggregate function: returns the average of the dependent variable for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_avgy(y: Column, x: Column): Column = Column.fn("regr_avgy", y, x)

  /**
   * Aggregate function: returns the number of non-null number pairs in a group, where `y` is the
   * dependent variable and `x` is the independent variable.
   */
  def regr_count(y: Column, x: Column): Column = Column.fn("regr_count", y, x)

  /**
   * Aggregate function: returns the intercept of the univariate linear regression line for non-null
   * pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_intercept(y: Column, x: Column): Column = Column.fn("regr_intercept", y, x)

  /**
   * Aggregate function: returns the coefficient of determination for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_r2(y: Column, x: Column): Column = Column.fn("regr_r2", y, x)

  /**
   * Aggregate function: returns the slope of the linear regression line for non-null pairs in a
   * group, where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_slope(y: Column, x: Column): Column = Column.fn("regr_slope", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where
   * `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_sxx(y: Column, x: Column): Column = Column.fn("regr_sxx", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group,
   * where `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_sxy(y: Column, x: Column): Column = Column.fn("regr_sxy", y, x)

  /**
   * Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where
   * `y` is the dependent variable and `x` is the independent variable.
   */
  def regr_syy(y: Column, x: Column): Column = Column.fn("regr_syy", y, x)

  /** Aggregate function: computes a histogram on numeric 'expr' using nb bins. */
  def histogram_numeric(e: Column, nBins: Column): Column =
    Column.fn("histogram_numeric", e, nBins)

  /**
   * Returns a bitmap with the positions of the bits set from all the values from the input column.
   */
  def bitmap_construct_agg(col: Column): Column =
    Column.fn("bitmap_construct_agg", col)

  /** Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column. */
  def bitmap_or_agg(col: Column): Column = Column.fn("bitmap_or_agg", col)

  /** Returns a bitmap that is the bitwise AND of all of the bitmaps from the input column. */
  def bitmap_and_agg(col: Column): Column = Column.fn("bitmap_and_agg", col)

  /** Aggregate function: returns a list of objects with duplicates. */
  def array_agg(e: Column): Column = Column.fn("array_agg", e)

  // -- Conditional / null handling --------------------------------------------

  /** Evaluates a list of conditions and returns one of multiple possible result expressions. */
  def when(condition: Column, value: Any): Column =
    Column.fn("when", condition, Column.lit(value))

  def coalesce(e: Column*): Column = Column.fn("coalesce", e*)
  def isnull(e: Column): Column = e.isNull
  def isnan(e: Column): Column = e.isNaN
  def nanvl(col1: Column, col2: Column): Column = Column.fn("nanvl", col1, col2)
  def nvl(col1: Column, col2: Column): Column = Column.fn("nvl", col1, col2)
  def nvl2(col1: Column, col2: Column, col3: Column): Column =
    Column.fn("nvl2", col1, col2, col3)
  def ifnull(col1: Column, col2: Column): Column = Column.fn("ifnull", col1, col2)
  def nullif(col1: Column, col2: Column): Column = Column.fn("nullif", col1, col2)
  def greatest(exprs: Column*): Column = Column.fn("greatest", exprs*)
  def least(exprs: Column*): Column = Column.fn("least", exprs*)

  /** Inversion of boolean expression, i.e. */
  def not(e: Column): Column = !e

  /** Returns true if `col` is not null, or false otherwise. */
  def isnotnull(col: Column): Column = Column.fn("isnotnull", col)

  /**
   * Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both
   * are null, false if one of the them is null.
   */
  def equal_null(col1: Column, col2: Column): Column = Column.fn("equal_null", col1, col2)

  /** Returns null if `col` is equal to zero, or `col` otherwise. */
  def nullifzero(col: Column): Column = Column.fn("nullifzero", col)

  /** Returns zero if `col` is null, or `col` otherwise. */
  def zeroifnull(col: Column): Column = Column.fn("zeroifnull", col)

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
  def round(e: Column): Column = round(e, 0)
  def round(e: Column, scale: Int): Column = Column.fn("round", e, lit(scale))
  def bround(e: Column): Column = bround(e, 0)
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
  def bitwise_not(e: Column): Column = Column.fn("~", e)
  @deprecated("Use bitwise_not", "3.2.0")
  def bitwiseNOT(e: Column): Column = bitwise_not(e)
  def bit_count(e: Column): Column = Column.fn("bit_count", e)

  /** Returns the sum of `left` and `right` and the result is null on overflow. */
  def try_add(left: Column, right: Column): Column = Column.fn("try_add", left, right)

  /** Returns the mean calculated from values of a group and the result is null on overflow. */
  def try_avg(e: Column): Column = Column.fn("try_avg", e)

  /** Returns `dividend``/``divisor`. */
  def try_divide(left: Column, right: Column): Column = Column.fn("try_divide", left, right)

  /** Returns the remainder of `dividend``/``divisor`. */
  def try_mod(left: Column, right: Column): Column = Column.fn("try_mod", left, right)

  /** Returns `left``*``right` and the result is null on overflow. */
  def try_multiply(left: Column, right: Column): Column = Column.fn("try_multiply", left, right)

  /** Returns `left``-``right` and the result is null on overflow. */
  def try_subtract(left: Column, right: Column): Column = Column.fn("try_subtract", left, right)

  /** Returns the sum calculated from values of a group and the result is null on overflow. */
  def try_sum(e: Column): Column = Column.fn("try_sum", e)

  /** Returns the value of the bit (0 or 1) at the specified position. */
  def bit_get(e: Column, pos: Column): Column = Column.fn("bit_get", e, pos)

  /** Returns the value of the bit (0 or 1) at the specified position. */
  def getbit(e: Column, pos: Column): Column = Column.fn("getbit", e, pos)

  def acosh(columnName: String): Column = acosh(Column(columnName))

  def asinh(columnName: String): Column = asinh(Column(columnName))

  def atan2(yName: String, xValue: Double): Column = atan2(Column(yName), xValue)

  def atan2(yValue: Double, xName: String): Column = atan2(yValue, Column(xName))

  def atanh(columnName: String): Column = atanh(Column(columnName))

  /** Computes the ceiling of the given value of `e` to `scale` decimal places. */
  def ceiling(e: Column, scale: Column): Column = Column.fn("ceiling", e, scale)

  /** Returns Euler's number. */
  def e(): Column = Column.fn("e")

  /** Returns the greatest value of the list of column names, skipping null values. */
  @scala.annotation.varargs
  def greatest(columnName: String, columnNames: String*): Column =
    greatest((columnName +: columnNames).map(Column.apply)*)

  /** Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow. */
  def hypot(leftName: String, r: Double): Column = hypot(Column(leftName), r)

  /** Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow. */
  def hypot(l: Double, rightName: String): Column = hypot(l, Column(rightName))

  /** Returns the least value of the list of column names, skipping null values. */
  @scala.annotation.varargs
  def least(columnName: String, columnNames: String*): Column =
    least((columnName +: columnNames).map(Column.apply)*)

  /** Returns Pi. */
  def pi(): Column = Column.fn("pi")

  /** Returns the value of the first argument raised to the power of the second argument. */
  def pow(leftName: String, r: Double): Column = pow(Column(leftName), r)

  /** Returns the value of the first argument raised to the power of the second argument. */
  def pow(l: Double, rightName: String): Column = pow(l, Column(rightName))

  /**
   * Round the value of `e` to `scale` decimal places with HALF_UP round mode if `scale` is greater
   * than or equal to 0 or at integral part when `scale` is less than 0.
   */
  def round(e: Column, scale: Column): Column = Column.fn("round", e, scale)

  /**
   * Round the value of `e` to `scale` decimal places with HALF_EVEN round mode if `scale` is
   * greater than or equal to 0 or at integral part when `scale` is less than 0.
   */
  def bround(e: Column, scale: Column): Column = Column.fn("bround", e, scale)

  /** Shift the given value numBits left. */
  @deprecated("Use shiftleft", "3.2.0")
  def shiftLeft(e: Column, numBits: Int): Column = shiftleft(e, numBits)

  /** (Signed) shift the given value numBits right. */
  @deprecated("Use shiftright", "3.2.0")
  def shiftRight(e: Column, numBits: Int): Column = shiftright(e, numBits)

  /** Unsigned shift the given value numBits right. */
  @deprecated("Use shiftrightunsigned", "3.2.0")
  def shiftRightUnsigned(e: Column, numBits: Int): Column = shiftrightunsigned(e, numBits)

  /** Computes the signum of the given value. */
  def sign(e: Column): Column = Column.fn("sign", e)

  @deprecated("Use degrees", "2.1.0")
  def toDegrees(columnName: String): Column = degrees(Column(columnName))

  @deprecated("Use radians", "2.1.0")
  def toRadians(columnName: String): Column = radians(Column(columnName))

  /**
   * Returns the bucket number into which the value of this expression would fall after being
   * evaluated.
   */
  def width_bucket(v: Column, min: Column, max: Column, numBucket: Column): Column =
    Column.fn("width_bucket", v, min, max, numBucket)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) values with the
   * specified range of numbers.
   */
  def uniform(min: Column, max: Column): Column =
    uniform(min, max, lit(randomSeed()))

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) values with the
   * specified range of numbers, with the chosen random seed.
   */
  def uniform(min: Column, max: Column, seed: Column): Column =
    Column.fn("uniform", min, max, seed)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) uniformly
   * distributed values in [0, 1).
   */
  def random(seed: Column): Column = Column.fn("random", seed)

  /**
   * Returns a random value with independent and identically distributed (i.i.d.) uniformly
   * distributed values in [0, 1).
   */
  def random(): Column = random(lit(randomSeed()))

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

  /**
   * Returns a string of the specified length whose characters are chosen uniformly at random from
   * the following pool of characters: 0-9, a-z, A-Z.
   */
  def randstr(length: Column): Column =
    randstr(length, lit(randomSeed()))

  /**
   * Returns a string of the specified length whose characters are chosen uniformly at random from
   * the following pool of characters: 0-9, a-z, A-Z, with the chosen random seed.
   */
  def randstr(length: Column, seed: Column): Column = Column.fn("randstr", length, seed)

  /**
   * Computes the BASE32 (RFC 4648) encoding of a binary column and returns it as a string column.
   */
  def to_base32(e: Column): Column = Column.fn("to_base32", e)

  /** Returns true if the input is a valid UTF-8 string, otherwise returns false. */
  def is_valid_utf8(str: Column): Column =
    Column.fn("is_valid_utf8", str)

  /**
   * Returns a new string in which all invalid UTF-8 byte sequences, if any, are replaced by the
   * Unicode replacement character (U+FFFD).
   */
  def make_valid_utf8(str: Column): Column =
    Column.fn("make_valid_utf8", str)

  /**
   * Returns the input value if it corresponds to a valid UTF-8 string, or emits a
   * SparkIllegalArgumentException exception otherwise.
   */
  def validate_utf8(str: Column): Column =
    Column.fn("validate_utf8", str)

  /** Returns the input value if it corresponds to a valid UTF-8 string, or NULL otherwise. */
  def try_validate_utf8(str: Column): Column =
    Column.fn("try_validate_utf8", str)

  /** Locate the position of the first occurrence of substr column in the given string. */
  def instr(str: Column, substring: Column): Column = Column.fn("instr", str, substring)

  /**
   * Locate the position of the first occurrence of `substring` in `str`, starting the search from
   * position `start`.
   */
  def instr(str: Column, substring: Column, start: Int): Column =
    Column.fn("instr", str, substring, lit(start))

  /**
   * Locate the position of the first occurrence of `substring` in `str`, starting the search from
   * position `start`.
   */
  def instr(str: Column, substring: Column, start: Column): Column =
    Column.fn("instr", str, substring, start)

  /**
   * Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
   * search from position `start`.
   */
  def instr(str: Column, substring: Column, start: Int, occurrence: Int): Column =
    Column.fn("instr", str, substring, lit(start), lit(occurrence))

  /**
   * Locate the position of the `occurrence`-th occurrence of `substring` in `str`, starting the
   * search from position `start`.
   */
  def instr(str: Column, substring: Column, start: Column, occurrence: Column): Column =
    Column.fn("instr", str, substring, start, occurrence)

  /** Computes the character length of a given string or number of bytes of a binary string. */
  def len(e: Column): Column = Column.fn("len", e)

  /** Computes the Jaro-Winkler similarity between the two given string columns. */
  def jaro_winkler_similarity(l: Column, r: Column): Column =
    Column.fn("jaro_winkler_similarity", l, r)

  /** Left-pad the binary column with pad to a byte length of len. */
  def lpad(str: Column, len: Int, pad: Array[Byte]): Column = lpad(str, lit(len), lit(pad))

  /** Left-pad the string column with pad to a length of len. */
  def lpad(str: Column, len: Column, pad: Column): Column = Column.fn("lpad", str, len, pad)

  /** Trim the specified character string from left end for the specified string column. */
  def ltrim(e: Column, trim: Column): Column = Column.fn("ltrim", trim, e)

  /** Marks a given column with specified collation. */
  def collate(e: Column, collation: String): Column = Column.fn("collate", e, lit(collation))

  /** Returns the collation name of a given column. */
  def collation(e: Column): Column = Column.fn("collation", e)

  /** Returns true if `str` matches `regexp`, or false otherwise. */
  def rlike(str: Column, regexp: Column): Column = Column.fn("rlike", str, regexp)

  /** Returns true if `str` matches `regexp`, or false otherwise. */
  def regexp(str: Column, regexp: Column): Column = Column.fn("regexp", str, regexp)

  /**
   * Replace all substrings of the specified string value that match regexp with rep, starting at
   * the specified position `pos`.
   */
  def regexp_replace(e: Column, pattern: String, replacement: String, pos: Int): Column =
    regexp_replace(e, lit(pattern), lit(replacement), lit(pos))

  /**
   * Replace all substrings of the specified string value that match regexp with rep, starting at
   * the specified position `pos`.
   */
  def regexp_replace(e: Column, pattern: Column, replacement: Column, pos: Column): Column =
    Column.fn("regexp_replace", e, pattern, replacement, pos)

  /**
   * Searches a string for a regular expression and returns an integer that indicates the beginning
   * position of the matched substring.
   */
  def regexp_instr(str: Column, regexp: Column, idx: Column): Column =
    Column.fn("regexp_instr", str, regexp, idx)

  /** Decodes a BASE32 (RFC 4648) encoded string column and returns it as a binary column. */
  def from_base32(e: Column): Column = Column.fn("from_base32", e)

  /** Right-pad the binary column with pad to a byte length of len. */
  def rpad(str: Column, len: Int, pad: Array[Byte]): Column = rpad(str, lit(len), lit(pad))

  /** Right-pad the string column with pad to a length of len. */
  def rpad(str: Column, len: Column, pad: Column): Column = Column.fn("rpad", str, len, pad)

  /** Repeats a string column n times, and returns it as a new string column. */
  def repeat(str: Column, n: Column): Column = Column.fn("repeat", str, n)

  /** Trim the specified character string from right end for the specified string column. */
  def rtrim(e: Column, trim: Column): Column = Column.fn("rtrim", trim, e)

  /** Splits str around matches of the given pattern. */
  def split(str: Column, pattern: Column): Column = Column.fn("split", str, pattern)

  /** Splits str around matches of the given pattern. */
  def split(str: Column, pattern: Column, limit: Column): Column =
    Column.fn("split", str, pattern, limit)

  /** Splits a string into arrays of sentences, where each sentence is an array of words. */
  def sentences(string: Column, language: Column): Column =
    Column.fn("sentences", string, language)

  /** Trim the specified character from both ends for the specified string column. */
  def trim(e: Column, trim: Column): Column = Column.fn("trim", trim, e)

  /** Converts the input `e` to a binary value based on the supplied `format`. */
  def to_binary(e: Column, f: Column): Column = Column.fn("to_binary", e, f)

  /** Converts the input `e` to a binary value based on the default format "hex". */
  def to_binary(e: Column): Column = Column.fn("to_binary", e)

  /**
   * Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of
   * byte array that starts at `pos` and is of length `len`.
   */
  def substr(str: Column, pos: Column, len: Column): Column =
    Column.fn("substr", str, pos, len)

  /**
   * Returns the substring of `str` that starts at `pos`, or the slice of byte array that starts at
   * `pos`.
   */
  def substr(str: Column, pos: Column): Column = Column.fn("substr", str, pos)

  /** Formats the arguments in printf-style and returns the result as a string column. */
  @scala.annotation.varargs
  def printf(format: Column, arguments: Column*): Column =
    Column.fn("printf", (format +: arguments)*)

  /** Returns the position of the first occurrence of `substr` in `str` after position `start`. */
  def position(substr: Column, str: Column, start: Column): Column =
    Column.fn("position", substr, str, start)

  /** Returns the position of the first occurrence of `substr` in `str` after position `1`. */
  def position(substr: Column, str: Column): Column =
    Column.fn("position", substr, str)

  /** Returns the ASCII character having the binary equivalent to `n`. */
  def char(n: Column): Column = Column.fn("char", n)

  /**
   * This is a special version of `to_binary` that performs the same operation, but returns a NULL
   * value instead of raising an error if the conversion cannot be performed.
   */
  def try_to_binary(e: Column, f: Column): Column = Column.fn("try_to_binary", e, f)

  /**
   * This is a special version of `to_binary` that performs the same operation, but returns a NULL
   * value instead of raising an error if the conversion cannot be performed.
   */
  def try_to_binary(e: Column): Column = Column.fn("try_to_binary", e)

  /** Convert string `e` to a number based on the string format `format`. */
  def try_to_number(e: Column, format: Column): Column = Column.fn("try_to_number", e, format)

  /** Returns the ASCII character having the binary equivalent to `n`. */
  def chr(n: Column): Column = Column.fn("chr", n)

  /** Returns the `n`-th input, e.g., returns `input2` when `n` is 2. */
  @scala.annotation.varargs
  def elt(inputs: Column*): Column = Column.fn("elt", inputs*)

  /**
   * Returns the index (1-based) of the given string (`str`) in the comma-delimited list
   * (`strArray`).
   */
  def find_in_set(str: Column, strArray: Column): Column = Column.fn("find_in_set", str, strArray)

  /**
   * Returns true if str matches `pattern` with `escapeChar`, null if any arguments are null, false
   * otherwise.
   */
  def like(str: Column, pattern: Column, escapeChar: Column): Column =
    Column.fn("like", str, pattern, escapeChar)

  /**
   * Returns true if str matches `pattern` with `escapeChar`('\'), null if any arguments are null,
   * false otherwise.
   */
  def like(str: Column, pattern: Column): Column = Column.fn("like", str, pattern)

  /**
   * Returns true if str matches `pattern` with `escapeChar` case-insensitively, null if any
   * arguments are null, false otherwise.
   */
  def ilike(str: Column, pattern: Column, escapeChar: Column): Column =
    Column.fn("ilike", str, pattern, escapeChar)

  /**
   * Returns true if str matches `pattern` with `escapeChar`('\') case-insensitively, null if any
   * arguments are null, false otherwise.
   */
  def ilike(str: Column, pattern: Column): Column = Column.fn("ilike", str, pattern)

  /**
   * Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len`
   * is less or equal than 0 the result is an empty string.
   */
  def left(str: Column, len: Column): Column = Column.fn("left", str, len)

  /**
   * Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if
   * `len` is less or equal than 0 the result is an empty string.
   */
  def right(str: Column, len: Column): Column = Column.fn("right", str, len)

  /**
   * Returns `str` enclosed by single quotes and each instance of single quote in it is preceded by
   * a backslash.
   */
  def quote(str: Column): Column = Column.fn("quote", str)

  /** Masks the given string value. */
  def mask(input: Column): Column = Column.fn("mask", input)

  /** Masks the given string value. */
  def mask(input: Column, upperChar: Column): Column =
    Column.fn("mask", input, upperChar)

  /** Masks the given string value. */
  def mask(input: Column, upperChar: Column, lowerChar: Column): Column =
    Column.fn("mask", input, upperChar, lowerChar)

  /** Masks the given string value. */
  def mask(input: Column, upperChar: Column, lowerChar: Column, digitChar: Column): Column =
    Column.fn("mask", input, upperChar, lowerChar, digitChar)

  /** Masks the given string value. */
  def mask(
      input: Column,
      upperChar: Column,
      lowerChar: Column,
      digitChar: Column,
      otherChar: Column
  ): Column =
    Column.fn("mask", input, upperChar, lowerChar, digitChar, otherChar)

  // -- Hashing -----------------------------------------------------------------

  def md5(e: Column): Column = Column.fn("md5", e)
  def sha1(e: Column): Column = Column.fn("sha1", e)
  def sha2(e: Column, numBits: Int): Column = Column.fn("sha2", e, lit(numBits))
  def crc32(e: Column): Column = Column.fn("crc32", e)
  def hash(cols: Column*): Column = Column.fn("hash", cols*)
  def xxhash64(cols: Column*): Column = Column.fn("xxhash64", cols*)

  /** Returns a sha1 hash value as a hex string of the `col`. */
  def sha(col: Column): Column = Column.fn("sha", col)

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
  def unix_timestamp(): Column = unix_timestamp(current_timestamp())
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
    window(timeColumn, windowDuration, windowDuration, "0 second")
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column =
    window(timeColumn, windowDuration, slideDuration, "0 second")
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

  /** Create time from hour, minute and second fields. */
  def make_time(hour: Column, minute: Column, second: Column): Column =
    Column.fn("make_time", hour, minute, second)

  /** Returns the current time at the start of query evaluation. */
  def current_time(): Column =
    Column.fn("current_time")

  /** Returns the current time at the start of query evaluation. */
  def current_time(precision: Int): Column =
    Column.fn("current_time", lit(precision))

  /** Returns the current date at the start of query evaluation as a date column. */
  def curdate(): Column = Column.fn("curdate")

  /** Returns the date that is `days` days after `start` */
  def dateadd(start: Column, days: Column): Column = Column.fn("dateadd", start, days)

  /** Parses a string value to a time value. */
  def to_time(str: Column): Column =
    Column.fn("to_time", str)

  /** Parses a string value to a time value. */
  def to_time(str: Column, format: Column): Column =
    Column.fn("to_time", str, format)

  /** Parses a string value to a time value. */
  def try_to_time(str: Column): Column =
    Column.fn("try_to_time", str)

  /** Parses a string value to a time value. */
  def try_to_time(str: Column, format: Column): Column =
    Column.fn("try_to_time", str, format)

  /** Parses the `s` with the `format` to a timestamp. */
  def try_to_timestamp(s: Column, format: Column): Column =
    Column.fn("try_to_timestamp", s, format)

  /** Parses the `s` to a timestamp. */
  def try_to_timestamp(s: Column): Column = Column.fn("try_to_timestamp", s)

  /**
   * This is a special version of `to_date` that performs the same operation, but returns a NULL
   * value instead of raising an error if date cannot be created.
   */
  def try_to_date(e: Column): Column = Column.fn("try_to_date", e)

  /**
   * This is a special version of `to_date` that performs the same operation, but returns a NULL
   * value instead of raising an error if date cannot be created.
   */
  def try_to_date(e: Column, fmt: String): Column = Column.fn("try_to_date", e, lit(fmt))

  /**
   * Returns the number of nanoseconds since 1970-01-01 00:00:00 UTC for a nanosecond-precision
   * timestamp (`TIMESTAMP_LTZ(p)` / `TIMESTAMP_NTZ(p)`, `p` in `[7, 9]`).
   */
  def unix_nanos(e: Column): Column = Column.fn("unix_nanos", e)

  /** Extracts the event time from the window column. */
  def window_time(windowColumn: Column): Column = Column.fn("window_time", windowColumn)

  /** Generates session window given a timestamp specifying column. */
  def session_window(timeColumn: Column, gapDuration: Column): Column =
    Column.fn("session_window", timeColumn, gapDuration)

  /**
   * Creates a timestamp with the local time zone and nanosecond precision (TIMESTAMP_LTZ(9)) from
   * the number of nanoseconds since UTC epoch.
   */
  def timestamp_nanos(e: Column): Column = Column.fn("timestamp_nanos", e)

  /**
   * Gets the difference between the timestamps in the specified units by truncating the fraction
   * part.
   */
  def timestamp_diff(unit: String, start: Column, end: Column): Column =
    Column.internalFn("timestampdiff", lit(unit), start, end)

  /** Adds the specified number of units to the given timestamp. */
  def timestamp_add(unit: String, quantity: Column, ts: Column): Column =
    Column.internalFn("timestampadd", lit(unit), quantity, ts)

  /**
   * Returns the start of the fixed-size bucket of `bucketSize` that contains `ts`, with buckets
   * aligned to the default origin (1970-01-01 00:00:00).
   */
  def time_bucket(bucketSize: Column, ts: Column): Column =
    Column.fn("time_bucket", bucketSize, ts)

  /**
   * Returns the start of the fixed-size bucket of `bucketSize` that contains `ts`, with buckets
   * aligned to `origin`.
   */
  def time_bucket(bucketSize: Column, ts: Column, origin: Column): Column =
    Column.fn("time_bucket", bucketSize, ts, origin)

  /** Returns the difference between two times, measured in specified units. */
  def time_diff(unit: Column, start: Column, end: Column): Column =
    Column.fn("time_diff", unit, start, end)

  /** Returns `time` truncated to the `unit`. */
  def time_trunc(unit: Column, time: Column): Column =
    Column.fn("time_trunc", unit, time)

  /** Creates a TIME from the number of seconds since midnight. */
  def time_from_seconds(e: Column): Column = Column.fn("time_from_seconds", e)

  /** Creates a TIME from the number of milliseconds since midnight. */
  def time_from_millis(e: Column): Column = Column.fn("time_from_millis", e)

  /** Creates a TIME from the number of microseconds since midnight. */
  def time_from_micros(e: Column): Column = Column.fn("time_from_micros", e)

  /** Extracts the number of seconds (including fractional seconds) from a TIME value. */
  def time_to_seconds(e: Column): Column = Column.fn("time_to_seconds", e)

  /** Extracts the number of milliseconds since midnight from a TIME value. */
  def time_to_millis(e: Column): Column = Column.fn("time_to_millis", e)

  /** Extracts the number of microseconds since midnight from a TIME value. */
  def time_to_micros(e: Column): Column = Column.fn("time_to_micros", e)

  /**
   * Parses the `timestamp` expression with the `format` expression to a timestamp with local time
   * zone.
   */
  def to_timestamp_ltz(timestamp: Column, format: Column): Column =
    Column.fn("to_timestamp_ltz", timestamp, format)

  /**
   * Parses the `timestamp` expression with the default format to a timestamp with local time zone.
   */
  def to_timestamp_ltz(timestamp: Column): Column =
    Column.fn("to_timestamp_ltz", timestamp)

  /**
   * Parses the `timestamp_str` expression with the `format` expression to a timestamp without time
   * zone.
   */
  def to_timestamp_ntz(timestamp: Column, format: Column): Column =
    Column.fn("to_timestamp_ntz", timestamp, format)

  /** Parses the `timestamp` expression with the default format to a timestamp without time zone. */
  def to_timestamp_ntz(timestamp: Column): Column =
    Column.fn("to_timestamp_ntz", timestamp)

  /** Returns the UNIX timestamp of the given time. */
  def to_unix_timestamp(timeExp: Column, format: Column): Column =
    Column.fn("to_unix_timestamp", timeExp, format)

  /** Returns the UNIX timestamp of the given time. */
  def to_unix_timestamp(timeExp: Column): Column =
    Column.fn("to_unix_timestamp", timeExp)

  /** Extracts the three-letter abbreviated month name from a given date/timestamp/string. */
  def monthname(timeExp: Column): Column =
    Column.fn("monthname", timeExp)

  /** Extracts the three-letter abbreviated day name from a given date/timestamp/string. */
  def dayname(timeExp: Column): Column =
    Column.fn("dayname", timeExp)

  /**
   * Converts the timestamp without time zone `sourceTs` from the `sourceTz` time zone to
   * `targetTz`.
   */
  def convert_timezone(sourceTz: Column, targetTz: Column, sourceTs: Column): Column =
    Column.fn("convert_timezone", sourceTz, targetTz, sourceTs)

  /**
   * Converts the timestamp without time zone `sourceTs` from the current time zone to `targetTz`.
   */
  def convert_timezone(targetTz: Column, sourceTs: Column): Column =
    Column.fn("convert_timezone", targetTz, sourceTs)

  /** Make DayTimeIntervalType duration from days, hours, mins and secs. */
  def make_dt_interval(days: Column, hours: Column, mins: Column, secs: Column): Column =
    Column.fn("make_dt_interval", days, hours, mins, secs)

  /** Make DayTimeIntervalType duration from days, hours and mins. */
  def make_dt_interval(days: Column, hours: Column, mins: Column): Column =
    Column.fn("make_dt_interval", days, hours, mins)

  /** Make DayTimeIntervalType duration from days and hours. */
  def make_dt_interval(days: Column, hours: Column): Column =
    Column.fn("make_dt_interval", days, hours)

  /** Make DayTimeIntervalType duration from days. */
  def make_dt_interval(days: Column): Column =
    Column.fn("make_dt_interval", days)

  /** Make DayTimeIntervalType duration. */
  def make_dt_interval(): Column =
    Column.fn("make_dt_interval")

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours, mins, secs)

  /** Make interval from years, months, weeks, days, hours, mins and secs. */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("make_interval", years, months, weeks, days, hours, mins, secs)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column
  ): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours, mins)

  /** Make interval from years, months, weeks, days, hours and mins. */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column
  ): Column =
    Column.fn("make_interval", years, months, weeks, days, hours, mins)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column
  ): Column =
    Column.fn("try_make_interval", years, months, weeks, days, hours)

  /** Make interval from years, months, weeks, days and hours. */
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column
  ): Column =
    Column.fn("make_interval", years, months, weeks, days, hours)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(years: Column, months: Column, weeks: Column, days: Column): Column =
    Column.fn("try_make_interval", years, months, weeks, days)

  /** Make interval from years, months, weeks and days. */
  def make_interval(years: Column, months: Column, weeks: Column, days: Column): Column =
    Column.fn("make_interval", years, months, weeks, days)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(years: Column, months: Column, weeks: Column): Column =
    Column.fn("try_make_interval", years, months, weeks)

  /** Make interval from years, months and weeks. */
  def make_interval(years: Column, months: Column, weeks: Column): Column =
    Column.fn("make_interval", years, months, weeks)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(years: Column, months: Column): Column =
    Column.fn("try_make_interval", years, months)

  /** Make interval from years and months. */
  def make_interval(years: Column, months: Column): Column =
    Column.fn("make_interval", years, months)

  /**
   * This is a special version of `make_interval` that performs the same operation, but returns a
   * NULL value instead of raising an error if interval cannot be created.
   */
  def try_make_interval(years: Column): Column =
    Column.fn("try_make_interval", years)

  /** Make interval from years. */
  def make_interval(years: Column): Column =
    Column.fn("make_interval", years)

  /** Make interval. */
  def make_interval(): Column =
    Column.fn("make_interval")

  /** Create timestamp from years, months, days, hours, mins, secs and timezone fields. */
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    Column.fn("make_timestamp", years, months, days, hours, mins, secs, timezone)

  /** Create a local date-time from date, time, and timezone fields. */
  def make_timestamp(date: Column, time: Column, timezone: Column): Column =
    Column.fn("make_timestamp", date, time, timezone)

  /** Create a local date-time from date and time fields. */
  def make_timestamp(date: Column, time: Column): Column =
    Column.fn("make_timestamp", date, time)

  /** Try to create a timestamp from years, months, days, hours, mins, secs and timezone fields. */
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    Column.fn("try_make_timestamp", years, months, days, hours, mins, secs, timezone)

  /** Try to create a timestamp from years, months, days, hours, mins, and secs fields. */
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("try_make_timestamp", years, months, days, hours, mins, secs)

  /** Try to create a local date-time from date, time, and timezone fields. */
  def try_make_timestamp(date: Column, time: Column, timezone: Column): Column =
    Column.fn("try_make_timestamp", date, time, timezone)

  /** Try to create a local date-time from date and time fields. */
  def try_make_timestamp(date: Column, time: Column): Column =
    Column.fn("try_make_timestamp", date, time)

  /**
   * Create the current timestamp with local time zone from years, months, days, hours, mins, secs
   * and timezone fields.
   */
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    Column.fn("make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)

  /**
   * Create the current timestamp with local time zone from years, months, days, hours, mins and
   * secs fields.
   */
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("make_timestamp_ltz", years, months, days, hours, mins, secs)

  /**
   * Try to create the current timestamp with local time zone from years, months, days, hours, mins,
   * secs and timezone fields.
   */
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    Column.fn("try_make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)

  /**
   * Try to create the current timestamp with local time zone from years, months, days, hours, mins
   * and secs fields.
   */
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("try_make_timestamp_ltz", years, months, days, hours, mins, secs)

  /** Create local date-time from years, months, days, hours, mins, secs fields. */
  def make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("make_timestamp_ntz", years, months, days, hours, mins, secs)

  /** Create a local date-time from date and time fields. */
  def make_timestamp_ntz(date: Column, time: Column): Column =
    Column.fn("make_timestamp_ntz", date, time)

  /** Try to create a local date-time from years, months, days, hours, mins, secs fields. */
  def try_make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    Column.fn("try_make_timestamp_ntz", years, months, days, hours, mins, secs)

  /** Try to create a local date-time from date and time fields. */
  def try_make_timestamp_ntz(date: Column, time: Column): Column =
    Column.fn("try_make_timestamp_ntz", date, time)

  /** Make year-month interval from years, months. */
  def make_ym_interval(years: Column, months: Column): Column =
    Column.fn("make_ym_interval", years, months)

  /** Make year-month interval from years. */
  def make_ym_interval(years: Column): Column = Column.fn("make_ym_interval", years)

  /** Make year-month interval. */
  def make_ym_interval(): Column = Column.fn("make_ym_interval")

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
  def shuffle(e: Column): Column = shuffle(e, lit(randomSeed()))
  def arrays_zip(e: Column*): Column = Column.fn("arrays_zip", e*)

  /** Separates `col1`, ..., `colk` into `n` rows. */
  @scala.annotation.varargs
  def stack(cols: Column*): Column = Column.fn("stack", cols*)

  /** (array, index) - Returns element of array at given (1-based) index. */
  def try_element_at(column: Column, value: Column): Column =
    Column.fn("try_element_at", column, value)

  /** Returns the total number of elements in the array. */
  def array_size(e: Column): Column = Column.fn("array_size", e)

  /** Returns a random permutation of the given array. */
  def shuffle(e: Column, seed: Column): Column = Column.fn("shuffle", e, seed)

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

  /** Creates a map after splitting the text into key/value pairs using delimiters. */
  def str_to_map(text: Column, pairDelim: Column, keyValueDelim: Column): Column =
    Column.fn("str_to_map", text, pairDelim, keyValueDelim)

  /** Creates a map after splitting the text into key/value pairs using delimiters. */
  def str_to_map(text: Column, pairDelim: Column): Column =
    Column.fn("str_to_map", text, pairDelim)

  /** Creates a map after splitting the text into key/value pairs using delimiters. */
  def str_to_map(text: Column): Column = Column.fn("str_to_map", text)

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

  /**
   * (Scala-specific) Parses a column containing a JSON string into a `StructType` with the
   * specified schema.
   */
  def from_json(e: Column, schema: StructType, options: Map[String, String]): Column =
    from_json(e, schema.asInstanceOf[DataType], options)

  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType` as
   * keys type, `StructType` or `ArrayType` with the specified schema.
   */
  def from_json(e: Column, schema: DataType, options: Map[String, String]): Column =
    from_json(e, lit(schema.sql), options.iterator)

  /**
   * (Java-specific) Parses a column containing a JSON string into a `StructType` with the specified
   * schema.
   */
  def from_json(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType` as
   * keys type, `StructType` or `ArrayType` with the specified schema.
   */
  def from_json(e: Column, schema: DataType, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  /** Parses a column containing a JSON string into a `StructType` with the specified schema. */
  def from_json(e: Column, schema: StructType): Column =
    from_json(e, schema, Map.empty[String, String])

  /**
   * Parses a column containing a JSON string into a `MapType` with `StringType` as keys type,
   * `StructType` or `ArrayType` with the specified schema.
   */
  def from_json(e: Column, schema: DataType): Column =
    from_json(e, schema, Map.empty[String, String])

  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType` as
   * keys type, `StructType` or `ArrayType` with the specified schema.
   */
  def from_json(e: Column, schema: String, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.toMap)

  /**
   * (Scala-specific) Parses a column containing a JSON string into a `MapType` with `StringType` as
   * keys type, `StructType` or `ArrayType` with the specified schema.
   */
  def from_json(e: Column, schema: String, options: Map[String, String]): Column =
    from_json(e, lit(schema), options.asJava)

  /**
   * (Java-specific) Parses a column containing a JSON string into a `MapType` with `StringType` as
   * keys type, `StructType` or `ArrayType` of `StructType`s with the specified schema.
   */
  def from_json(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_json(e, schema, options.asScala.iterator)

  /** Parses a JSON string and infers its schema in DDL format using options. */
  def schema_of_json(json: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_json", options.asScala.iterator, json)

  /** Returns the number of elements in the outermost JSON array. */
  def json_array_length(e: Column): Column = Column.fn("json_array_length", e)

  /** Returns all the keys of the outermost JSON object as an array. */
  def json_object_keys(e: Column): Column = Column.fn("json_object_keys", e)

  /**
   * (Scala-specific) Converts a column containing a `StructType`, `ArrayType` or a `MapType` into a
   * JSON string with the specified schema.
   */
  def to_json(e: Column, options: Map[String, String]): Column =
    Column.fnWithOptions("to_json", options.iterator, e)

  /**
   * (Java-specific) Converts a column containing a `StructType`, `ArrayType` or a `MapType` into a
   * JSON string with the specified schema.
   */
  def to_json(e: Column, options: java.util.Map[String, String]): Column =
    to_json(e, options.asScala.toMap)

  /** Parses a column containing a CSV string into a `StructType` with the specified schema. */
  def from_csv(e: Column, schema: StructType, options: Map[String, String]): Column =
    from_csv(e, lit(schema.toDDL), options.iterator)

  /**
   * (Java-specific) Parses a column containing a CSV string into a `StructType` with the specified
   * schema.
   */
  def from_csv(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_csv(e, schema, options.asScala.iterator)

  /** Parses a CSV string and infers its schema in DDL format using options. */
  def schema_of_csv(csv: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_csv", options.asScala.iterator, csv)

  /**
   * (Java-specific) Converts a column containing a `StructType` into a CSV string with the
   * specified schema.
   */
  def to_csv(e: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("to_csv", options.asScala.iterator, e)

  // -- XML ---------------------------------------------------------------------

  /**
   * Parses a column containing a XML string into the data type corresponding to the specified
   * schema.
   */
  def from_xml(e: Column, schema: StructType, options: java.util.Map[String, String]): Column =
    from_xml(e, lit(schema.sql), options.asScala.iterator)

  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the specified
   * schema.
   */
  def from_xml(e: Column, schema: String, options: java.util.Map[String, String]): Column =
    from_xml(e, lit(schema), options)

  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the specified
   * schema.
   */
  def from_xml(e: Column, schema: Column): Column =
    from_xml(e, schema, Iterator.empty)

  /**
   * (Java-specific) Parses a column containing a XML string into a `StructType` with the specified
   * schema.
   */
  def from_xml(e: Column, schema: Column, options: java.util.Map[String, String]): Column =
    from_xml(e, schema, options.asScala.iterator)

  /**
   * Parses a column containing a XML string into the data type corresponding to the specified
   * schema.
   */
  def from_xml(e: Column, schema: StructType): Column =
    from_xml(e, schema, Map.empty[String, String].asJava)

  /** Parses a XML string and infers its schema in DDL format. */
  def schema_of_xml(xml: String): Column = schema_of_xml(lit(xml))

  /** Parses a XML string and infers its schema in DDL format. */
  def schema_of_xml(xml: Column): Column = Column.fn("schema_of_xml", xml)

  /** Parses a XML string and infers its schema in DDL format using options. */
  def schema_of_xml(xml: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("schema_of_xml", options.asScala.iterator, xml)

  /**
   * (Java-specific) Converts a column containing a `StructType` into a XML string with the
   * specified schema.
   */
  def to_xml(e: Column, options: java.util.Map[String, String]): Column =
    Column.fnWithOptions("to_xml", options.asScala.iterator, e)

  /** Converts a column containing a `StructType` into a XML string with the specified schema. */
  def to_xml(e: Column): Column = to_xml(e, Map.empty[String, String].asJava)

  /** Returns a string array of values within the nodes of xml that match the XPath expression. */
  def xpath(xml: Column, path: Column): Column =
    Column.fn("xpath", xml, path)

  /** Returns true if the XPath expression evaluates to true, or if a matching node is found. */
  def xpath_boolean(xml: Column, path: Column): Column =
    Column.fn("xpath_boolean", xml, path)

  /**
   * Returns a double value, the value zero if no match is found, or NaN if a match is found but the
   * value is non-numeric.
   */
  def xpath_double(xml: Column, path: Column): Column =
    Column.fn("xpath_double", xml, path)

  /**
   * Returns a double value, the value zero if no match is found, or NaN if a match is found but the
   * value is non-numeric.
   */
  def xpath_number(xml: Column, path: Column): Column =
    Column.fn("xpath_number", xml, path)

  /**
   * Returns a float value, the value zero if no match is found, or NaN if a match is found but the
   * value is non-numeric.
   */
  def xpath_float(xml: Column, path: Column): Column =
    Column.fn("xpath_float", xml, path)

  /**
   * Returns an integer value, or the value zero if no match is found, or a match is found but the
   * value is non-numeric.
   */
  def xpath_int(xml: Column, path: Column): Column =
    Column.fn("xpath_int", xml, path)

  /**
   * Returns a long integer value, or the value zero if no match is found, or a match is found but
   * the value is non-numeric.
   */
  def xpath_long(xml: Column, path: Column): Column =
    Column.fn("xpath_long", xml, path)

  /**
   * Returns a short integer value, or the value zero if no match is found, or a match is found but
   * the value is non-numeric.
   */
  def xpath_short(xml: Column, path: Column): Column =
    Column.fn("xpath_short", xml, path)

  /** Returns the text contents of the first xml node that matches the XPath expression. */
  def xpath_string(xml: Column, path: Column): Column =
    Column.fn("xpath_string", xml, path)

  // -- URL ---------------------------------------------------------------------

  /** Extracts a part from a URL. */
  def try_parse_url(url: Column, partToExtract: Column, key: Column): Column =
    Column.fn("try_parse_url", url, partToExtract, key)

  /** Extracts a part from a URL. */
  def try_parse_url(url: Column, partToExtract: Column): Column =
    Column.fn("try_parse_url", url, partToExtract)

  /** Extracts a part from a URL. */
  def parse_url(url: Column, partToExtract: Column, key: Column): Column =
    Column.fn("parse_url", url, partToExtract, key)

  /** Extracts a part from a URL. */
  def parse_url(url: Column, partToExtract: Column): Column =
    Column.fn("parse_url", url, partToExtract)

  /**
   * Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding scheme.
   */
  def url_decode(str: Column): Column = Column.fn("url_decode", str)

  /**
   * This is a special version of `url_decode` that performs the same operation, but returns a NULL
   * value instead of raising an error if the decoding cannot be performed.
   */
  def try_url_decode(str: Column): Column = Column.fn("try_url_decode", str)

  /**
   * Translates a string into 'application/x-www-form-urlencoded' format using a specific encoding
   * scheme.
   */
  def url_encode(str: Column): Column = Column.fn("url_encode", str)

  // -- Variant -----------------------------------------------------------------

  /** Parses a JSON string and constructs a Variant value. */
  def try_parse_json(json: Column): Column = Column.fn("try_parse_json", json)

  /** Parses a JSON string and constructs a Variant value. */
  def parse_json(json: Column): Column = Column.fn("parse_json", json)

  /**
   * Converts a column containing nested inputs (array/map/struct) into a variants where maps and
   * structs are converted to variant objects which are unordered unlike SQL structs.
   */
  def to_variant_object(col: Column): Column = Column.fn("to_variant_object", col)

  /** Check if a variant value is a variant null. */
  def is_variant_null(v: Column): Column = Column.fn("is_variant_null", v)

  /** Check if a variant value is valid. */
  def is_valid_variant(v: Column): Column = Column.fn("is_valid_variant", v)

  /** Removes fields or array elements from a variant at the given JSONPath locations. */
  @scala.annotation.varargs
  def variant_delete(v: Column, path: Column, paths: Column*): Column =
    Column.fn("variant_delete", (v +: path +: paths)*)

  /** Removes fields or array elements from a variant at the given JSONPath locations. */
  @scala.annotation.varargs
  def variant_delete(v: Column, path: String, paths: String*): Column =
    Column.fn("variant_delete", (v +: lit(path) +: paths.map(lit))*)

  /** Inserts a value into a variant at the given JSONPath location. */
  def variant_insert(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_insert", v, path, value)

  /** Inserts a value into a variant at the given JSONPath location. */
  def variant_insert(v: Column, path: String, value: Column): Column =
    Column.fn("variant_insert", v, lit(path), value)

  /** Inserts a value into a variant at the given JSONPath location. */
  def try_variant_insert(v: Column, path: Column, value: Column): Column =
    Column.fn("try_variant_insert", v, path, value)

  /** Inserts a value into a variant at the given JSONPath location. */
  def try_variant_insert(v: Column, path: String, value: Column): Column =
    Column.fn("try_variant_insert", v, lit(path), value)

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def variant_set(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_set", v, path, value)

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def variant_set(v: Column, path: String, value: Column): Column =
    Column.fn("variant_set", v, lit(path), value)

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def variant_set(v: Column, path: Column, value: Column, createIfMissing: Boolean): Column =
    Column.fn("variant_set", v, path, value, lit(createIfMissing))

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def variant_set(v: Column, path: String, value: Column, createIfMissing: Boolean): Column =
    Column.fn("variant_set", v, lit(path), value, lit(createIfMissing))

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def try_variant_set(v: Column, path: Column, value: Column): Column =
    Column.fn("try_variant_set", v, path, value)

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def try_variant_set(v: Column, path: String, value: Column): Column =
    Column.fn("try_variant_set", v, lit(path), value)

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def try_variant_set(v: Column, path: Column, value: Column, createIfMissing: Boolean): Column =
    Column.fn("try_variant_set", v, path, value, lit(createIfMissing))

  /** Sets or upserts a value in a variant at the given JSONPath location. */
  def try_variant_set(v: Column, path: String, value: Column, createIfMissing: Boolean): Column =
    Column.fn("try_variant_set", v, lit(path), value, lit(createIfMissing))

  /** Appends a value to the array in a variant at the given JSONPath location. */
  def variant_array_append(v: Column, path: Column, value: Column): Column =
    Column.fn("variant_array_append", v, path, value)

  /** Appends a value to the array in a variant at the given JSONPath location. */
  def variant_array_append(v: Column, path: String, value: Column): Column =
    Column.fn("variant_array_append", v, lit(path), value)

  /** Appends a value to the array in a variant at the given JSONPath location. */
  def try_variant_array_append(v: Column, path: Column, value: Column): Column =
    Column.fn("try_variant_array_append", v, path, value)

  /** Appends a value to the array in a variant at the given JSONPath location. */
  def try_variant_array_append(v: Column, path: String, value: Column): Column =
    Column.fn("try_variant_array_append", v, lit(path), value)

  /** Recursively removes object fields and array elements whose value is a variant null. */
  def variant_strip_nulls(v: Column): Column = Column.fn("variant_strip_nulls", v)

  /**
   * Recursively removes object fields and array elements whose value is a variant null, unless
   * `includeArrays` is false, in which case null array elements are kept.
   */
  def variant_strip_nulls(v: Column, includeArrays: Boolean): Column =
    Column.fn("variant_strip_nulls", v, lit(includeArrays))

  /**
   * Extracts a sub-variant from `v` according to `path` string, and then cast the sub-variant to
   * `targetType`.
   */
  def variant_get(v: Column, path: String, targetType: String): Column =
    Column.fn("variant_get", v, lit(path), lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` column, and then cast the sub-variant to
   * `targetType`.
   */
  def variant_get(v: Column, path: Column, targetType: String): Column =
    Column.fn("variant_get", v, path, lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` string, and then cast the sub-variant to
   * `targetType`.
   */
  def try_variant_get(v: Column, path: String, targetType: String): Column =
    Column.fn("try_variant_get", v, lit(path), lit(targetType))

  /**
   * Extracts a sub-variant from `v` according to `path` column, and then cast the sub-variant to
   * `targetType`.
   */
  def try_variant_get(v: Column, path: Column, targetType: String): Column =
    Column.fn("try_variant_get", v, lit(path), lit(targetType))

  /** Returns schema in the SQL format of a variant. */
  def schema_of_variant(v: Column): Column = Column.fn("schema_of_variant", v)

  /** Returns the merged schema in the SQL format of a variant column. */
  def schema_of_variant_agg(v: Column): Column = Column.fn("schema_of_variant_agg", v)

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
  def uuid(): Column = Column.fn("uuid", lit(randomSeed()))
  def typeof(e: Column): Column = Column.fn("typeof", e)
  def rand(): Column = rand(randomSeed())
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))
  def randn(): Column = randn(randomSeed())
  def randn(seed: Long): Column = Column.fn("randn", lit(seed))

  def monotonicallyIncreasingId(): Column = monotonically_increasing_id()

  /** Returns the current SQL path as a comma-separated list of qualified schema names. */
  def current_path(): Column = Column.fn("current_path")

  /** Returns null if the condition is true, and throws an exception otherwise. */
  def assert_true(c: Column): Column = Column.fn("assert_true", c)

  /**
   * Returns null if the condition is true; throws an exception with the error message otherwise.
   */
  def assert_true(c: Column, e: Column): Column = Column.fn("assert_true", c, e)

  /** Throws an exception with the provided error message. */
  def raise_error(c: Column): Column = Column.fn("raise_error", c)

  /** Returns the user name of current execution context. */
  def user(): Column = Column.fn("user")

  /** Returns the user name of current execution context. */
  def session_user(): Column = Column.fn("session_user")

  /** Returns an universally unique identifier (UUID) string. */
  def uuid(seed: Column): Column = Column.fn("uuid", seed)

  /**
   * Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and the
   * given hash `algorithm`.
   */
  def hmac(key: Column, message: Column, algorithm: Column): Column =
    Column.fn("hmac", key, message, algorithm)

  /**
   * Returns the keyed-hash message authentication code (HMAC) of `message` using `key` and SHA-256.
   */
  def hmac(key: Column, message: Column): Column =
    Column.fn("hmac", key, message)

  /**
   * Returns an encrypted value of `input` using AES in given `mode` with the specified `padding`.
   */
  def aes_encrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      iv: Column,
      aad: Column
  ): Column = Column.fn("aes_encrypt", input, key, mode, padding, iv, aad)

  /** Returns an encrypted value of `input`. */
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column, iv: Column): Column =
    Column.fn("aes_encrypt", input, key, mode, padding, iv)

  /** Returns an encrypted value of `input`. */
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("aes_encrypt", input, key, mode, padding)

  /** Returns an encrypted value of `input`. */
  def aes_encrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("aes_encrypt", input, key, mode)

  /** Returns an encrypted value of `input`. */
  def aes_encrypt(input: Column, key: Column): Column =
    Column.fn("aes_encrypt", input, key)

  /** Returns a decrypted value of `input` using AES in `mode` with `padding`. */
  def aes_decrypt(input: Column, key: Column, mode: Column, padding: Column, aad: Column): Column =
    Column.fn("aes_decrypt", input, key, mode, padding, aad)

  /** Returns a decrypted value of `input`. */
  def aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("aes_decrypt", input, key, mode, padding)

  /** Returns a decrypted value of `input`. */
  def aes_decrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("aes_decrypt", input, key, mode)

  /** Returns a decrypted value of `input`. */
  def aes_decrypt(input: Column, key: Column): Column =
    Column.fn("aes_decrypt", input, key)

  /**
   * This is a special version of `aes_decrypt` that performs the same operation, but returns a NULL
   * value instead of raising an error if the decryption cannot be performed.
   */
  def try_aes_decrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      aad: Column
  ): Column =
    Column.fn("try_aes_decrypt", input, key, mode, padding, aad)

  /** Returns a decrypted value of `input`. */
  def try_aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    Column.fn("try_aes_decrypt", input, key, mode, padding)

  /** Returns a decrypted value of `input`. */
  def try_aes_decrypt(input: Column, key: Column, mode: Column): Column =
    Column.fn("try_aes_decrypt", input, key, mode)

  /** Returns a decrypted value of `input`. */
  def try_aes_decrypt(input: Column, key: Column): Column =
    Column.fn("try_aes_decrypt", input, key)

  /** Calls a method with reflection. */
  @scala.annotation.varargs
  def reflect(cols: Column*): Column = Column.fn("reflect", cols*)

  /** Calls a method with reflection. */
  @scala.annotation.varargs
  def java_method(cols: Column*): Column = Column.fn("java_method", cols*)

  /**
   * This is a special version of `reflect` that performs the same operation, but returns a NULL
   * value instead of raising an error if the invoke method thrown exception.
   */
  @scala.annotation.varargs
  def try_reflect(cols: Column*): Column = Column.fn("try_reflect", cols*)

  /** Returns the bit position for the given input column. */
  def bitmap_bit_position(col: Column): Column =
    Column.fn("bitmap_bit_position", col)

  /** Returns the bucket number for the given input column. */
  def bitmap_bucket_number(col: Column): Column =
    Column.fn("bitmap_bucket_number", col)

  /** Returns the number of set bits in the input bitmap. */
  def bitmap_count(col: Column): Column = Column.fn("bitmap_count", col)

  /** Unwrap UDT data type column into its underlying type. */
  def unwrap_udt(column: Column): Column = Column.internalFn("unwrap_udt", column)

  // -- Partition transforms ----------------------------------------------------

  /**
   * Partition transform functions for `DataFrameWriterV2.partitionedBy`, e.g.
   * `df.writeTo("t").partitionedBy(partitioning.years(col("ts")))`. The deprecated top-level
   * aliases below forward here.
   */
  object partitioning {

    /** A transform for timestamps and dates to partition data into years. */
    def years(e: Column): Column = Column.internalFn("years", e)

    /** A transform for timestamps and dates to partition data into months. */
    def months(e: Column): Column = Column.internalFn("months", e)

    /** A transform for timestamps and dates to partition data into days. */
    def days(e: Column): Column = Column.internalFn("days", e)

    /** A transform for timestamps to partition data into hours. */
    def hours(e: Column): Column = Column.internalFn("hours", e)

    /** A transform for any type that partitions by a hash of the input column. */
    def bucket(numBuckets: Column, e: Column): Column = Column.internalFn("bucket", numBuckets, e)

    /** A transform for any type that partitions by a hash of the input column. */
    def bucket(numBuckets: Int, e: Column): Column = bucket(lit(numBuckets), e)
  }

  /** (Java-specific) A transform for timestamps and dates to partition data into years. */
  def years(e: Column): Column = partitioning.years(e)

  /** (Java-specific) A transform for timestamps and dates to partition data into months. */
  def months(e: Column): Column = partitioning.months(e)

  /** (Java-specific) A transform for timestamps and dates to partition data into days. */
  def days(e: Column): Column = partitioning.days(e)

  /** (Java-specific) A transform for timestamps to partition data into hours. */
  def hours(e: Column): Column = partitioning.hours(e)

  /** (Java-specific) A transform for any type that partitions by a hash of the input column. */
  def bucket(numBuckets: Column, e: Column): Column = partitioning.bucket(numBuckets, e)

  /** (Java-specific) A transform for any type that partitions by a hash of the input column. */
  def bucket(numBuckets: Int, e: Column): Column = partitioning.bucket(numBuckets, e)

  // -- Sketches (HLL, KLL, Theta, Tuple) ---------------------------------------

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   */
  def hll_sketch_agg(e: Column, lgConfigK: Column): Column =
    Column.fn("hll_sketch_agg", e, lgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   */
  def hll_sketch_agg(e: Column, lgConfigK: Int): Column =
    Column.fn("hll_sketch_agg", e, lit(lgConfigK))

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with lgConfigK arg.
   */
  def hll_sketch_agg(columnName: String, lgConfigK: Int): Column =
    hll_sketch_agg(Column(columnName), lgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with default lgConfigK value.
   */
  def hll_sketch_agg(e: Column): Column =
    Column.fn("hll_sketch_agg", e)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch
   * configured with default lgConfigK value.
   */
  def hll_sketch_agg(columnName: String): Column =
    hll_sketch_agg(Column(columnName))

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch,
   * generated by merging previously created Datasketches HllSketch instances via a Datasketches
   * Union instance.
   */
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Column): Column =
    Column.fn("hll_union_agg", e, allowDifferentLgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch,
   * generated by merging previously created Datasketches HllSketch instances via a Datasketches
   * Union instance.
   */
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Boolean): Column =
    Column.fn("hll_union_agg", e, lit(allowDifferentLgConfigK))

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch,
   * generated by merging previously created Datasketches HllSketch instances via a Datasketches
   * Union instance.
   */
  def hll_union_agg(columnName: String, allowDifferentLgConfigK: Boolean): Column =
    hll_union_agg(Column(columnName), allowDifferentLgConfigK)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch,
   * generated by merging previously created Datasketches HllSketch instances via a Datasketches
   * Union instance.
   */
  def hll_union_agg(e: Column): Column =
    Column.fn("hll_union_agg", e)

  /**
   * Aggregate function: returns the updatable binary representation of the Datasketches HllSketch,
   * generated by merging previously created Datasketches HllSketch instances via a Datasketches
   * Union instance.
   */
  def hll_union_agg(columnName: String): Column =
    hll_union_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by intersecting the Datasketches ThetaSketch instances in the input column via a
   * Datasketches Intersection instance.
   */
  def theta_intersection_agg(e: Column): Column =
    Column.fn("theta_intersection_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by intersecting the Datasketches ThetaSketch instances in the input volumn via a
   * Datasketches Intersection instance.
   */
  def theta_intersection_agg(columnName: String): Column =
    theta_intersection_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   */
  def theta_sketch_agg(e: Column, lgNomEntries: Column): Column =
    Column.fn("theta_sketch_agg", e, lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   */
  def theta_sketch_agg(e: Column, lgNomEntries: Int): Column =
    Column.fn("theta_sketch_agg", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the `lgNomEntries` nominal
   * entries.
   */
  def theta_sketch_agg(columnName: String, lgNomEntries: Int): Column =
    theta_sketch_agg(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the default value of 12 for
   * `lgNomEntries`.
   */
  def theta_sketch_agg(e: Column): Column =
    Column.fn("theta_sketch_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch
   * built with the values in the input column and configured with the default value of 12 for
   * `lgNomEntries`.
   */
  def theta_sketch_agg(columnName: String): Column =
    theta_sketch_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by the union of Datasketches ThetaSketch instances in the input column via a
   * Datasketches Union instance.
   */
  def theta_union_agg(e: Column, lgNomEntries: Column): Column =
    Column.fn("theta_union_agg", e, lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by the union of Datasketches ThetaSketch instances in the input column via a
   * Datasketches Union instance.
   */
  def theta_union_agg(e: Column, lgNomEntries: Int): Column =
    Column.fn("theta_union_agg", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by the union of Datasketches ThetaSketch instances in the input column via a
   * Datasketches Union instance.
   */
  def theta_union_agg(columnName: String, lgNomEntries: Int): Column =
    theta_union_agg(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by the union of Datasketches ThetaSketch instances in the input column via a
   * Datasketches Union instance.
   */
  def theta_union_agg(e: Column): Column =
    Column.fn("theta_union_agg", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches ThetaSketch,
   * generated by the union of Datasketches ThetaSketch instances in the input column via a
   * Datasketches Union instance.
   */
  def theta_union_agg(columnName: String): Column =
    theta_union_agg(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances in
   * the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_double(e: Column, mode: Column): Column =
    Column.fn("tuple_intersection_agg_double", e, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances in
   * the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_double(e: Column, mode: String): Column =
    Column.fn("tuple_intersection_agg_double", e, lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances in
   * the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_double(columnName: String, mode: String): Column =
    tuple_intersection_agg_double(Column(columnName), mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances in
   * the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_double(e: Column): Column =
    Column.fn("tuple_intersection_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by intersecting the Datasketches TupleSketch instances in
   * the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_double(columnName: String): Column =
    tuple_intersection_agg_double(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_integer(e: Column, mode: Column): Column =
    Column.fn("tuple_intersection_agg_integer", e, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_integer(e: Column, mode: String): Column =
    Column.fn("tuple_intersection_agg_integer", e, lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_integer(columnName: String, mode: String): Column =
    tuple_intersection_agg_integer(Column(columnName), mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_integer(e: Column): Column =
    Column.fn("tuple_intersection_agg_integer", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by intersecting the Datasketches TupleSketch instances
   * in the input column via a Datasketches Intersection instance.
   */
  def tuple_intersection_agg_integer(columnName: String): Column =
    tuple_intersection_agg_integer(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_double(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries.
   */
  def tuple_sketch_agg_double(key: Column, summary: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_sketch_agg_double", key, summary, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries.
   */
  def tuple_sketch_agg_double(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int
  ): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns.
   */
  def tuple_sketch_agg_double(key: Column, summary: Column): Column =
    Column.fn("tuple_sketch_agg_double", key, summary)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary built with the key and summary values in the input columns.
   */
  def tuple_sketch_agg_double(keyColumnName: String, summaryColumnName: String): Column =
    tuple_sketch_agg_double(Column(keyColumnName), Column(summaryColumnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries and aggregation mode.
   */
  def tuple_sketch_agg_integer(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries.
   */
  def tuple_sketch_agg_integer(key: Column, summary: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns and
   * configured with the `lgNomEntries` nominal entries.
   */
  def tuple_sketch_agg_integer(
      keyColumnName: String,
      summaryColumnName: String,
      lgNomEntries: Int
  ): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns.
   */
  def tuple_sketch_agg_integer(key: Column, summary: Column): Column =
    Column.fn("tuple_sketch_agg_integer", key, summary)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary built with the key and summary values in the input columns.
   */
  def tuple_sketch_agg_integer(keyColumnName: String, summaryColumnName: String): Column =
    tuple_sketch_agg_integer(Column(keyColumnName), Column(summaryColumnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_agg_double", e, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_agg_double", e, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(columnName: String, lgNomEntries: Int, mode: String): Column =
    tuple_union_agg_double(Column(columnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(e: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_agg_double", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(columnName: String, lgNomEntries: Int): Column =
    tuple_union_agg_double(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(e: Column): Column =
    Column.fn("tuple_union_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with a double type summary, generated by the union of Datasketches TupleSketch instances in the
   * input column via a Datasketches Union instance.
   */
  def tuple_union_agg_double(columnName: String): Column =
    tuple_union_agg_double(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_agg_integer", e, lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_agg_integer", e, lit(lgNomEntries), lit(mode))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(columnName: String, lgNomEntries: Int, mode: String): Column =
    tuple_union_agg_integer(Column(columnName), lgNomEntries, mode)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_agg_integer", e, lit(lgNomEntries))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(columnName: String, lgNomEntries: Int): Column =
    tuple_union_agg_integer(Column(columnName), lgNomEntries)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(e: Column): Column =
    Column.fn("tuple_union_agg_integer", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches TupleSketch
   * with an integer type summary, generated by the union of Datasketches TupleSketch instances in
   * the input column via a Datasketches Union instance.
   */
  def tuple_union_agg_integer(columnName: String): Column =
    tuple_union_agg_integer(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column.
   */
  def kll_sketch_agg_bigint(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_bigint", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column.
   */
  def kll_sketch_agg_bigint(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_bigint", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column.
   */
  def kll_sketch_agg_bigint(columnName: String, k: Int): Column =
    kll_sketch_agg_bigint(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_bigint(e: Column): Column =
    Column.fn("kll_sketch_agg_bigint", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllLongsSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_bigint(columnName: String): Column =
    kll_sketch_agg_bigint(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column.
   */
  def kll_sketch_agg_float(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_float", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column.
   */
  def kll_sketch_agg_float(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_float", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column.
   */
  def kll_sketch_agg_float(columnName: String, k: Int): Column =
    kll_sketch_agg_float(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_float(e: Column): Column =
    Column.fn("kll_sketch_agg_float", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllFloatsSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_float(columnName: String): Column =
    kll_sketch_agg_float(Column(columnName))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column.
   */
  def kll_sketch_agg_double(e: Column, k: Column): Column =
    Column.fn("kll_sketch_agg_double", e, k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column.
   */
  def kll_sketch_agg_double(e: Column, k: Int): Column =
    Column.fn("kll_sketch_agg_double", e, lit(k))

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column.
   */
  def kll_sketch_agg_double(columnName: String, k: Int): Column =
    kll_sketch_agg_double(Column(columnName), k)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_double(e: Column): Column =
    Column.fn("kll_sketch_agg_double", e)

  /**
   * Aggregate function: returns the compact binary representation of the Datasketches
   * KllDoublesSketch built with the values in the input column with default k value of 200.
   */
  def kll_sketch_agg_double(columnName: String): Column =
    kll_sketch_agg_double(Column(columnName))

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged sketch.
   */
  def kll_merge_agg_bigint(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_bigint", e, k)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged sketch.
   */
  def kll_merge_agg_bigint(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_bigint", e, lit(k))

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged sketch.
   */
  def kll_merge_agg_bigint(columnName: String, k: Int): Column =
    kll_merge_agg_bigint(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged sketch.
   */
  def kll_merge_agg_bigint(e: Column): Column =
    Column.fn("kll_merge_agg_bigint", e)

  /**
   * Aggregate function: merges binary KllLongsSketch representations and returns the merged sketch.
   */
  def kll_merge_agg_bigint(columnName: String): Column =
    kll_merge_agg_bigint(Column(columnName))

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   */
  def kll_merge_agg_float(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_float", e, k)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   */
  def kll_merge_agg_float(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_float", e, lit(k))

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   */
  def kll_merge_agg_float(columnName: String, k: Int): Column =
    kll_merge_agg_float(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   */
  def kll_merge_agg_float(e: Column): Column =
    Column.fn("kll_merge_agg_float", e)

  /**
   * Aggregate function: merges binary KllFloatsSketch representations and returns merged sketch.
   */
  def kll_merge_agg_float(columnName: String): Column =
    kll_merge_agg_float(Column(columnName))

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   */
  def kll_merge_agg_double(e: Column, k: Column): Column =
    Column.fn("kll_merge_agg_double", e, k)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   */
  def kll_merge_agg_double(e: Column, k: Int): Column =
    Column.fn("kll_merge_agg_double", e, lit(k))

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   */
  def kll_merge_agg_double(columnName: String, k: Int): Column =
    kll_merge_agg_double(Column(columnName), k)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   */
  def kll_merge_agg_double(e: Column): Column =
    Column.fn("kll_merge_agg_double", e)

  /**
   * Aggregate function: merges binary KllDoublesSketch representations and returns merged sketch.
   */
  def kll_merge_agg_double(columnName: String): Column =
    kll_merge_agg_double(Column(columnName))

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * HllSketch.
   */
  def hll_sketch_estimate(c: Column): Column = Column.fn("hll_sketch_estimate", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * HllSketch.
   */
  def hll_sketch_estimate(columnName: String): Column =
    hll_sketch_estimate(Column(columnName))

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches Union
   * object.
   */
  def hll_union(c1: Column, c2: Column): Column =
    Column.fn("hll_union", c1, c2)

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches Union
   * object.
   */
  def hll_union(columnName1: String, columnName2: String): Column =
    hll_union(Column(columnName1), Column(columnName2))

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches Union
   * object.
   */
  def hll_union(c1: Column, c2: Column, allowDifferentLgConfigK: Boolean): Column =
    Column.fn("hll_union", c1, c2, lit(allowDifferentLgConfigK))

  /**
   * Merges two binary representations of Datasketches HllSketch objects, using a Datasketches Union
   * object.
   */
  def hll_union(
      columnName1: String,
      columnName2: String,
      allowDifferentLgConfigK: Boolean
  ): Column =
    hll_union(Column(columnName1), Column(columnName2), allowDifferentLgConfigK)

  /**
   * Subtracts two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches AnotB object
   */
  def theta_difference(c1: Column, c2: Column): Column =
    Column.fn("theta_difference", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches AnotB object
   */
  def theta_difference(columnName1: String, columnName2: String): Column =
    theta_difference(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Intersection object
   */
  def theta_intersection(c1: Column, c2: Column): Column =
    Column.fn("theta_intersection", c1, c2)

  /**
   * Intersects two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Intersection object
   */
  def theta_intersection(columnName1: String, columnName2: String): Column =
    theta_intersection(Column(columnName1), Column(columnName2))

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * ThetaSketch.
   */
  def theta_sketch_estimate(c: Column): Column = Column.fn("theta_sketch_estimate", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * ThetaSketch.
   */
  def theta_sketch_estimate(columnName: String): Column =
    theta_sketch_estimate(Column(columnName))

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object.
   */
  def theta_union(c1: Column, c2: Column): Column =
    Column.fn("theta_union", c1, c2)

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object.
   */
  def theta_union(columnName1: String, columnName2: String): Column =
    theta_union(Column(columnName1), Column(columnName2))

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object.
   */
  def theta_union(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("theta_union", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object.
   */
  def theta_union(columnName1: String, columnName2: String, lgNomEntries: Int): Column =
    theta_union(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions two binary representations of Datasketches ThetaSketch objects in the input columns
   * using a Datasketches Union object.
   */
  def theta_union(c1: Column, c2: Column, lgNomEntries: Column): Column =
    Column.fn("theta_union", c1, c2, lgNomEntries)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_double", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_double(columnName1: String, columnName2: String): Column =
    tuple_difference_double(Column(columnName1), Column(columnName2))

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_integer", c1, c2)

  /**
   * Subtracts two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_integer(columnName1: String, columnName2: String): Column =
    tuple_difference_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_double", c1, c2)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_double(columnName1: String, columnName2: String): Column =
    tuple_intersection_double(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_double(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_double", c1, c2, lit(mode))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_double(columnName1: String, columnName2: String, mode: String): Column =
    tuple_intersection_double(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with double summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_double(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_double", c1, c2, mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_integer", c1, c2)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_integer(columnName1: String, columnName2: String): Column =
    tuple_intersection_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_integer(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_integer", c1, c2, lit(mode))

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_integer(columnName1: String, columnName2: String, mode: String): Column =
    tuple_intersection_integer(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects two binary representations of Datasketches TupleSketch objects with integer summary
   * data type in the input columns using a Datasketches Intersection object.
   */
  def tuple_intersection_integer(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_integer", c1, c2, mode)

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * TupleSketch with double summary data type.
   */
  def tuple_sketch_estimate_double(c: Column): Column =
    Column.fn("tuple_sketch_estimate_double", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * TupleSketch with double summary data type.
   */
  def tuple_sketch_estimate_double(columnName: String): Column =
    tuple_sketch_estimate_double(Column(columnName))

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * TupleSketch with integer summary data type.
   */
  def tuple_sketch_estimate_integer(c: Column): Column =
    Column.fn("tuple_sketch_estimate_integer", c)

  /**
   * Returns the estimated number of unique values given the binary representation of a Datasketches
   * TupleSketch with integer summary data type.
   */
  def tuple_sketch_estimate_integer(columnName: String): Column =
    tuple_sketch_estimate_integer(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   */
  def tuple_sketch_summary_double(c: Column): Column =
    Column.fn("tuple_sketch_summary_double", c)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   */
  def tuple_sketch_summary_double(columnName: String): Column =
    tuple_sketch_summary_double(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   */
  def tuple_sketch_summary_double(c: Column, mode: String): Column =
    Column.fn("tuple_sketch_summary_double", c, lit(mode))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   */
  def tuple_sketch_summary_double(columnName: String, mode: String): Column =
    tuple_sketch_summary_double(Column(columnName), mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with double summary data type.
   */
  def tuple_sketch_summary_double(c: Column, mode: Column): Column =
    Column.fn("tuple_sketch_summary_double", c, mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   */
  def tuple_sketch_summary_integer(c: Column): Column =
    Column.fn("tuple_sketch_summary_integer", c)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   */
  def tuple_sketch_summary_integer(columnName: String): Column =
    tuple_sketch_summary_integer(Column(columnName))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   */
  def tuple_sketch_summary_integer(c: Column, mode: String): Column =
    Column.fn("tuple_sketch_summary_integer", c, lit(mode))

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   */
  def tuple_sketch_summary_integer(columnName: String, mode: String): Column =
    tuple_sketch_summary_integer(Column(columnName), mode)

  /**
   * Aggregates the summary values from a Datasketches TupleSketch with integer summary data type.
   */
  def tuple_sketch_summary_integer(c: Column, mode: Column): Column =
    Column.fn("tuple_sketch_summary_integer", c, mode)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with double summary
   * data type.
   */
  def tuple_sketch_theta_double(c: Column): Column =
    Column.fn("tuple_sketch_theta_double", c)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with double summary
   * data type.
   */
  def tuple_sketch_theta_double(columnName: String): Column =
    tuple_sketch_theta_double(Column(columnName))

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with integer summary
   * data type.
   */
  def tuple_sketch_theta_integer(c: Column): Column =
    Column.fn("tuple_sketch_theta_integer", c)

  /**
   * Returns the theta value (sampling rate) from a Datasketches TupleSketch with integer summary
   * data type.
   */
  def tuple_sketch_theta_integer(columnName: String): Column =
    tuple_sketch_theta_integer(Column(columnName))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_double", c1, c2)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(columnName1: String, columnName2: String): Column =
    tuple_union_double(Column(columnName1), Column(columnName2))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_double", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(columnName1: String, columnName2: String, lgNomEntries: Int): Column =
    tuple_union_double(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_double", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_union_double(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with double summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_double", c1, c2, lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_integer", c1, c2)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(columnName1: String, columnName2: String): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_integer", c1, c2, lit(lgNomEntries))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(columnName1: String, columnName2: String, lgNomEntries: Int): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_integer", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_union_integer(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions two binary representations of Datasketches TupleSketch objects with integer summary data
   * type in the input columns using a Datasketches Union object.
   */
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_integer", c1, c2, lgNomEntries, mode)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * double summary data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_theta_double", c1, c2)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * double summary data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_theta_double(columnName1: String, columnName2: String): Column =
    tuple_difference_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * integer summary data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_difference_theta_integer", c1, c2)

  /**
   * Subtracts the binary representation of a Datasketches ThetaSketch from a TupleSketch with
   * integer summary data type in the input columns using a Datasketches AnotB object.
   */
  def tuple_difference_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_difference_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_double(columnName1: String, columnName2: String): Column =
    tuple_intersection_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2, lit(mode))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_double(
      columnName1: String,
      columnName2: String,
      mode: String
  ): Column =
    tuple_intersection_theta_double(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with double summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_theta_double", c1, c2, mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_intersection_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: String): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2, lit(mode))

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_integer(
      columnName1: String,
      columnName2: String,
      mode: String
  ): Column =
    tuple_intersection_theta_integer(Column(columnName1), Column(columnName2), mode)

  /**
   * Intersects the binary representation of a Datasketches TupleSketch with integer summary data
   * type with a Datasketches ThetaSketch in the input columns using a Datasketches Intersection
   * object.
   */
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: Column): Column =
    Column.fn("tuple_intersection_theta_integer", c1, c2, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_theta_double", c1, c2)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(columnName1: String, columnName2: String): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lit(lgNomEntries))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int
  ): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_union_theta_double(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with double summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Column, mode: Column): Column =
    Column.fn("tuple_union_theta_double", c1, c2, lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column): Column =
    Column.fn("tuple_union_theta_integer", c1, c2)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(columnName1: String, columnName2: String): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lit(lgNomEntries))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int
  ): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2), lgNomEntries)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(c1: Column, c2: Column, lgNomEntries: Int, mode: String): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lit(lgNomEntries), lit(mode))

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(
      columnName1: String,
      columnName2: String,
      lgNomEntries: Int,
      mode: String
  ): Column =
    tuple_union_theta_integer(Column(columnName1), Column(columnName2), lgNomEntries, mode)

  /**
   * Unions the binary representation of a Datasketches TupleSketch with integer summary data type
   * with a Datasketches ThetaSketch in the input columns using a Datasketches Union object.
   */
  def tuple_union_theta_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    Column.fn("tuple_union_theta_integer", c1, c2, lgNomEntries, mode)

  /** Returns a string with human readable summary information about the KLL bigint sketch. */
  def kll_sketch_to_string_bigint(e: Column): Column =
    Column.fn("kll_sketch_to_string_bigint", e)

  /** Returns a string with human readable summary information about the KLL float sketch. */
  def kll_sketch_to_string_float(e: Column): Column =
    Column.fn("kll_sketch_to_string_float", e)

  /** Returns a string with human readable summary information about the KLL double sketch. */
  def kll_sketch_to_string_double(e: Column): Column =
    Column.fn("kll_sketch_to_string_double", e)

  /** Returns the number of items collected in the KLL bigint sketch. */
  def kll_sketch_get_n_bigint(e: Column): Column =
    Column.fn("kll_sketch_get_n_bigint", e)

  /** Returns the number of items collected in the KLL float sketch. */
  def kll_sketch_get_n_float(e: Column): Column =
    Column.fn("kll_sketch_get_n_float", e)

  /** Returns the number of items collected in the KLL double sketch. */
  def kll_sketch_get_n_double(e: Column): Column =
    Column.fn("kll_sketch_get_n_double", e)

  /** Merges two KLL bigint sketch buffers together into one. */
  def kll_sketch_merge_bigint(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_bigint", left, right)

  /** Merges two KLL float sketch buffers together into one. */
  def kll_sketch_merge_float(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_float", left, right)

  /** Merges two KLL double sketch buffers together into one. */
  def kll_sketch_merge_double(left: Column, right: Column): Column =
    Column.fn("kll_sketch_merge_double", left, right)

  /** Extracts a quantile value from a KLL bigint sketch given an input rank value. */
  def kll_sketch_get_quantile_bigint(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_bigint", sketch, rank)

  /** Extracts a quantile value from a KLL float sketch given an input rank value. */
  def kll_sketch_get_quantile_float(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_float", sketch, rank)

  /** Extracts a quantile value from a KLL double sketch given an input rank value. */
  def kll_sketch_get_quantile_double(sketch: Column, rank: Column): Column =
    Column.fn("kll_sketch_get_quantile_double", sketch, rank)

  /** Extracts a rank value from a KLL bigint sketch given an input quantile value. */
  def kll_sketch_get_rank_bigint(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_bigint", sketch, quantile)

  /** Extracts a rank value from a KLL float sketch given an input quantile value. */
  def kll_sketch_get_rank_float(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_float", sketch, quantile)

  /** Extracts a rank value from a KLL double sketch given an input quantile value. */
  def kll_sketch_get_rank_double(sketch: Column, quantile: Column): Column =
    Column.fn("kll_sketch_get_rank_double", sketch, quantile)

  // -- Geospatial (ST) ---------------------------------------------------------

  /** Returns the input GEOGRAPHY or GEOMETRY value in WKB format. */
  def st_asbinary(geo: Column): Column =
    Column.fn("st_asbinary", geo)

  /** Returns the input GEOGRAPHY or GEOMETRY value in WKB format using the specified endianness. */
  def st_asbinary(geo: Column, endianness: Column): Column =
    Column.fn("st_asbinary", geo, endianness)

  /** Returns the input GEOGRAPHY or GEOMETRY value in WKB format using the specified endianness. */
  def st_asbinary(geo: Column, endianness: String): Column =
    Column.fn("st_asbinary", geo, lit(endianness))

  /** Parses the WKB description of a geography and returns the corresponding GEOGRAPHY value. */
  def st_geogfromwkb(wkb: Column): Column =
    Column.fn("st_geogfromwkb", wkb)

  /** Parses the WKB description of a geometry and returns the corresponding GEOMETRY value. */
  def st_geomfromwkb(wkb: Column): Column =
    Column.fn("st_geomfromwkb", wkb)

  /** Parses the WKB description of a geometry and returns the corresponding GEOMETRY value. */
  def st_geomfromwkb(wkb: Column, srid: Column): Column =
    Column.fn("st_geomfromwkb", wkb, srid)

  /** Parses the WKB description of a geometry and returns the corresponding GEOMETRY value. */
  def st_geomfromwkb(wkb: Column, srid: Int): Column =
    Column.fn("st_geomfromwkb", wkb, lit(srid))

  /** Returns a new GEOGRAPHY or GEOMETRY value whose SRID is the specified SRID value. */
  def st_setsrid(geo: Column, srid: Column): Column =
    Column.fn("st_setsrid", geo, srid)

  /** Returns a new GEOGRAPHY or GEOMETRY value whose SRID is the specified SRID value. */
  def st_setsrid(geo: Column, srid: Int): Column =
    Column.fn("st_setsrid", geo, lit(srid))

  /** Returns the SRID of the input GEOGRAPHY or GEOMETRY value. */
  def st_srid(geo: Column): Column =
    Column.fn("st_srid", geo)

  // -- Vectors -----------------------------------------------------------------

  /** Returns the cosine similarity between two float vectors. */
  def vector_cosine_similarity(left: Column, right: Column): Column =
    Column.fn("vector_cosine_similarity", left, right)

  /** Returns the inner product (dot product) between two float vectors. */
  def vector_inner_product(left: Column, right: Column): Column =
    Column.fn("vector_inner_product", left, right)

  /** Returns the Euclidean (L2) distance between two float vectors. */
  def vector_l2_distance(left: Column, right: Column): Column =
    Column.fn("vector_l2_distance", left, right)

  /** Returns the Lp norm of a float vector. */
  def vector_norm(vector: Column, degree: Column): Column =
    Column.fn("vector_norm", vector, degree)

  /** Returns the Lp norm of a float vector using degree 2.0 (Euclidean norm). */
  def vector_norm(vector: Column): Column =
    Column.fn("vector_norm", vector)

  /** Normalizes a float vector to unit length. */
  def vector_normalize(vector: Column, degree: Column): Column =
    Column.fn("vector_normalize", vector, degree)

  /** Normalizes a float vector to unit length using degree 2.0 (Euclidean norm). */
  def vector_normalize(vector: Column): Column =
    Column.fn("vector_normalize", vector)

  /** Aggregate function: returns the element-wise mean of float vectors in a group. */
  def vector_avg(col: Column): Column = Column.fn("vector_avg", col)

  /** Aggregate function: returns the element-wise sum of float vectors in a group. */
  def vector_sum(col: Column): Column = Column.fn("vector_sum", col)

  // -- Window functions --------------------------------------------------------

  def row_number(): Column = Column.fn("row_number")
  def rank(): Column = Column.fn("rank")
  def dense_rank(): Column = Column.fn("dense_rank")
  def percent_rank(): Column = Column.fn("percent_rank")
  def cume_dist(): Column = Column.fn("cume_dist")
  def ntile(n: Int): Column = Column.fn("ntile", lit(n))
  def lag(e: Column, offset: Int): Column = lag(e, offset, null)
  def lag(columnName: String, offset: Int): Column = lag(columnName, offset, null)
  def lag(e: Column, offset: Int, defaultValue: Any): Column = lag(e, offset, defaultValue, false)
  def lead(e: Column, offset: Int): Column = lead(e, offset, null)
  def lead(columnName: String, offset: Int): Column = lead(columnName, offset, null)
  def lead(e: Column, offset: Int, defaultValue: Any): Column =
    lead(e, offset, defaultValue, false)
  def nth_value(e: Column, offset: Int): Column = nth_value(e, offset, false)
  def nth_value(e: Column, offset: Int, ignoreNulls: Boolean): Column =
    Column.fn("nth_value", e, lit(offset), lit(ignoreNulls))

  /**
   * Window function: computes the differences between consecutive cumulative counter values in a
   * time series, thereby converting the counter from the cumulative to the delta format.
   */
  def counter_diff(value: Column): Column = Column.fn("counter_diff", value)

  /**
   * Window function: computes the differences between consecutive cumulative counter values in a
   * time series, thereby converting the counter from the cumulative to the delta format.
   */
  def counter_diff(value: Column, startTime: Column): Column =
    Column.fn("counter_diff", value, startTime)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row.
   */
  def lag(columnName: String, offset: Int, defaultValue: Any): Column =
    lag(Column(columnName), offset, defaultValue)

  /**
   * Window function: returns the value that is `offset` rows before the current row, and
   * `defaultValue` if there is less than `offset` rows before the current row.
   */
  def lag(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    Column.fn("lag", false, e, lit(offset), lit(defaultValue), lit(ignoreNulls))

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row.
   */
  def lead(columnName: String, offset: Int, defaultValue: Any): Column =
    lead(Column(columnName), offset, defaultValue)

  /**
   * Window function: returns the value that is `offset` rows after the current row, and
   * `defaultValue` if there is less than `offset` rows after the current row.
   */
  def lead(e: Column, offset: Int, defaultValue: Any, ignoreNulls: Boolean): Column =
    Column.fn("lead", false, e, lit(offset), lit(defaultValue), lit(ignoreNulls))

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
      ),
      body.subqueryRelations
    )
  }

  private def createLambda(f: Column => Column): Column = createLambda(1, cs => f(cs(0)))

  private def createLambda(f: (Column, Column) => Column): Column =
    createLambda(2, cs => f(cs(0), cs(1)))

  private def createLambda(f: (Column, Column, Column) => Column): Column =
    createLambda(3, cs => f(cs(0), cs(1), cs(2)))

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

  /** Sorts the input array based on the given comparator function. */
  def array_sort(e: Column, comparator: (Column, Column) => Column): Column =
    Column.fn("array_sort", e, createLambda(comparator))

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

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state.
   */
  def reduce(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    Column.fn("reduce", expr, initialValue, createLambda(merge), createLambda(finish))

  /**
   * Applies a binary operator to an initial state and all elements in the array, and reduces this
   * to a single state.
   */
  def reduce(expr: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    reduce(expr, initialValue, merge, c => c)
  def broadcast[U](df: Dataset[U]): Dataset[U] = df.hint("broadcast")

  // -- Internal helpers ---------------------------------------------------------

  private val seedGenerator = new java.util.Random()

  /**
   * A random seed for the nondeterministic functions (`rand`, `uuid`, `shuffle`, ...). Like Apache
   * Spark, it is fixed when the column is built, so a DataFrame returns the same values across
   * actions.
   */
  private def randomSeed(): Long = seedGenerator.nextLong()

  private def from_json(e: Column, schema: Column, options: Iterator[(String, String)]): Column =
    Column.fnWithOptions("from_json", options, e, schema)

  private def from_csv(e: Column, schema: Column, options: Iterator[(String, String)]): Column =
    Column.fnWithOptions("from_csv", options, e, schema)

  private def from_xml(e: Column, schema: Column, options: Iterator[(String, String)]): Column =
    Column.fnWithOptions("from_xml", options, e, schema)
}
