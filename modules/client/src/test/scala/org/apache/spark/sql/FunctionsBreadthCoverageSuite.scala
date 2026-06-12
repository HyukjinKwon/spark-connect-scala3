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

import org.apache.spark.sql.functions.*

/**
 * Breadth coverage for the single-`Column` entry points of the [[functions]] object: every such
 * function is invoked and must build a defined expression. This is a pure plan-building check (no
 * server) that guards against a wrapper failing to compile or producing an empty expression.
 */
class FunctionsBreadthCoverageSuite extends munit.FunSuite {

  private val a: Column = col("x")

  private def defined(c: Column): Boolean = c.expr.exprType.isDefined

  test("every single-column function builds a defined expression") {
    val built: Seq[Column] = Seq(
      abs(a),
      acos(a),
      acosh(a),
      any(a),
      any_value(a),
      approx_count_distinct(a),
      array_compact(a),
      array_distinct(a),
      array_max(a),
      array_min(a),
      array_sort(a),
      ascii(a),
      asin(a),
      asinh(a),
      atan(a),
      atanh(a),
      avg(a),
      base64(a),
      bin(a),
      bit_and(a),
      bit_count(a),
      bit_length(a),
      bit_or(a),
      bit_xor(a),
      bitwise_not(a),
      bitwiseNOT(a),
      bool_and(a),
      bool_or(a),
      bround(a),
      cardinality(a),
      cbrt(a),
      ceil(a),
      ceiling(a),
      char_length(a),
      character_length(a),
      collect_list(a),
      collect_set(a),
      cos(a),
      cosh(a),
      cot(a),
      count(a),
      count_if(a),
      crc32(a),
      csc(a),
      date_from_unix_date(a),
      day(a),
      dayofmonth(a),
      dayofweek(a),
      dayofyear(a),
      degrees(a),
      every(a),
      exp(a),
      explode(a),
      explode_outer(a),
      expm1(a),
      factorial(a),
      first(a),
      first_value(a),
      flatten(a),
      floor(a),
      grouping(a),
      hex(a),
      hour(a),
      initcap(a),
      `inline`(a),
      inline_outer(a),
      isnan(a),
      isnull(a),
      kurtosis(a),
      last(a),
      last_day(a),
      last_value(a),
      lcase(a),
      length(a),
      ln(a),
      log(a),
      log10(a),
      log1p(a),
      log2(a),
      lower(a),
      ltrim(a),
      map_entries(a),
      map_from_entries(a),
      map_keys(a),
      map_values(a),
      max(a),
      md5(a),
      mean(a),
      median(a),
      min(a),
      minute(a),
      mode(a),
      month(a),
      negate(a),
      negative(a),
      octet_length(a),
      posexplode(a),
      posexplode_outer(a),
      positive(a),
      product(a),
      quarter(a),
      radians(a),
      reverse(a),
      rint(a),
      round(a),
      rtrim(a),
      sec(a),
      second(a),
      sha1(a),
      shuffle(a),
      signum(a),
      sin(a),
      sinh(a),
      size(a),
      skewness(a),
      some(a),
      sort_array(a),
      soundex(a),
      sqrt(a),
      stddev(a),
      stddev_pop(a),
      stddev_samp(a),
      sum(a),
      sum_distinct(a),
      sumDistinct(a),
      tan(a),
      tanh(a),
      timestamp_micros(a),
      timestamp_millis(a),
      timestamp_seconds(a),
      to_csv(a),
      to_date(a),
      to_json(a),
      to_timestamp(a),
      toDegrees(a),
      toRadians(a),
      trim(a),
      typeof(a),
      ucase(a),
      unbase64(a),
      unhex(a),
      unix_date(a),
      unix_micros(a),
      unix_millis(a),
      unix_seconds(a),
      upper(a),
      var_pop(a),
      var_samp(a),
      variance(a),
      weekday(a),
      weekofyear(a),
      year(a)
    )
    built.foreach(c => assert(defined(c), s"undefined expression: ${c.expr}"))
    assertEquals(built.size, 152)
  }
}
