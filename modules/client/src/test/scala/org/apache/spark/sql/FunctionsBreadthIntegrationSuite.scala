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

import org.apache.spark.sql.functions._

/**
 * Breadth coverage for the `functions` object. Each `select` applies a large batch of functions to
 * typed columns and forces execution with `collect()`, so every function's wiring is validated
 * end-to-end against a real server -- a mis-wired function name fails here rather than silently in
 * production. (Most of these are thin delegations to `UnresolvedFunction`, so a single executed row
 * exercises them.)
 */
class FunctionsBreadthIntegrationSuite extends RemoteSparkSuite {

  private def base: DataFrame =
    spark
      .range(1, 4)
      .select(
        col("id"),
        (col("id") * lit(1.5)).as("d"),
        lit("Hello World").as("str"),
        array(lit(3), lit(1), lit(2)).as("arr"),
        current_date().as("dt"),
        current_timestamp().as("ts")
      )

  test("numeric and math functions execute") {
    withSpark { _ =>
      val d = col("d")
      val id = col("id")
      base
        .select(
          abs(d),
          acos(lit(0.5)),
          acosh(lit(1.5)),
          asin(lit(0.5)),
          asinh(d),
          atan(d),
          atan2(d, d),
          atanh(lit(0.5)),
          cbrt(d),
          ceil(d),
          cos(d),
          cosh(d),
          cot(d),
          csc(d),
          degrees(d),
          exp(d),
          expm1(d),
          factorial(lit(5)),
          floor(d),
          greatest(id, lit(10L)),
          hypot(d, d),
          least(id, lit(0L)),
          ln(d),
          log10(d),
          log1p(d),
          log2(d),
          pmod(id, lit(3L)),
          pow(d, lit(2.0)),
          radians(d),
          rint(d),
          round(d, 1),
          bround(d),
          sec(d),
          signum(d),
          sin(d),
          sinh(d),
          sqrt(d),
          bin(id),
          negative(d),
          positive(d)
        )
        .collect()
    }
  }

  test("string functions execute") {
    withSpark { _ =>
      val s = col("str")
      base
        .select(
          ascii(s),
          base64(encode(s, "UTF-8")),
          bit_length(s),
          btrim(s),
          char_length(s),
          character_length(s),
          concat(s, s),
          concat_ws("-", s, s),
          contains(s, lit("World")),
          endswith(s, lit("d")),
          format_number(col("d"), 2),
          format_string("%s!", s),
          initcap(s),
          instr(s, "o"),
          lcase(s),
          length(s),
          levenshtein(s, lit("hello")),
          locate("o", s),
          lower(s),
          lpad(s, 20, " "),
          ltrim(s),
          octet_length(s),
          regexp_replace(s, "o", "0"),
          repeat(s, 2),
          reverse(s),
          rpad(s, 20, " "),
          rtrim(s),
          soundex(s),
          split(s, " "),
          split_part(s, lit(" "), lit(1)),
          decode(encode(s, "UTF-8"), "UTF-8")
        )
        .collect()
    }
  }

  test("array and collection functions execute") {
    withSpark { _ =>
      val a = col("arr")
      base
        .select(
          array_append(a, lit(9)),
          array_compact(a),
          array_contains(a, lit(1)),
          array_distinct(a),
          array_except(a, a),
          array_intersect(a, a),
          array_join(a, ","),
          array_max(a),
          array_min(a),
          array_position(a, lit(1)),
          array_remove(a, lit(1)),
          array_repeat(lit(1), lit(2)),
          array_sort(a),
          array_union(a, a),
          arrays_overlap(a, a),
          cardinality(a),
          element_at(a, lit(1)),
          flatten(array(a, a)),
          reverse(a),
          shuffle(a),
          size(a),
          slice(a, lit(1), lit(2)),
          sort_array(a)
        )
        .collect()
    }
  }

  test("date and timestamp functions execute") {
    withSpark { _ =>
      val dt = col("dt")
      val ts = col("ts")
      base
        .select(
          add_months(dt, 1),
          current_date(),
          current_timestamp(),
          date_add(dt, 1),
          date_format(ts, "yyyy-MM-dd"),
          date_sub(dt, 1),
          datediff(dt, dt),
          dayofmonth(dt),
          dayofweek(dt),
          dayofyear(dt),
          hour(ts),
          last_day(dt),
          minute(ts),
          month(dt),
          months_between(dt, dt),
          next_day(dt, "Mon"),
          quarter(dt),
          second(ts)
        )
        .collect()
    }
  }

  test("aggregate functions execute") {
    withSpark { _ =>
      base
        .agg(
          avg(col("d")),
          count(lit(1)),
          countDistinct(col("id")),
          first(col("id")),
          kurtosis(col("d")),
          last(col("id")),
          max(col("id")),
          mean(col("d")),
          min(col("id")),
          skewness(col("d")),
          corr(col("d"), col("id")),
          covar_pop(col("d"), col("id")),
          covar_samp(col("d"), col("id"))
        )
        .collect()
    }
  }
}
