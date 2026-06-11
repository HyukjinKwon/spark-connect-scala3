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

/** Live coverage for a representative function from each major `functions` category. */
class FunctionsIntegrationSuite extends RemoteSparkSuite {

  test("string functions: upper, lower, length, concat_ws, substring, trim") {
    withSpark { s =>
      import s.implicits.*
      val row = Seq((" Spark ", "connect"))
        .toDF("a", "b")
        .select(
          upper(trim($"a")).as("u"),
          lower($"b").as("l"),
          length($"b").as("len"),
          concat_ws("-", trim($"a"), $"b").as("c"),
          substring($"b", 1, 3).as("sub")
        )
        .collect()
        .head
      assertEquals(row.getString(0), "SPARK")
      assertEquals(row.getString(1), "connect")
      assertEquals(row.getInt(2), 7)
      assertEquals(row.getString(3), "Spark-connect")
      assertEquals(row.getString(4), "con")
    }
  }

  test("math functions: round, abs, sqrt, pow") {
    withSpark { s =>
      import s.implicits.*
      val row = Seq(2.0)
        .toDF("x")
        .select(
          round(lit(3.14159), 2).as("r"),
          abs(lit(-5)).as("a"),
          sqrt($"x").as("sq"),
          pow($"x", lit(10)).as("p")
        )
        .collect()
        .head
      assertEquals(row.getDouble(0), 3.14)
      assertEquals(row.getInt(1), 5)
      assertEquals(row.getDouble(2), math.sqrt(2.0))
      assertEquals(row.getDouble(3), 1024.0)
    }
  }

  test("conditional functions: when/otherwise, coalesce") {
    withSpark { s =>
      val rows = s
        .range(0, 4)
        .select(
          when(col("id") % 2 === 0, lit("even")).otherwise(lit("odd")).as("parity"),
          coalesce(lit(null), col("id")).as("c")
        )
        .collect()
      assertEquals(rows.map(_.getString(0)).toSeq, Seq("even", "odd", "even", "odd"))
      assertEquals(rows.map(_.getLong(1)).toSeq, Seq(0L, 1L, 2L, 3L))
    }
  }

  test("collection functions: array, array_contains, size, explode") {
    withSpark { s =>
      import s.implicits.*
      val df = Seq(1).toDF("x").select(array(lit(1), lit(2), lit(3)).as("arr"))
      val agg =
        df.select(array_contains(col("arr"), 2).as("has2"), size(col("arr")).as("n")).collect().head
      assertEquals(agg.getBoolean(0), true)
      assertEquals(agg.getInt(1), 3)
      val exploded = df.select(explode(col("arr")).as("e")).collect().map(_.getInt(0)).toSeq
      assertEquals(exploded, Seq(1, 2, 3))
    }
  }

  test("aggregate functions: sum, avg, min, max, count, countDistinct") {
    withSpark { s =>
      val r = s
        .range(0, 10)
        .agg(
          sum(col("id")).as("sum"),
          avg(col("id")).as("avg"),
          min(col("id")).as("min"),
          max(col("id")).as("max"),
          count(lit(1)).as("cnt"),
          countDistinct(col("id")).as("distinct")
        )
        .collect()
        .head
      assertEquals(r.getLong(0), 45L)
      assertEquals(r.getDouble(1), 4.5)
      assertEquals(r.getLong(2), 0L)
      assertEquals(r.getLong(3), 9L)
      assertEquals(r.getLong(4), 10L)
      assertEquals(r.getLong(5), 10L)
    }
  }

  test("datetime functions: to_date, year, date_add") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq("2021-03-15")
        .toDF("d")
        .select(year(to_date($"d")).as("y"), date_add(to_date($"d"), 10).as("plus10"))
        .collect()
        .head
      assertEquals(r.getInt(0), 2021)
      assertEquals(r.getDate(1).toString, "2021-03-25")
    }
  }
}
