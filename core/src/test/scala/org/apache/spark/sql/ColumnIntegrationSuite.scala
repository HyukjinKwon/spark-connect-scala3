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

/** Live coverage for the [[Column]] expression operators. */
class ColumnIntegrationSuite extends RemoteSparkSuite {

  test("arithmetic and comparison operators") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq((10, 3))
        .toDF("a", "b")
        .select(
          ($"a" + $"b").as("add"),
          ($"a" - $"b").as("sub"),
          ($"a" * $"b").as("mul"),
          ($"a" > $"b").as("gt"),
          ($"a" === lit(10)).as("eq"),
          ($"a" =!= $"b").as("ne")
        )
        .collect()
        .head
      assertEquals(r.getInt(0), 13)
      assertEquals(r.getInt(1), 7)
      assertEquals(r.getInt(2), 30)
      assertEquals(r.getBoolean(3), true)
      assertEquals(r.getBoolean(4), true)
      assertEquals(r.getBoolean(5), true)
    }
  }

  test("logical operators and null predicates") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq((1, "x"), (2, null))
        .toDF("id", "v")
        .select(
          $"v".isNull.as("isNull"),
          $"v".isNotNull.as("isNotNull"),
          ($"id" >= 1 && $"id" <= 1).as("between1")
        )
        .orderBy($"id")
        .collect()
      assertEquals(r(0).getBoolean(0), false)
      assertEquals(r(1).getBoolean(0), true)
      assertEquals(r(0).getBoolean(2), true)
    }
  }

  test("string predicates: like, rlike, contains, startsWith, substr") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq("spark-connect")
        .toDF("s")
        .select(
          $"s".like("spark%").as("like"),
          $"s".rlike("conn.ct$").as("rlike"),
          $"s".contains("connect").as("contains"),
          $"s".startsWith("spark").as("starts"),
          $"s".substr(1, 5).as("sub")
        )
        .collect()
        .head
      assertEquals(r.getBoolean(0), true)
      assertEquals(r.getBoolean(1), true)
      assertEquals(r.getBoolean(2), true)
      assertEquals(r.getBoolean(3), true)
      assertEquals(r.getString(4), "spark")
    }
  }

  test("cast, alias, isin") {
    withSpark { s =>
      import s.implicits.*
      val r = Seq("42")
        .toDF("s")
        .select($"s".cast("int").as("n"), lit("b").isin("a", "b", "c").as("inset"))
        .collect()
        .head
      assertEquals(r.getInt(0), 42)
      assertEquals(r.getBoolean(1), true)
    }
  }
}
