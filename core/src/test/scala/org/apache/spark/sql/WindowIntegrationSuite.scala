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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

/** Live coverage for window functions. */
class WindowIntegrationSuite extends RemoteSparkSuite {

  test("row_number, rank, and running sum over a window") {
    withSpark { s =>
      import s.implicits.*
      val df = Seq(("a", 10), ("a", 30), ("a", 20), ("b", 5))
        .toDF("grp", "v")
      val w = Window.partitionBy($"grp").orderBy($"v".desc)
      val rows = df
        .select(
          $"grp",
          $"v",
          row_number().over(w).as("rn"),
          rank().over(w).as("rk"),
          sum($"v").over(w).as("running")
        )
        .orderBy($"grp", $"rn")
        .collect()

      // Group a, ordered by v desc: 30, 20, 10
      assertEquals(rows(0).getString(0), "a")
      assertEquals(rows(0).getInt(1), 30)
      assertEquals(rows(0).getInt(2), 1)
      assertEquals(rows(1).getInt(1), 20)
      assertEquals(rows(1).getInt(2), 2)
      // Running sum within the partition (cumulative): 30, 50, 60
      assertEquals(rows(0).getLong(4), 30L)
      assertEquals(rows(1).getLong(4), 50L)
      assertEquals(rows(2).getLong(4), 60L)
    }
  }

  test("lag and lead") {
    withSpark { s =>
      import s.implicits.*
      val w = Window.orderBy($"id")
      val rows = s
        .range(0, 3)
        .select($"id", lag($"id", 1).over(w).as("prev"), lead($"id", 1).over(w).as("next"))
        .orderBy($"id")
        .collect()
      assert(rows(0).isNullAt(1))
      assertEquals(rows(1).getLong(1), 0L)
      assertEquals(rows(1).getLong(2), 2L)
      assert(rows(2).isNullAt(2))
    }
  }
}
