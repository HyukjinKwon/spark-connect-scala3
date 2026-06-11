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

/** Live coverage for the newer reader and join APIs. */
class NewApisIntegrationSuite extends RemoteSparkSuite {

  test("read.csv(Dataset[String]) parses in-memory CSV records") {
    withSpark { s =>
      val lines = s.createDataset(Seq("1,a", "2,b", "3,c"))
      val df = s.read.option("header", "false").csv(lines)
      assertEquals(df.count(), 3L)
    }
  }

  test("read.json(Dataset[String]) parses in-memory JSON records") {
    withSpark { s =>
      val lines = s.createDataset(Seq("""{"id":1,"name":"a"}""", """{"id":2,"name":"b"}"""))
      val df = s.read.json(lines)
      assertEquals(df.count(), 2L)
      assert(df.columns.toSeq.contains("name"))
    }
  }

  test("lateralJoin (Spark 4.0+)") {
    withSpark { s =>
      whenServerAtLeast(4, 0) {
        // An uncorrelated lateral join behaves like a cross join: 3 x 2 = 6 rows.
        val rows = s.range(0, 3).lateralJoin(s.range(0, 2)).count()
        assertEquals(rows, 6L)
      }
    }
  }
}
