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
 * End-to-end Spark Declarative Pipelines tests: exercise the full run path
 * (`createMaterializedView` -> `startRun` -> read the materialized result -> `drop`), not just
 * graph creation. Requires Spark 4.1+, so it self-skips on the 3.5 and 4.0 matrix rows.
 */
class PipelinesIntegrationSuite extends RemoteSparkSuite {

  test("materialized views run end to end and produce the expected rows") {
    whenServerAtLeast(4, 1) {
      withSpark { s =>
        val pipe = s.pipeline()
        pipe.createMaterializedView("sc3_pl_numbers", Some(s.range(0, 50)))
        pipe.createMaterializedView(
          "sc3_pl_evens",
          Some(pipe.read("sc3_pl_numbers").where("id % 2 = 0"))
        )
        try {
          val storage = s"file:///tmp/sc3-pipeline-it-${System.nanoTime()}"
          val events = pipe.startRun(storage = Some(storage))
          assert(
            events.exists(_.message.exists(_.contains("COMPLETED"))),
            s"expected a COMPLETED event, got: ${events.flatMap(_.message).mkString(" | ")}"
          )
          assertEquals(s.read.table("sc3_pl_evens").count(), 25L)
        } finally pipe.drop()
      }
    }
  }
}
