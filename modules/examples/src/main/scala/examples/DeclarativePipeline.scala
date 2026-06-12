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

package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Spark Declarative Pipelines (SDP): build a small dataflow graph of a source materialized view and
 * a derived one, then run it. Requires an Apache Spark 4.1 or newer server.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.DeclarativePipeline"
 * }}}
 */
object DeclarativePipeline {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    // The server requires an absolute URI for pipeline storage (checkpoint/metadata).
    val storage = if (args.length > 1) args(1) else "file:///tmp/sc3-pipeline-storage"
    val spark = SparkSession.builder.remote(remote).appName("declarative-pipeline").getOrCreate()
    try {
      // The pipeline materializes managed tables; drop any from a previous run so the example
      // is idempotent (managed-table locations cannot be recreated while they still exist).
      spark.sql("DROP TABLE IF EXISTS evens")
      spark.sql("DROP TABLE IF EXISTS numbers")

      val pipe = spark.pipeline()
      pipe.createMaterializedView("numbers", Some(spark.range(0, 100)))
      pipe.createMaterializedView("evens", Some(pipe.read("numbers").where(col("id") % 2 === 0)))

      val events = pipe.startRun(storage = Some(storage))
      events.foreach(e => println(s"[pipeline] ${e.message.getOrElse("")}"))

      spark.read.table("evens").orderBy("id").show(5)
    } finally spark.stop()
  }
}
