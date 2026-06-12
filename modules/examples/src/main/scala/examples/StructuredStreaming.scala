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
import org.apache.spark.sql.streaming.Trigger

/**
 * Structured Streaming: read the built-in `rate` source, transform it, and write to the in-memory
 * sink, then query the sink as a table.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.StructuredStreaming"
 * }}}
 */
object StructuredStreaming {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("structured-streaming").getOrCreate()
    try {
      val rates = spark.readStream
        .format("rate")
        .option("rowsPerSecond", 10)
        .load()

      val query = rates
        .selectExpr("value", "value % 5 AS bucket")
        .writeStream
        .format("memory")
        .queryName("rates")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      // The rate source emits rows as wall-clock time passes, so give it a moment to produce
      // some before draining: processAllAvailable() commits whatever is available right now.
      Thread.sleep(2000)
      query.processAllAvailable()
      spark
        .sql("SELECT bucket, count(*) AS n FROM rates GROUP BY bucket ORDER BY bucket")
        .show()

      query.stop()
    } finally spark.stop()
  }
}
