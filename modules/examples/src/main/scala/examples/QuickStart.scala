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
 * The smallest end-to-end program: connect, build a range, project a derived column, and run a
 * couple of actions.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.QuickStart"
 * }}}
 *
 * Pass a connection string as the first argument to target a different server; otherwise the
 * `SPARK_REMOTE` environment variable is used, defaulting to a local server.
 */
object QuickStart {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("quickstart").getOrCreate()
    try {
      val df = spark.range(1, 6).select(col("id"), (col("id") * col("id")).as("square"))

      df.show()
      // +---+------+
      // | id|square|
      // +---+------+
      // |  1|     1|
      // |  2|     4|
      // |  3|     9|
      // |  4|    16|
      // |  5|    25|
      // +---+------+

      println(s"row count = ${df.count()}")
    } finally spark.stop()
  }
}
