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
 * Classic word count: split lines into words, explode to one row per word, then group and count.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.WordCount"
 * }}}
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("word-count").getOrCreate()
    try {
      val lines = spark.sql(
        "SELECT * FROM VALUES " +
          "('the quick brown fox'), ('the lazy dog'), ('the quick dog') AS t(line)"
      )

      val counts = lines
        .select(explode(split(col("line"), " ")).as("word"))
        .groupBy("word")
        .count()
        .orderBy(col("count").desc, col("word"))

      counts.show()
      // +-----+-----+
      // | word|count|
      // +-----+-----+
      // |  the|    3|
      // |  dog|    2|
      // |quick|    2|
      // |brown|    1|
      // |  fox|    1|
      // | lazy|    1|
      // +-----+-----+
    } finally spark.stop()
  }
}
