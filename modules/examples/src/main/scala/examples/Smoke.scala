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

/** Smoke test run against a live Spark Connect server. */
object Smoke {
  def main(args: Array[String]): Unit = {
    val remote = if (args.nonEmpty) args(0) else "sc://localhost:15099"
    val spark = SparkSession.builder.remote(remote).getOrCreate()
    try {
      println("== version ==")
      println(spark.version)

      println("== range/filter/collect ==")
      val rows = spark.range(10).filter(col("id") % 2 === 0).collect()
      println(rows.map(_.getLong(0)).mkString(", "))

      println("== show ==")
      spark.range(5).select(col("id"), (col("id") * 2).as("doubled")).show()

      println("== sql + groupBy/agg ==")
      spark
        .range(1, 10)
        .select(col("id"), (col("id") % 3).as("bucket"))
        .groupBy("bucket")
        .agg(count(lit(1)).as("n"), sum("id").as("total"))
        .orderBy("bucket")
        .show()

      println("== count ==")
      println(spark.range(100).count())

      println("== sql ==")
      spark.sql("select 1 as a, 'hello' as b").show()
    } finally spark.stop()
  }
}
