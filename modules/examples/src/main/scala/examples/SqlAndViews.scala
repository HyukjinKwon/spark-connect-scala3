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
 * Mixing the DataFrame API with raw SQL through temporary views, including parameterised SQL.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.SqlAndViews"
 * }}}
 */
object SqlAndViews {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("sql-and-views").getOrCreate()
    try {
      // Plain SQL returns a DataFrame.
      spark.sql("SELECT id, id * 2 AS doubled FROM range(5)").show()

      // Register a DataFrame as a temp view, then query it with SQL.
      val nums = spark.range(0, 20).withColumn("bucket", col("id") % 4)
      nums.createOrReplaceTempView("nums")
      spark.sql("SELECT bucket, count(*) AS n FROM nums GROUP BY bucket ORDER BY bucket").show()

      // Parameterised SQL keeps user input out of the query string.
      spark.sql("SELECT * FROM nums WHERE bucket = :b", Map("b" -> 2)).show()
    } finally spark.stop()
  }
}
