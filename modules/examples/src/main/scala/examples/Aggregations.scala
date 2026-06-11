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
 * Grouped aggregation with several aggregate functions over synthetic sales data.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.Aggregations"
 * }}}
 */
object Aggregations {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("aggregations").getOrCreate()
    try {
      val sales = spark.sql(
        "SELECT * FROM VALUES " +
          "('KR', 'book', 12.0), ('KR', 'pen', 3.5), ('KR', 'book', 9.0), " +
          "('US', 'book', 15.0), ('US', 'pen', 2.0), ('US', 'pen', 2.5) " +
          "AS t(country, item, amount)"
      )

      // One row per country with a handful of aggregates.
      sales
        .groupBy("country")
        .agg(
          count("*").as("orders"),
          round(sum("amount"), 2).as("total"),
          round(avg("amount"), 2).as("avg"),
          max("amount").as("max")
        )
        .orderBy("country")
        .show()

      // Conditional aggregation: total spent on books per country.
      sales
        .groupBy("country")
        .agg(
          round(sum(when(col("item") === "book", col("amount")).otherwise(0.0)), 2)
            .as("book_total")
        )
        .orderBy("country")
        .show()
    } finally spark.stop()
  }
}
