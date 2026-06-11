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
 * Join two DataFrames on a shared key, then aggregate the joined result.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.JoinExample"
 * }}}
 */
object JoinExample {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("join-example").getOrCreate()
    try {
      val orders = spark.sql(
        "SELECT * FROM VALUES " +
          "(1, 'KR', 120.0), (2, 'US', 80.0), (3, 'KR', 50.0), (4, 'JP', 30.0) " +
          "AS t(id, country, amount)"
      )

      val regions = spark.sql(
        "SELECT * FROM VALUES ('KR', 'Asia'), ('US', 'Americas'), ('JP', 'Asia') " +
          "AS t(country, region)"
      )

      // Inner join on the shared `country` column, then total by region.
      orders
        .join(regions, "country")
        .groupBy("region")
        .agg(round(sum("amount"), 2).as("total"), count("*").as("orders"))
        .orderBy(col("total").desc)
        .show()

      // A left join keeps every order even when no region matches.
      val partialRegions = spark.sql("SELECT * FROM VALUES ('KR', 'Asia') AS t(country, region)")
      orders
        .join(partialRegions, orders.col("country") === partialRegions.col("country"), "left")
        .select(orders.col("id"), orders.col("country"), col("region"))
        .orderBy("id")
        .show()
    } finally spark.stop()
  }
}
