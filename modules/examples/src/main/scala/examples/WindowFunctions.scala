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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Window functions: per-department salary ranking and running totals.
 *
 * Run it with:
 * {{{
 * sbt "examples/runMain examples.WindowFunctions"
 * }}}
 */
object WindowFunctions {

  def main(args: Array[String]): Unit = {
    val remote =
      if (args.nonEmpty) args(0) else sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")
    val spark = SparkSession.builder.remote(remote).appName("window-functions").getOrCreate()
    try {
      val employees = spark.sql(
        "SELECT * FROM VALUES " +
          "('eng', 'Ada', 120), ('eng', 'Alan', 110), ('eng', 'Grace', 130), " +
          "('sales', 'Tom', 90), ('sales', 'Sue', 95), ('sales', 'Ravi', 95) " +
          "AS t(dept, name, salary)"
      )

      val byDeptSalary = Window.partitionBy(col("dept")).orderBy(col("salary").desc)

      employees
        .select(
          col("dept"),
          col("name"),
          col("salary"),
          rank().over(byDeptSalary).as("rank"),
          dense_rank().over(byDeptSalary).as("dense_rank"),
          sum(col("salary")).over(byDeptSalary).as("running_total")
        )
        .orderBy(col("dept"), col("rank"))
        .show()
    } finally spark.stop()
  }
}
