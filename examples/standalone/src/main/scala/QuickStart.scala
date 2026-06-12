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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

/**
 * A minimal, self-contained Spark Connect application.
 *
 * Start a Spark Connect server, then run:
 * {{{
 *   SPARK_REMOTE=sc://localhost:15002 sbt run
 * }}}
 */
@main def quickStart(): Unit =
  val remote = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")

  val spark = SparkSession.builder
    .remote(remote)
    .appName("spark-connect-scala3-standalone")
    .getOrCreate()

  try
    println(s"Connected to Apache Spark ${spark.version} at $remote")

    // Build a lazy plan and run it on the server; results stream back as Arrow.
    val byBucket = spark
      .range(1, 1000)
      .select(col("id"), (col("id") % 3).as("bucket"))
      .groupBy("bucket")
      .agg(count("*").as("n"), sum("id").as("total"))
      .orderBy("bucket")

    byBucket.show()

    val total = byBucket.agg(sum("total").as("grand_total")).collect().head.getLong(0)
    println(s"grand total = $total")
  finally spark.stop()
