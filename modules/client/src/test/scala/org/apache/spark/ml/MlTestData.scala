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
package org.apache.spark.ml

import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, lit}

/** Small, deterministic datasets for the ML integration suites, built on the server. */
object MlTestData {

  /**
   * Two classes separated by `x`: label 0 for x < 20, label 1 otherwise. Columns: label, features.
   */
  def binary(spark: SparkSession): DataFrame =
    spark
      .sql(
        "SELECT CAST(IF(id < 20, 0, 1) AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x, " +
          "CAST(id * id % 7 AS DOUBLE) AS y FROM range(40)"
      )
      .select(col("label"), array_to_vector(array(col("x"), col("y"))).as("features"))

  /** Multiclass data with three classes (`id % 3`). Columns: label, features. */
  def multiclass(spark: SparkSession): DataFrame =
    spark
      .sql(
        "SELECT CAST(id % 3 AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x, " +
          "CAST((id * 7) % 5 AS DOUBLE) AS y FROM range(45)"
      )
      .select(col("label"), array_to_vector(array(col("x"), col("y"))).as("features"))

  /** Noise-free linear data: label = 2x + 1. Columns: label, x, features. */
  def linear(spark: SparkSession): DataFrame =
    spark
      .sql(
        "SELECT CAST(2 * id + 1 AS DOUBLE) AS label, CAST(id AS DOUBLE) AS x, " +
          "CAST(id % 5 AS DOUBLE) AS z FROM range(30)"
      )
      .select(col("label"), col("x"), array_to_vector(array(col("x"), col("z"))).as("features"))

  /** Two well separated blobs around 0 and 100. Columns: features. */
  def blobs(spark: SparkSession): DataFrame =
    spark
      .sql(
        "SELECT CAST(IF(id < 12, id % 4, 100 + id % 4) AS DOUBLE) AS x, " +
          "CAST(id % 3 AS DOUBLE) AS y FROM range(24)"
      )
      .select(array_to_vector(array(col("x"), col("y"))).as("features"))

  /** Users, items and ratings for ALS. */
  def ratings(spark: SparkSession): DataFrame =
    spark.sql(
      "SELECT CAST(id % 6 AS INT) AS user, CAST((id * 3) % 5 AS INT) AS item, " +
        "CAST((id % 7) + 1 AS FLOAT) AS rating FROM range(60)"
    )

  /** Transactions for FP-growth. Columns: items. */
  def transactions(spark: SparkSession): DataFrame =
    spark
      .sql("SELECT id AS txn FROM range(12)")
      .select(array(lit("bread"), lit("milk")).as("items"))
      .union(spark.sql("SELECT 1").select(array(lit("bread"), lit("eggs")).as("items")))
      .union(spark.sql("SELECT 1").select(array(lit("milk"), lit("eggs")).as("items")))

  /** Text documents for the text transformers. Columns: id, text. */
  def documents(spark: SparkSession): DataFrame =
    spark.sql(
      "SELECT CAST(id AS INT) AS id, CONCAT('Spark connect ML word', CAST(id % 3 AS STRING)) AS text " +
        "FROM range(6)"
    )
}
