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
package org.apache.spark.ml.stat

import org.apache.spark.ml.connect.RemoteOps
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Pearson and Spearman correlation matrices over a vector column. Mirrors
 * `org.apache.spark.ml.stat.Correlation`; the computation runs on the server.
 */
object Correlation {

  /** The Pearson correlation matrix of the vector column `column`. */
  def corr(dataset: Dataset[_], column: String): DataFrame = corr(dataset, column, "pearson")

  /** The correlation matrix of `column` by `method` (`pearson` or `spearman`). */
  def corr(dataset: Dataset[_], column: String, method: String): DataFrame =
    RemoteOps.fetchRelation(
      RemoteOps.helperRef(dataset.sparkSession),
      Seq("correlation"),
      Seq(dataset, column, method)
    )
}
