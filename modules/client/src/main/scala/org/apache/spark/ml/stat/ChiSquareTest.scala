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
 * Chi-squared independence test between categorical features and a label. Mirrors
 * `org.apache.spark.ml.stat.ChiSquareTest`; the computation runs on the server.
 */
object ChiSquareTest {

  /** Runs the test, returning one row per feature with its statistic and p-value. */
  def test(dataset: Dataset[_], featuresCol: String, labelCol: String): DataFrame =
    test(dataset, featuresCol, labelCol, flatten = false)

  /**
   * Runs the test. With `flatten`, returns one row per feature (`featureIndex`, `pValue`,
   * `degreesOfFreedom`, `statistic`); otherwise returns a single row with array columns.
   */
  def test(
      dataset: Dataset[_],
      featuresCol: String,
      labelCol: String,
      flatten: Boolean
  ): DataFrame =
    RemoteOps.fetchRelation(
      RemoteOps.helperRef(dataset.sparkSession),
      Seq("chiSquareTest"),
      Seq(dataset, featuresCol, labelCol, flatten)
    )
}
