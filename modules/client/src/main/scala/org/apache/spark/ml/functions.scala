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

import org.apache.spark.sql.{Column, MlFunctionBridge}
import org.apache.spark.sql.{functions => F}

/**
 * SQL functions that convert between vector columns and array columns. Mirrors
 * `org.apache.spark.ml.functions`; the calls run on the server.
 */
object functions {

  /**
   * Converts a vector column to an array column of `dtype` (`float64` by default, or `float32`).
   */
  def vector_to_array(col: Column, dtype: String = "float64"): Column =
    MlFunctionBridge.internal("vector_to_array", col, F.lit(dtype))

  /** Converts an array column of numbers to a vector column. */
  def array_to_vector(col: Column): Column = MlFunctionBridge.internal("array_to_vector", col)
}
