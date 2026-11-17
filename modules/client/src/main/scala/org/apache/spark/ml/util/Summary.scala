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
package org.apache.spark.ml.util

import org.apache.spark.ml.connect.{ModelRef, RemoteOps}
import org.apache.spark.sql.DataFrame

/**
 * A summary of a fitted model, such as the training summary of a
 * [[org.apache.spark.ml.classification.LogisticRegressionModel]]. Summaries are computed on the
 * server, and each accessor is a request for one attribute of the cached summary.
 */
abstract class Summary private[ml] (
    private[ml] val ref: ModelRef,
    private[ml] val path: Seq[String]
) {

  /** Reads a scalar or array attribute of this summary. */
  protected final def attr[T](name: String): T =
    RemoteOps.fetch(ref, path :+ name).asInstanceOf[T]

  /** Reads an attribute of this summary that takes an argument. */
  protected final def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, path :+ name, args).asInstanceOf[T]

  /** Reads a DataFrame attribute of this summary. */
  protected final def frame(name: String): DataFrame =
    RemoteOps.fetchRelation(ref, path :+ name)
}
