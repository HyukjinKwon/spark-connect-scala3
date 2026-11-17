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

/**
 * A [[Transformer]] produced by an [[Estimator]]. Remote models hold a reference to the fitted
 * model cached on the server, which is evicted when the last reference is released or the session
 * closes. Mirrors `org.apache.spark.ml.Model`.
 */
abstract class Model[M <: Model[M]] extends Transformer {

  override def copy(extra: org.apache.spark.ml.param.ParamMap): M

  /**
   * Releases the server-side cache entry of this model once no copy of it still needs it. Models
   * that are not held on the server (for example a [[PipelineModel]] over local stages) have
   * nothing to release, so the default is a no-op.
   */
  def release(): Unit = ()
}
