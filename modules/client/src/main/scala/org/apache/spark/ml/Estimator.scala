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

import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.Dataset

/**
 * An algorithm that fits a [[Model]] on a dataset. Fitting runs on the Spark Connect server; the
 * returned model refers to the fitted model cached there. Mirrors `org.apache.spark.ml.Estimator`.
 */
abstract class Estimator[M <: Model[M]] extends PipelineStage {

  /** Fits a model to `dataset` with the parameters of this estimator. */
  def fit(dataset: Dataset[_]): M

  /** Fits a model to `dataset` with `paramMap` applied on top of this estimator's parameters. */
  def fit(dataset: Dataset[_], paramMap: ParamMap): M =
    copy(paramMap).asInstanceOf[Estimator[M]].fit(dataset)

  /** Fits one model per parameter map. */
  def fit(dataset: Dataset[_], paramMaps: Seq[ParamMap]): Seq[M] =
    paramMaps.map(fit(dataset, _))

  /** Fits a model to `dataset` with the given parameter pairs applied. */
  def fit(dataset: Dataset[_], firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]*): M =
    fit(dataset, ParamMap(firstParamPair +: otherParamPairs*))

  override def copy(extra: ParamMap): Estimator[M]
}
