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
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * A stage that transforms a dataset. Transformation is lazy: it builds a relation that the server
 * evaluates. Mirrors `org.apache.spark.ml.Transformer`.
 */
abstract class Transformer extends PipelineStage {

  /** Transforms `dataset`. */
  def transform(dataset: Dataset[_]): DataFrame

  /** Transforms `dataset` with `paramMap` applied on top of this transformer's parameters. */
  def transform(dataset: Dataset[_], paramMap: ParamMap): DataFrame =
    copy(paramMap).asInstanceOf[Transformer].transform(dataset)

  /** Transforms `dataset` with the given parameter pairs applied. */
  def transform(
      dataset: Dataset[_],
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*
  ): DataFrame =
    transform(dataset, ParamMap(firstParamPair +: otherParamPairs*))

  override def copy(extra: ParamMap): Transformer
}
