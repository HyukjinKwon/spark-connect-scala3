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
package evaluation

import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.Dataset

/**
 * Computes a metric over a dataset of predictions. Evaluation runs on the server. Mirrors
 * `org.apache.spark.ml.evaluation.Evaluator`.
 */
abstract class Evaluator extends PipelineStage {

  /** Evaluates the metric of `dataset`. */
  def evaluate(dataset: Dataset[_]): Double

  /** Evaluates with `paramMap` applied on top of this evaluator's parameters. */
  def evaluate(dataset: Dataset[_], paramMap: ParamMap): Double =
    copy(paramMap).asInstanceOf[Evaluator].evaluate(dataset)

  /** Evaluates with the given parameter pairs applied. */
  def evaluate(
      dataset: Dataset[_],
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*
  ): Double =
    evaluate(dataset, ParamMap(firstParamPair +: otherParamPairs*))

  /** Whether a larger value of the metric is better (used by tuning). */
  def isLargerBetter: Boolean = true

  override def copy(extra: ParamMap): Evaluator
}
