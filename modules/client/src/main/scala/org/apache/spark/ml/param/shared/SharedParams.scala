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
package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param._

// Shared parameter traits, mirroring org.apache.spark.ml.param.shared.sharedParams. Each trait
// declares one parameter and its getter. Defaults that Spark defines in these traits are kept.

trait HasRegParam extends Params {

  final val regParam: DoubleParam =
    new DoubleParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0.0))

  final def getRegParam: Double = $(regParam)
}

trait HasMaxIter extends Params {

  final val maxIter: IntParam =
    new IntParam(this, "maxIter", "maximum number of iterations (>= 0)", ParamValidators.gtEq(0))

  final def getMaxIter: Int = $(maxIter)
}

trait HasFeaturesCol extends Params {

  final val featuresCol: Param[String] =
    new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  final def getFeaturesCol: String = $(featuresCol)
}

trait HasLabelCol extends Params {

  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")

  setDefault(labelCol, "label")

  final def getLabelCol: String = $(labelCol)
}

trait HasPredictionCol extends Params {

  final val predictionCol: Param[String] =
    new Param[String](this, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  final def getPredictionCol: String = $(predictionCol)
}

trait HasRawPredictionCol extends Params {

  final val rawPredictionCol: Param[String] =
    new Param[String](this, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")

  setDefault(rawPredictionCol, "rawPrediction")

  final def getRawPredictionCol: String = $(rawPredictionCol)
}

trait HasProbabilityCol extends Params {

  final val probabilityCol: Param[String] = new Param[String](
    this,
    "probabilityCol",
    "Column name for predicted class conditional probabilities. Note: Not all models output " +
      "well-calibrated probability estimates! These probabilities should be treated as " +
      "confidences, not precise probabilities"
  )

  setDefault(probabilityCol, "probability")

  final def getProbabilityCol: String = $(probabilityCol)
}

trait HasVarianceCol extends Params {

  final val varianceCol: Param[String] =
    new Param[String](
      this,
      "varianceCol",
      "Column name for the biased sample variance of prediction"
    )

  final def getVarianceCol: String = $(varianceCol)
}

trait HasThreshold extends Params {

  final val threshold: DoubleParam = new DoubleParam(
    this,
    "threshold",
    "threshold in binary classification prediction, in range [0, 1]",
    ParamValidators.inRange(0.0, 1.0)
  )

  final def getThreshold: Double = $(threshold)
}

trait HasThresholds extends Params {

  final val thresholds: DoubleArrayParam = new DoubleArrayParam(
    this,
    "thresholds",
    "Thresholds in multi-class classification to adjust the probability of predicting each " +
      "class. Array must have length equal to the number of classes, with values > 0 excepting " +
      "that at most one value may be 0. The class with largest value p/t is predicted, where p " +
      "is the original probability of that class and t is the class's threshold",
    (t: Array[Double]) => t.forall(_ >= 0) && t.count(_ == 0) <= 1
  )

  final def getThresholds: Array[Double] = $(thresholds)
}

trait HasInputCol extends Params {

  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)
}

trait HasInputCols extends Params {

  final val inputCols: StringArrayParam =
    new StringArrayParam(this, "inputCols", "input column names")

  final def getInputCols: Array[String] = $(inputCols)
}

trait HasOutputCol extends Params {

  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  final def getOutputCol: String = $(outputCol)
}

trait HasOutputCols extends Params {

  final val outputCols: StringArrayParam =
    new StringArrayParam(this, "outputCols", "output column names")

  final def getOutputCols: Array[String] = $(outputCols)
}

trait HasNumFeatures extends Params {

  final val numFeatures: IntParam = new IntParam(
    this,
    "numFeatures",
    "Number of features. Should be greater than 0",
    ParamValidators.gt(0)
  )

  setDefault(numFeatures, 262144)

  final def getNumFeatures: Int = $(numFeatures)
}

trait HasCheckpointInterval extends Params {

  final val checkpointInterval: IntParam = new IntParam(
    this,
    "checkpointInterval",
    "set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache " +
      "will get checkpointed every 10 iterations. Note: this setting will be ignored if the " +
      "checkpoint directory is not set in the SparkContext",
    (interval: Int) => interval == -1 || interval >= 1
  )

  final def getCheckpointInterval: Int = $(checkpointInterval)
}

trait HasFitIntercept extends Params {

  final val fitIntercept: BooleanParam =
    new BooleanParam(this, "fitIntercept", "whether to fit an intercept term")

  setDefault(fitIntercept, true)

  final def getFitIntercept: Boolean = $(fitIntercept)
}

trait HasHandleInvalid extends Params {

  final val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "how to handle invalid entries. Options are skip (which will filter out rows with bad " +
      "values), or error (which will throw an error). More options may be added later",
    ParamValidators.inArray(Array("skip", "error"))
  )

  final def getHandleInvalid: String = $(handleInvalid)
}

trait HasStandardization extends Params {

  final val standardization: BooleanParam = new BooleanParam(
    this,
    "standardization",
    "whether to standardize the training features before fitting the model"
  )

  setDefault(standardization, true)

  final def getStandardization: Boolean = $(standardization)
}

trait HasSeed extends Params {

  final val seed: LongParam = new LongParam(this, "seed", "random seed")

  setDefault(seed, this.getClass.getName.hashCode.toLong)

  final def getSeed: Long = $(seed)
}

trait HasElasticNetParam extends Params {

  final val elasticNetParam: DoubleParam = new DoubleParam(
    this,
    "elasticNetParam",
    "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 " +
      "penalty. For alpha = 1, it is an L1 penalty",
    ParamValidators.inRange(0.0, 1.0)
  )

  final def getElasticNetParam: Double = $(elasticNetParam)
}

trait HasTol extends Params {

  final val tol: DoubleParam = new DoubleParam(
    this,
    "tol",
    "the convergence tolerance for iterative algorithms (>= 0)",
    ParamValidators.gtEq(0.0)
  )

  final def getTol: Double = $(tol)
}

trait HasRelativeError extends Params {

  final val relativeError: DoubleParam = new DoubleParam(
    this,
    "relativeError",
    "the relative target precision for the approximate quantile algorithm. Must be in the range " +
      "[0, 1]",
    ParamValidators.inRange(0.0, 1.0)
  )

  final def getRelativeError: Double = $(relativeError)
}

trait HasStepSize extends Params {

  final val stepSize: DoubleParam = new DoubleParam(
    this,
    "stepSize",
    "Step size to be used for each iteration of optimization (> 0)",
    ParamValidators.gt(0.0)
  )

  final def getStepSize: Double = $(stepSize)
}

trait HasWeightCol extends Params {

  final val weightCol: Param[String] = new Param[String](
    this,
    "weightCol",
    "weight column name. If this is not set or empty, we treat all instance weights as 1.0"
  )

  final def getWeightCol: String = $(weightCol)
}

trait HasSolver extends Params {

  final val solver: Param[String] =
    new Param[String](this, "solver", "the solver algorithm for optimization")

  final def getSolver: String = $(solver)
}

trait HasAggregationDepth extends Params {

  final val aggregationDepth: IntParam = new IntParam(
    this,
    "aggregationDepth",
    "suggested depth for treeAggregate (>= 2)",
    ParamValidators.gtEq(2)
  )

  setDefault(aggregationDepth, 2)

  final def getAggregationDepth: Int = $(aggregationDepth)
}

trait HasCollectSubModels extends Params {

  final val collectSubModels: BooleanParam = new BooleanParam(
    this,
    "collectSubModels",
    "whether to collect a list of sub-models trained during tuning. If set to false, then only " +
      "the single best sub-model will be available after fitting. If set to true, then all " +
      "sub-models will be collected and can be retrieved by the model's sub-model accessor"
  )

  setDefault(collectSubModels, false)

  final def getCollectSubModels: Boolean = $(collectSubModels)
}

trait HasLoss extends Params {

  final val loss: Param[String] =
    new Param[String](this, "loss", "the loss function to be optimized")

  final def getLoss: String = $(loss)
}

trait HasDistanceMeasure extends Params {

  final val distanceMeasure: Param[String] = new Param[String](
    this,
    "distanceMeasure",
    "The distance measure. Supported options: 'euclidean' and 'cosine'",
    ParamValidators.inArray(Array("euclidean", "cosine"))
  )

  setDefault(distanceMeasure, "euclidean")

  final def getDistanceMeasure: String = $(distanceMeasure)
}

trait HasValidationIndicatorCol extends Params {

  final val validationIndicatorCol: Param[String] = new Param[String](
    this,
    "validationIndicatorCol",
    "name of the column that indicates whether each row is for training or for validation. " +
      "False indicates training; true indicates validation."
  )

  final def getValidationIndicatorCol: String = $(validationIndicatorCol)
}

trait HasMaxBlockSizeInMB extends Params {

  final val maxBlockSizeInMB: DoubleParam = new DoubleParam(
    this,
    "maxBlockSizeInMB",
    "Maximum memory in MB for stacking input data into blocks. Data is stacked within " +
      "partitions. If more than remaining data size in a partition then it is adjusted to the " +
      "data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. " +
      "Must be >= 0.",
    ParamValidators.gtEq(0.0)
  )

  setDefault(maxBlockSizeInMB, 0.0)

  final def getMaxBlockSizeInMB: Double = $(maxBlockSizeInMB)
}
