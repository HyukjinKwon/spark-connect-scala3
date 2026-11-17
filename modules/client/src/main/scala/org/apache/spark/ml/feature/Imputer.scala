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
package org.apache.spark.ml.feature

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.connect.{ModelRef, RemoteIO, RemoteModelLike, RemoteOps}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLWritable, MLWriter, Summary}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Params of Imputer. */
trait ImputerParams extends HasInputCols with HasOutputCols with HasRelativeError {

  final val strategy: Param[String] = new Param[String](
    this,
    "strategy",
    "strategy for imputation. If mean, then replace missing values using the mean value of the feature. If median, then replace missing values using the approximate median value of the feature. If mode, then replace missing values using the most frequent value of the feature.",
    ParamValidators.inArray(Array("mean", "median", "mode"))
  )

  final val missingValue: DoubleParam = new DoubleParam(
    this,
    "missingValue",
    "The placeholder for the missing values. All occurrences of missingValue will be imputed."
  )

  setDefault(strategy -> "mean", missingValue -> Double.NaN, relativeError -> 0.001)

  def setStrategy(value: String): this.type = set(strategy, value)
  def getStrategy: String = $(strategy)

  def setMissingValue(value: Double): this.type = set(missingValue, value)
  def getMissingValue: Double = $(missingValue)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setRelativeError(value: Double): this.type = set(relativeError, value)

}

/**
 * Imputation estimator for completing missing values, either using the mean, median or mode of the
 * columns in which the missing values are located. Mirrors `org.apache.spark.ml.feature.Imputer`.
 */
class Imputer(override val uid: String)
    extends Estimator[ImputerModel]
    with ImputerParams
    with MLWritable {

  def this() = this(Identifiable.randomUID("imputer"))

  override def fit(dataset: Dataset[_]): ImputerModel =
    RemoteOps.fit(this, Imputer.OperatorName, dataset, (id, ref) => new ImputerModel(id, ref))

  override def copy(extra: ParamMap): Imputer = copyValues(new Imputer(uid), extra)

  override def write: MLWriter = new MLWriter(this)
}

object Imputer extends MLReadable[Imputer] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.Imputer"

  override def load(path: String): Imputer =
    RemoteIO.loadOperator(
      OperatorName,
      proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR,
      path,
      new Imputer(_)
    )
}

/**
 * The model fit by [[Imputer]], held on the server. Mirrors
 * `org.apache.spark.ml.feature.ImputerModel`.
 */
class ImputerModel private[ml] (override val uid: String, private[ml] val ref: ModelRef)
    extends Model[ImputerModel]
    with ImputerParams
    with RemoteModelLike
    with MLWritable {

  private def attr[T](name: String): T = RemoteOps.fetch(ref, Seq(name)).asInstanceOf[T]

  private def attrWith[T](name: String, args: Any*): T =
    RemoteOps.fetch(ref, Seq(name), args).asInstanceOf[T]

  private def frame(name: String): DataFrame = RemoteOps.fetchRelation(ref, Seq(name))

  private def frameWith(name: String, args: Any*): DataFrame =
    RemoteOps.fetchRelation(ref, Seq(name), args)

  /** Model attribute `surrogateDF`, a DataFrame on the server. */
  def surrogateDF: DataFrame = frame("surrogateDF")

  override def copy(extra: ParamMap): ImputerModel =
    copyValues(new ImputerModel(uid, ref.retain()), extra)

  override def transform(dataset: Dataset[_]): DataFrame =
    RemoteOps.transformWithModel(this, ref, dataset)

  override def release(): Unit = ref.release()

  override def write: MLWriter = new MLWriter(this)
}

object ImputerModel extends MLReadable[ImputerModel] {

  private[ml] val OperatorName: String = "org.apache.spark.ml.feature.ImputerModel"

  override def load(path: String): ImputerModel =
    RemoteIO.loadModel(OperatorName, path, (id, ref) => new ImputerModel(id, ref))
}
