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
package org.apache.spark.ml.connect

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.ml.MlClient

/**
 * A handle to a model cached on the server. Copies of a model share one handle; the cached model is
 * deleted when the last copy releases it.
 *
 * @param id
 *   the server-side object id
 * @param session
 *   the session that owns the cached model
 * @param summaryDataset
 *   the training dataset, sent back to the server if the training summary is lost
 */
private[ml] final class ModelRef(
    val id: String,
    val session: SparkSession,
    val summaryDataset: Option[proto.Relation]
) {

  private val refCount = new AtomicInteger(1)

  /** Registers another owner of this handle. */
  def retain(): ModelRef = {
    refCount.incrementAndGet()
    this
  }

  /** Drops one owner, deleting the server-side model when none remain. */
  def release(): Unit =
    if (refCount.decrementAndGet() == 0) {
      MlClient.delete(session, Seq(id))
    }
}

/** Helpers shared by the remote ML classes; they turn Scala calls into ML Connect commands. */
private[ml] object RemoteOps {

  /** The server-side class name of an estimator or transformer. */
  def operator(
      name: String,
      uid: String,
      operatorType: proto.MlOperator.OperatorType
  ): proto.MlOperator =
    proto.MlOperator(name = name, uid = uid, `type` = operatorType)

  /** Fits `estimator` on `dataset` and builds the model with `build`. */
  def fit[M <: Model[M]](
      estimator: Estimator[M] & Params,
      name: String,
      dataset: Dataset[_],
      build: (String, ModelRef) => M
  ): M = {
    val session = dataset.sparkSession
    val input = dataset.plan.getRoot
    val info = MlClient.fit(
      session,
      operator(name, estimator.uid, proto.MlOperator.OperatorType.OPERATOR_TYPE_ESTIMATOR),
      MlSerializer.encodeParams(estimator),
      input
    )
    warn(name, info.getWarningMessage)
    val ref = new ModelRef(info.getObjRef.id, session, Some(input))
    val model = build(estimator.uid, ref)
    estimator.copyValues(model)
  }

  /** The estimated in-memory size of a cached model, in bytes (server 4.1 or newer). */
  def modelSize(ref: ModelRef): Long = MlClient.modelSize(ref.session, ref.id)

  /** A handle to the server's ML helper object, which hosts the stat functions. */
  private[ml] def helperRef(session: SparkSession): ModelRef =
    new ModelRef(HelperId, session, None)

  /** The object id the server uses for its ML helper (`ConnectHelper`). */
  private[ml] val HelperId: String = "______ML_CONNECT_HELPER______"

  /** Transforms `dataset` with the cached model `ref`, using the model's set parameters. */
  def transformWithModel(model: Params, ref: ModelRef, dataset: Dataset[_]): DataFrame =
    MlClient.transformRelation(
      ref.session,
      Left(ref.id),
      MlSerializer.encodeParams(model),
      dataset.plan.getRoot
    )

  /** Transforms `dataset` with a new (non-model) transformer of class `name`. */
  def transformWithTransformer(
      transformer: Transformer & Params,
      name: String,
      dataset: Dataset[_]
  ): DataFrame = {
    val session = dataset.sparkSession
    MlClient.transformRelation(
      session,
      Right(
        operator(name, transformer.uid, proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER)
      ),
      MlSerializer.encodeParams(transformer),
      dataset.plan.getRoot
    )
  }

  /** Evaluates `evaluator` with `name` on `dataset`, returning the metric. */
  def evaluate(evaluator: Params, name: String, dataset: Dataset[_]): Double = {
    val literal = MlClient.evaluate(
      dataset.sparkSession,
      operator(name, evaluator.uid, proto.MlOperator.OperatorType.OPERATOR_TYPE_EVALUATOR),
      MlSerializer.encodeParams(evaluator),
      dataset.plan.getRoot
    )
    MlSerializer.decodeValue(literal).asInstanceOf[Number].doubleValue()
  }

  /**
   * Reads an attribute of a cached object: `ref` followed by `path` (which may name a summary and
   * then a field of it), with optional arguments for the last method.
   */
  def fetch(ref: ModelRef, path: Seq[String], args: Seq[Any] = Nil): Any = {
    val methods = methodChain(path, args)
    val result = withSummaryRecovery(ref) {
      MlClient.fetch(ref.session, ref.id, methods)
    }
    if (result.resultType.isParam) {
      MlSerializer.decodeValue(result.getParam)
    } else if (result.resultType.isOperatorInfo) {
      result.getOperatorInfo.getObjRef.id
    } else {
      result.getSummary
    }
  }

  /** Reads a DataFrame attribute of a cached object. */
  def fetchRelation(ref: ModelRef, path: Seq[String], args: Seq[Any] = Nil): DataFrame = {
    val methods = methodChain(path, args)
    try
      MlClient.fetchRelation(ref.session, ref.id, methods, ref.summaryDataset)
    catch {
      case e: org.apache.spark.SparkException if isRecoverableSummaryLoss(e, ref) =>
        // Restore the summary from the training data and retry once.
        MlClient.createSummary(ref.session, ref.id, ref.summaryDataset.get)
        MlClient.fetchRelation(ref.session, ref.id, methods, ref.summaryDataset)
    }
  }

  private def methodChain(path: Seq[String], args: Seq[Any]): Seq[proto.Fetch.Method] =
    path.zipWithIndex.map { case (name, i) =>
      val isLast = i == path.length - 1
      val argList = if (isLast) args.map(argument) else Nil
      proto.Fetch.Method(method = name, args = argList)
    }

  private def argument(value: Any): proto.Fetch.Method.Args = value match {
    case ds: Dataset[_] =>
      proto.Fetch.Method.Args(argsType = proto.Fetch.Method.Args.ArgsType.Input(ds.plan.getRoot))
    case other =>
      proto.Fetch.Method
        .Args(argsType = proto.Fetch.Method.Args.ArgsType.Param(MlSerializer.encodeArgument(other)))
  }

  private def withSummaryRecovery[T](ref: ModelRef)(body: => T): T =
    try
      body
    catch {
      case e: org.apache.spark.SparkException if isRecoverableSummaryLoss(e, ref) =>
        MlClient.createSummary(ref.session, ref.id, ref.summaryDataset.get)
        body
    }

  // A lost summary can be recreated from the training data. CreateSummary exists from server 4.1,
  // so on an older server the original error is reported.
  private def isRecoverableSummaryLoss(e: org.apache.spark.SparkException, ref: ModelRef): Boolean =
    e.getErrorClass == "CONNECT_ML.MODEL_SUMMARY_LOST" && ref.summaryDataset.isDefined &&
      MlClient.supports(ref.session, 4, 1)

  private def warn(name: String, message: String): Unit =
    if (message != null && message.nonEmpty) {
      org.slf4j.LoggerFactory.getLogger(s"org.apache.spark.ml.$name").warn(message)
    }
}
