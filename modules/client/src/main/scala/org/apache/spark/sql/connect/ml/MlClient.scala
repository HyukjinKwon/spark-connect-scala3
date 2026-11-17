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
package org.apache.spark.sql.connect.ml

import org.apache.spark.connect.proto
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter

/**
 * Sends ML commands and ML relations to the server, mirroring the PySpark ML Connect client
 * (`pyspark/ml/util.py` and `pyspark/ml/connect/readwrite.py`). [[MlCommands]] builds the protos.
 */
private[spark] object MlClient {

  // The server version per session, so the version RPC runs once per session.
  private val versions =
    java.util.Collections.synchronizedMap(new java.util.WeakHashMap[SparkSession, (Int, Int)]())

  /** Whether the server behind `session` is at least `major.minor`. */
  def supports(session: SparkSession, major: Int, minor: Int): Boolean = {
    val (svMajor, svMinor) = versions.synchronized {
      Option(versions.get(session)).getOrElse {
        val parts = session.version.split("[.-]").take(2).map(_.toIntOption.getOrElse(0))
        val v = (parts.headOption.getOrElse(0), parts.lift(1).getOrElse(0))
        versions.put(session, v)
        v
      }
    }
    svMajor > major || (svMajor == major && svMinor >= minor)
  }

  /**
   * The server version of `session` if an earlier ML command already learned it. Never makes a
   * round trip, so it is safe on a session that is closing.
   */
  def knownVersion(session: SparkSession): Option[(Int, Int)] =
    versions.synchronized(Option(versions.get(session)))

  /** Fails with a clear message when the server is older than `major.minor`. */
  def requireVersion(session: SparkSession, major: Int, minor: Int, feature: String): Unit =
    if (!supports(session, major, minor)) {
      throw new UnsupportedOperationException(
        s"$feature requires a Spark Connect server $major.$minor or newer, " +
          s"but the server is Spark ${session.version}."
      )
    }

  // The session that ran the latest ML command. Used when no active session is usable.
  @volatile private var lastSession: java.lang.ref.WeakReference[SparkSession] = null

  /**
   * The session ML persistence and loading should use: the active session when its connection is
   * still open, otherwise the session of the latest ML command. `SparkSession.active` is a
   * thread-local, so it can name a session that another thread has closed, and a call on that
   * session would wait out the client's retries.
   */
  def currentSession(): SparkSession = {
    val active =
      try Some(SparkSession.active)
      catch { case _: IllegalStateException => None }
    val open = (active.toSeq ++ Option(lastSession).flatMap(r => Option(r.get())).toSeq)
      .find(s => !s.client.channel.isShutdown)
    open.getOrElse(throw new IllegalStateException("No open Spark session for the ML operation."))
  }

  /** Runs `command` and returns the `MlCommandResult` the server sends back. */
  def run(session: SparkSession, command: proto.MlCommand): proto.MlCommandResult = {
    requireVersion(session, 4, 0, "Spark ML over Spark Connect")
    lastSession = new java.lang.ref.WeakReference(session)
    session.mlCacheUsed = true
    val responses = session.client.execute(MlCommands.plan(command))
    try {
      var result: Option[proto.MlCommandResult] = None
      while (responses.hasNext) {
        val response = responses.next()
        if (response.responseType.isMlCommandResult) {
          result = Some(response.getMlCommandResult)
        }
      }
      result.getOrElse(
        throw new IllegalStateException("The server did not return an ML command result.")
      )
    } finally responses.close()
  }

  /** Fits `estimator` on `dataset` and returns the operator info of the fitted model. */
  def fit(
      session: SparkSession,
      estimator: proto.MlOperator,
      params: proto.MlParams,
      dataset: proto.Relation
  ): proto.MlCommandResult.MlOperatorInfo =
    run(session, MlCommands.fit(estimator, params, dataset)).getOperatorInfo

  /** Evaluates `evaluator` on `dataset` and returns the metric literal. */
  def evaluate(
      session: SparkSession,
      evaluator: proto.MlOperator,
      params: proto.MlParams,
      dataset: proto.Relation
  ): proto.Expression.Literal =
    run(session, MlCommands.evaluate(evaluator, params, dataset)).getParam

  /** Fetches an attribute or calls a method as a command, returning the raw result. */
  def fetch(
      session: SparkSession,
      objRef: String,
      methods: Seq[proto.Fetch.Method]
  ): proto.MlCommandResult =
    run(session, MlCommands.fetch(objRef, methods))

  /** Fetches an attribute that is a DataFrame (a model summary or helper relation). */
  def fetchRelation(
      session: SparkSession,
      objRef: String,
      methods: Seq[proto.Fetch.Method],
      summaryDataset: Option[proto.Relation] = None
  ): DataFrame =
    relation(session, MlCommands.fetchRelation(objRef, methods, summaryDataset))

  /** The DataFrame of `transform(input)` with a cached model or a new transformer. */
  def transformRelation(
      session: SparkSession,
      operator: Either[String, proto.MlOperator],
      params: proto.MlParams,
      input: proto.Relation
  ): DataFrame =
    relation(session, MlCommands.transform(operator, params, input))

  /** Saves a cached model or an operator to `path`. */
  def write(session: SparkSession, write: proto.MlCommand.Write): Unit =
    run(session, MlCommands.write(write))

  /** Loads an operator of `name` (and `operatorType`) from `path`. */
  def read(
      session: SparkSession,
      name: String,
      operatorType: proto.MlOperator.OperatorType,
      path: String
  ): proto.MlCommandResult.MlOperatorInfo =
    run(session, MlCommands.read(name, operatorType, path)).getOperatorInfo

  /** Deletes cached ML objects (models or summaries). Returns the ids the server removed. */
  def delete(session: SparkSession, ids: Seq[String]): Seq[String] =
    if (ids.isEmpty) Seq.empty
    else
      run(session, MlCommands.delete(ids)).getOperatorInfo.getObjRef.id
        .split(",")
        .toSeq
        .filter(_.nonEmpty)

  /** Evicts every cached ML object in the session. */
  def cleanCache(session: SparkSession): Unit = run(session, MlCommands.cleanCache)

  /** Recreates a lost model summary from `dataset`. */
  def createSummary(session: SparkSession, modelRef: String, dataset: proto.Relation): Unit =
    run(session, MlCommands.createSummary(modelRef, dataset))

  /** The estimated in-memory size of the cached model `id`, in bytes. Needs server 4.1 or newer. */
  def modelSize(session: SparkSession, id: String): Long = {
    requireVersion(session, 4, 1, "Model size")
    val literal = run(session, MlCommands.getModelSize(id)).getParam
    LiteralValueProtoConverter.toScalaValue(literal).asInstanceOf[Number].longValue()
  }

  /** A description of the ML objects cached in the session. Needs server 4.1 or newer. */
  def cacheInfo(session: SparkSession): String = {
    requireVersion(session, 4, 1, "ML cache information")
    val literal = run(session, MlCommands.getCacheInfo).getParam
    LiteralValueProtoConverter.toScalaValue(literal).toString
  }

  private def relation(session: SparkSession, mlRelation: proto.MlRelation): DataFrame =
    session.newDataFrame(proto.Relation.RelType.MlRelation(mlRelation))
}
