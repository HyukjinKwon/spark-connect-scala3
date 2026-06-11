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
package org.apache.spark.sql.streaming

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession

/**
 * A handle to a query that is executing continuously in the background as new data arrives. All
 * these methods are thread-safe. Mirrors `org.apache.spark.sql.streaming.StreamingQuery`.
 */
class StreamingQuery private[sql] (
    sparkSession: SparkSession,
    private[sql] val instanceId: proto.StreamingQueryInstanceId,
    private val queryName: String
) {

  /** Returns the unique id of this query that persists across restarts from checkpoint data. */
  def id: String = instanceId.id

  /** Returns the unique id of this run of the query. */
  def runId: String = instanceId.runId

  /** Returns the user-specified name of the query, or `null` if not specified. */
  def name: String = if (queryName == null || queryName.isEmpty) null else queryName

  /** Returns the [[SparkSession]] associated with `this`. */
  def sparkSessionRef: SparkSession = sparkSession

  /** Whether the query is currently active. */
  def isActive: Boolean = executeCommand(_.withStatus(true)).getStatus.isActive

  /** Returns the current status of the query. */
  def status: StreamingQueryStatus =
    StreamingQueryStatus(executeCommand(_.withStatus(true)).getStatus)

  /**
   * Returns an array of the most recent [[StreamingQueryProgress]] updates for this query. The
   * number of progress updates retained is configured on the server.
   */
  def recentProgress: Array[StreamingQueryProgress] =
    executeCommand(_.withRecentProgress(true)).getRecentProgress.recentProgressJson
      .map(StreamingQueryProgress(_))
      .toArray

  /** Returns the most recent [[StreamingQueryProgress]], or `null` if there were no progresses. */
  def lastProgress: StreamingQueryProgress =
    executeCommand(_.withLastProgress(true)).getRecentProgress.recentProgressJson.lastOption
      .map(StreamingQueryProgress(_))
      .orNull

  /**
   * Waits for the termination of `this` query, either by `stop()` or by an exception. If the query
   * has terminated with an exception, then the exception will be thrown.
   */
  def awaitTermination(): Unit = {
    val terminated =
      executeCommand(
        _.withAwaitTermination(proto.StreamingQueryCommand.AwaitTerminationCommand())
      ).getAwaitTermination.terminated
    if (terminated) throwExceptionIfPresent()
  }

  /**
   * Waits for the termination of `this` query, either by `stop()` or by an exception, or until
   * `timeoutMs` has elapsed.
   *
   * @return
   *   whether the query has terminated within the timeout.
   */
  def awaitTermination(timeoutMs: Long): Boolean = {
    val terminated = executeCommand(
      _.withAwaitTermination(
        proto.StreamingQueryCommand.AwaitTerminationCommand(timeoutMs = Some(timeoutMs))
      )
    ).getAwaitTermination.terminated
    if (terminated) throwExceptionIfPresent()
    terminated
  }

  /** Blocks until all available data in the source has been processed and committed to the sink. */
  def processAllAvailable(): Unit = {
    executeCommand(_.withProcessAllAvailable(true))
    ()
  }

  /** Stops the execution of this query if it is running. */
  def stop(): Unit = {
    executeCommand(_.withStop(true))
    ()
  }

  /** Prints the physical plan to the console for debugging purposes. */
  def explain(): Unit = explain(extended = false)

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @param extended
   *   whether to do extended explain or not.
   */
  def explain(extended: Boolean): Unit = {
    val result = executeCommand(
      _.withExplain(proto.StreamingQueryCommand.ExplainCommand(extended = extended))
    ).getExplain.result
    // scalastyle:off println
    println(result)
    // scalastyle:on println
  }

  /**
   * Returns the [[StreamingQueryException]] if the query was terminated by an exception, or `None`
   * otherwise.
   */
  def exception: Option[StreamingQueryException] = {
    val result = executeCommand(_.withException(true)).getException
    result.exceptionMessage.filter(_.nonEmpty).map { msg =>
      new StreamingQueryException(msg, result.errorClass, result.stackTrace)
    }
  }

  override def toString: String =
    s"StreamingQuery(id=$id, runId=$runId, name=${if (name == null) "null" else name})"

  private def throwExceptionIfPresent(): Unit = exception.foreach(e => throw e)

  private def executeCommand(
      build: proto.StreamingQueryCommand => proto.StreamingQueryCommand
  ): proto.StreamingQueryCommandResult = {
    val command = build(proto.StreamingQueryCommand(queryId = Some(instanceId)))
    val plan = proto.Plan().withCommand(proto.Command().withStreamingQueryCommand(command))
    val responses = sparkSession.execute(plan)
    try
      responses.asScala
        .find(_.responseType.isStreamingQueryCommandResult)
        .map(_.getStreamingQueryCommandResult)
        .getOrElse(
          throw new IllegalStateException(
            "Server did not return a StreamingQueryCommandResult for the streaming command."
          )
        )
    finally while (responses.hasNext) responses.next()
  }
}
