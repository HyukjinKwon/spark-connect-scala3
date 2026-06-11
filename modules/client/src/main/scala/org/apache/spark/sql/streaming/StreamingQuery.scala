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

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession

/**
 * A handle to a query that is executing continuously in the background as new data arrives.
 * Returned by [[DataStreamWriter#start]] and [[StreamingQueryManager]].
 *
 * Mirrors the public surface of `org.apache.spark.sql.streaming.StreamingQuery` over the Spark
 * Connect protocol. Every method issues a `StreamingQueryCommand` to the server, identified by the
 * query's [[id]] and [[runId]], and reads the typed result back from the response stream.
 *
 * @param spark
 *   the session that owns this query.
 * @param id
 *   the stable query id that persists across restarts from a checkpoint.
 * @param runId
 *   the run id that is unique for each start or restart of the query.
 * @param name
 *   the user-specified query name, or `null` if none was set.
 */
class StreamingQuery private[sql] (
    spark: SparkSession,
    val id: String,
    val runId: String,
    val name: String
) {

  private val instanceId: proto.StreamingQueryInstanceId =
    proto.StreamingQueryInstanceId(id = id, runId = runId)

  /** Returns whether the query is currently active (still running). */
  def isActive: Boolean = command(_.withStatus(true)).getStatus.isActive

  /**
   * Returns the current status of the query as a [[StreamingQueryStatus]] describing whether data
   * is available, a trigger is active, and the query is running.
   */
  def status: StreamingQueryStatus = {
    val s = command(_.withStatus(true)).getStatus
    StreamingQueryStatus(
      message = s.statusMessage,
      isDataAvailable = s.isDataAvailable,
      isTriggerActive = s.isTriggerActive,
      isActive = s.isActive
    )
  }

  /**
   * Returns an array of the most recent progress updates for this query, as JSON strings, ordered
   * from oldest to newest.
   */
  def recentProgress: Array[String] =
    command(_.withRecentProgress(true)).getRecentProgress.recentProgressJson.toArray

  /**
   * Returns the most recent progress update of this query as a JSON string, or `null` if no
   * progress has been made yet.
   */
  def lastProgress: String = {
    val progress = command(_.withLastProgress(true)).getRecentProgress.recentProgressJson
    progress.lastOption.orNull
  }

  /**
   * Waits for the termination of this query, blocking the current thread until the query stops or
   * fails.
   *
   * @return
   *   whether the query has terminated.
   */
  def awaitTermination(): Boolean =
    command(
      _.withAwaitTermination(proto.StreamingQueryCommand.AwaitTerminationCommand())
    ).getAwaitTermination.terminated

  /**
   * Waits for the termination of this query, blocking the current thread until the query stops or
   * fails, or until `timeoutMs` milliseconds have elapsed.
   *
   * @return
   *   whether the query has terminated within the timeout.
   */
  def awaitTermination(timeoutMs: Long): Boolean =
    command(
      _.withAwaitTermination(
        proto.StreamingQueryCommand.AwaitTerminationCommand(timeoutMs = Some(timeoutMs))
      )
    ).getAwaitTermination.terminated

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
   * Intended for testing with bounded sources.
   */
  def processAllAvailable(): Unit =
    command(_.withProcessAllAvailable(true))

  /** Stops the execution of this query if it is running. */
  def stop(): Unit =
    command(_.withStop(true))

  /** Prints the (logical and physical) plans of this query to the console for debugging. */
  def explain(): Unit = explain(extended = false)

  /**
   * Prints the (logical and physical) plans of this query to the console for debugging.
   *
   * @param extended
   *   whether to print the extended (more verbose) plan information.
   */
  def explain(extended: Boolean): Unit = {
    val result =
      command(
        _.withExplain(proto.StreamingQueryCommand.ExplainCommand(extended = extended))
      ).getExplain.result
    // scalastyle:off println
    println(result)
    // scalastyle:on println
  }

  /**
   * Returns the [[StreamingQueryException]] that terminated this query, if the query failed, or
   * `None` if the query is active or stopped normally.
   */
  def exception: Option[StreamingQueryException] = {
    val result = command(_.withException(true)).getException
    if (result.exceptionMessage.forall(_.isEmpty)) {
      None
    } else {
      Some(
        StreamingQueryException(
          message = result.exceptionMessage.getOrElse(""),
          errorClass = result.errorClass.getOrElse(""),
          stackTrace = result.stackTrace.getOrElse("")
        )
      )
    }
  }

  override def toString: String = s"StreamingQuery [id=$id, runId=$runId, name=$name]"

  /**
   * Builds a [[proto.StreamingQueryCommand]] for this query by applying `f`, sends it as a command
   * plan, drains the response stream and returns the typed [[proto.StreamingQueryCommandResult]].
   */
  private def command(
      f: proto.StreamingQueryCommand => proto.StreamingQueryCommand
  ): proto.StreamingQueryCommandResult = {
    val cmd = f(proto.StreamingQueryCommand(queryId = Some(instanceId)))
    val commandProto =
      proto.Command(commandType = proto.Command.CommandType.StreamingQueryCommand(cmd))
    val plan = proto.Plan(proto.Plan.OpType.Command(commandProto))
    val responses = spark.client.execute(plan)
    var result: Option[proto.StreamingQueryCommandResult] = None
    while (responses.hasNext) {
      val response = responses.next()
      if (result.isEmpty && response.responseType.streamingQueryCommandResult.isDefined) {
        result = response.responseType.streamingQueryCommandResult
      }
    }
    result.getOrElse(
      throw new IllegalStateException("Server did not return a StreamingQueryCommandResult")
    )
  }
}

/**
 * A snapshot of a [[StreamingQuery]]'s current status.
 *
 * @param message
 *   a human-readable description of what the query is currently doing.
 * @param isDataAvailable
 *   whether there is any data available to be processed.
 * @param isTriggerActive
 *   whether a trigger is currently active (a micro-batch is being processed).
 * @param isActive
 *   whether the query is still running.
 */
case class StreamingQueryStatus(
    message: String,
    isDataAvailable: Boolean,
    isTriggerActive: Boolean,
    isActive: Boolean
)

/**
 * Describes the exception that terminated a [[StreamingQuery]].
 *
 * @param message
 *   the exception message, matching the server-side `StreamingQueryException.toString`.
 * @param errorClass
 *   the error class of the exception, if any.
 * @param stackTrace
 *   the stack trace of the exception, if any.
 */
case class StreamingQueryException(message: String, errorClass: String, stackTrace: String)
