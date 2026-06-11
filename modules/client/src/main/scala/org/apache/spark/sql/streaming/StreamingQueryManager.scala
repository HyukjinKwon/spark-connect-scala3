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
 * Manages all the [[StreamingQuery]] instances active in a single [[SparkSession]]. Use
 * `SparkSession.streams` to access this.
 *
 * Mirrors the public surface of `org.apache.spark.sql.streaming.StreamingQueryManager` over the
 * Spark Connect protocol. Every method issues a `StreamingQueryManagerCommand` to the server and
 * reads the typed result back from the response stream.
 *
 * @param spark
 *   the session whose streaming queries are managed.
 */
class StreamingQueryManager private[sql] (spark: SparkSession) {

  /** Returns an array of the currently active queries associated with this session. */
  def active: Array[StreamingQuery] =
    command(_.withActive(true)).getActive.activeQueries.map(toStreamingQuery).toArray

  /**
   * Returns the active query identified by `id`, or `null` if no active query with that id exists.
   */
  def get(id: String): StreamingQuery = {
    val result = command(_.withGetQuery(id))
    if (result.resultType.isQuery) toStreamingQuery(result.getQuery) else null
  }

  /**
   * Waits until any of the active queries on the associated session terminates, blocking the
   * current thread.
   *
   * @return
   *   whether any query has terminated.
   */
  def awaitAnyTermination(): Boolean =
    command(
      _.withAwaitAnyTermination(proto.StreamingQueryManagerCommand.AwaitAnyTerminationCommand())
    ).getAwaitAnyTermination.terminated

  /**
   * Waits until any of the active queries on the associated session terminates, blocking the
   * current thread, or until `timeoutMs` milliseconds have elapsed.
   *
   * @return
   *   whether any query has terminated within the timeout.
   */
  def awaitAnyTermination(timeoutMs: Long): Boolean =
    command(
      _.withAwaitAnyTermination(
        proto.StreamingQueryManagerCommand.AwaitAnyTerminationCommand(timeoutMs = Some(timeoutMs))
      )
    ).getAwaitAnyTermination.terminated

  /**
   * Forgets about the past terminated queries so that [[awaitAnyTermination()]] can be used again
   * to wait for new terminations.
   */
  def resetTerminated(): Unit =
    command(_.withResetTerminated(true))

  private def toStreamingQuery(
      instance: proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
  ): StreamingQuery = {
    val instanceId = instance.getId
    new StreamingQuery(spark, instanceId.id, instanceId.runId, instance.name.orNull)
  }

  /**
   * Builds a [[proto.StreamingQueryManagerCommand]] by applying `f`, sends it as a command plan,
   * drains the response stream and returns the typed [[proto.StreamingQueryManagerCommandResult]].
   */
  private def command(
      f: proto.StreamingQueryManagerCommand => proto.StreamingQueryManagerCommand
  ): proto.StreamingQueryManagerCommandResult = {
    val cmd = f(proto.StreamingQueryManagerCommand())
    val commandProto =
      proto.Command(commandType = proto.Command.CommandType.StreamingQueryManagerCommand(cmd))
    val plan = proto.Plan(proto.Plan.OpType.Command(commandProto))
    val responses = spark.client.execute(plan)
    var result: Option[proto.StreamingQueryManagerCommandResult] = None
    while (responses.hasNext) {
      val response = responses.next()
      if (result.isEmpty && response.responseType.streamingQueryManagerCommandResult.isDefined) {
        result = response.responseType.streamingQueryManagerCommandResult
      }
    }
    result.getOrElse(
      throw new IllegalStateException("Server did not return a StreamingQueryManagerCommandResult")
    )
  }
}
