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
 * A class to manage all the [[StreamingQuery]] active in a [[SparkSession]]. Mirrors
 * `org.apache.spark.sql.streaming.StreamingQueryManager`. Use `SparkSession.streams` to access
 * this.
 */
class StreamingQueryManager private[sql] (sparkSession: SparkSession) {

  /** Returns a list of active queries associated with this SparkSession. */
  def active: Array[StreamingQuery] =
    executeCommand(_.withActive(true)).getActive.activeQueries
      .map(instanceToQuery)
      .toArray

  /**
   * Returns the query if there is an active query with the given id, or `null` otherwise.
   *
   * @param id
   *   the id of the query (see [[StreamingQuery.id]]).
   */
  def get(id: String): StreamingQuery = {
    val result = executeCommand(_.withGetQuery(id))
    if (result.resultType.isQuery) instanceToQuery(result.getQuery) else null
  }

  /** Wait until any of the queries on the associated SparkSession has terminated. */
  def awaitAnyTermination(): Unit = {
    executeCommand(
      _.withAwaitAnyTermination(proto.StreamingQueryManagerCommand.AwaitAnyTerminationCommand())
    )
    ()
  }

  /**
   * Wait until any of the queries on the associated SparkSession has terminated, or `timeoutMs` has
   * elapsed.
   *
   * @return
   *   whether any query has terminated within the timeout.
   */
  def awaitAnyTermination(timeoutMs: Long): Boolean =
    executeCommand(
      _.withAwaitAnyTermination(
        proto.StreamingQueryManagerCommand
          .AwaitAnyTerminationCommand(timeoutMs = Some(timeoutMs))
      )
    ).getAwaitAnyTermination.terminated

  /**
   * Forget about past terminated queries so that [[awaitAnyTermination()]] can be used again to
   * wait for new terminations.
   */
  def resetTerminated(): Unit = {
    executeCommand(_.withResetTerminated(true))
    ()
  }

  private def instanceToQuery(
      instance: proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
  ): StreamingQuery =
    new StreamingQuery(
      sparkSession,
      instance.id.getOrElse(proto.StreamingQueryInstanceId()),
      instance.name.getOrElse("")
    )

  private def executeCommand(
      build: proto.StreamingQueryManagerCommand => proto.StreamingQueryManagerCommand
  ): proto.StreamingQueryManagerCommandResult = {
    val command = build(proto.StreamingQueryManagerCommand())
    val plan = proto.Plan().withCommand(proto.Command().withStreamingQueryManagerCommand(command))
    val responses = sparkSession.execute(plan)
    try
      responses.asScala
        .find(_.responseType.isStreamingQueryManagerCommandResult)
        .map(_.getStreamingQueryManagerCommandResult)
        .getOrElse(
          throw new IllegalStateException(
            "Server did not return a StreamingQueryManagerCommandResult for the command."
          )
        )
    finally while (responses.hasNext) responses.next()
  }
}
