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

package org.apache.spark.sql.connect.client

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Per-RPC gRPC deadlines (timeouts). Each field bounds one RPC type; `None` disables the deadline
 * for that RPC. The defaults match the Apache Spark 4.3 Connect clients.
 *
 * `reattachableExecutePlan` and `reattachExecute` bound each individual result-stream segment, not
 * the query: when one fires, the server-side operation keeps running and the client resumes with a
 * fresh `ReattachExecute` stream. Non-reattachable `ExecutePlan` has no deadline because a timeout
 * there would kill the execution with no way to recover.
 *
 * {{{
 *   SparkConnectClient.builder().rpcDeadlines(RpcDeadlines(analyzePlan = Some(10.minutes)))
 *   SparkConnectClient.builder().rpcDeadlines(RpcDeadlines.disabled)
 * }}}
 */
case class RpcDeadlines(
    reattachableExecutePlan: Option[FiniteDuration] = Some(10.minutes),
    reattachExecute: Option[FiniteDuration] = Some(10.minutes),
    analyzePlan: Option[FiniteDuration] = Some(1.hour),
    addArtifacts: Option[FiniteDuration] = Some(1.hour),
    config: Option[FiniteDuration] = Some(10.minutes),
    interrupt: Option[FiniteDuration] = Some(10.minutes),
    releaseSession: Option[FiniteDuration] = Some(10.minutes),
    artifactStatus: Option[FiniteDuration] = Some(10.minutes),
    cloneSession: Option[FiniteDuration] = Some(10.minutes),
    getStatus: Option[FiniteDuration] = Some(10.minutes),
    fetchErrorDetails: Option[FiniteDuration] = Some(10.minutes)
) {

  private val namedFields: Seq[(String, Option[FiniteDuration])] =
    productElementNames.toSeq.zip(
      productIterator.map(_.asInstanceOf[Option[FiniteDuration]]).toSeq
    )

  namedFields.foreach { case (name, opt) =>
    opt.foreach(d =>
      require(d.toMillis > 0, s"RpcDeadlines.$name must be a positive duration, got $d")
    )
  }

  override def toString: String = {
    val configured = namedFields.collect { case (name, Some(d)) => s"$name=$d" }
    if (configured.isEmpty) "RpcDeadlines(all disabled)"
    else s"RpcDeadlines(${configured.mkString(", ")})"
  }
}

object RpcDeadlines {

  /** Deadlines with every per-RPC timeout disabled. */
  val disabled: RpcDeadlines = RpcDeadlines(
    reattachableExecutePlan = None,
    reattachExecute = None,
    analyzePlan = None,
    addArtifacts = None,
    config = None,
    interrupt = None,
    releaseSession = None,
    artifactStatus = None,
    cloneSession = None,
    getStatus = None,
    fetchErrorDetails = None
  )
}
