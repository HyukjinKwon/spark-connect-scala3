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

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Random

import com.google.rpc.RetryInfo
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto

/**
 * Configures the retry mechanism used by [[GrpcRetryHandler]]. Ported from Apache Spark's
 * `org.apache.spark.sql.connect.client.RetryPolicy`.
 *
 * @param maxRetries
 *   maximum number of retries (None = unbounded).
 * @param initialBackoff
 *   start of the exponential backoff.
 * @param maxBackoff
 *   cap on the exponential backoff.
 * @param backoffMultiplier
 *   multiplicative base of the exponential backoff.
 * @param jitter
 *   random value in `[0, jitter]` added to the backoff.
 * @param minJitterThreshold
 *   minimum backoff at which jitter is added.
 * @param canRetry
 *   predicate deciding whether an error is retryable.
 * @param name
 *   policy name (for diagnostics).
 * @param recognizeServerRetryDelay
 *   honor a server-provided `RetryInfo.retry_delay`.
 * @param maxServerRetryDelay
 *   cap on the server-provided retry delay.
 */
case class RetryPolicy(
    maxRetries: Option[Int] = None,
    initialBackoff: FiniteDuration = FiniteDuration(1000, "ms"),
    maxBackoff: Option[FiniteDuration] = None,
    backoffMultiplier: Double = 1.0,
    jitter: FiniteDuration = FiniteDuration(0, "s"),
    minJitterThreshold: FiniteDuration = FiniteDuration(0, "s"),
    canRetry: Throwable => Boolean = RetryPolicy.defaultPolicyRetryException,
    name: String = "DefaultPolicy",
    recognizeServerRetryDelay: Boolean = false,
    maxServerRetryDelay: Option[FiniteDuration] = None
) {

  def getName: String = name
  def toState: RetryPolicy.RetryPolicyState = new RetryPolicy.RetryPolicyState(this)
}

object RetryPolicy {

  def defaultPolicy(): RetryPolicy = RetryPolicy(
    name = "DefaultPolicy",
    // These constants mirror the PySpark/JVM clients: the maximum tolerated wait is at
    // least 10 minutes.
    maxRetries = Some(15),
    initialBackoff = FiniteDuration(50, "ms"),
    maxBackoff = Some(FiniteDuration(1, "min")),
    backoffMultiplier = 4.0,
    jitter = FiniteDuration(500, "ms"),
    minJitterThreshold = FiniteDuration(2, "s"),
    canRetry = defaultPolicyRetryException,
    recognizeServerRetryDelay = true,
    maxServerRetryDelay = Some(FiniteDuration(10, "min"))
  )

  def defaultPolicies(): Seq[RetryPolicy] = List(defaultPolicy())

  /** Tracks how many retries have happened and how long to wait before the next one. */
  private[client] class RetryPolicyState(val policy: RetryPolicy) {
    private var numberAttempts = 0
    private var nextWait: Duration = policy.initialBackoff

    /** Returns the wait before the next attempt, or None once max retries is exceeded. */
    def nextAttempt(e: Throwable): Option[Duration] = {
      if (policy.maxRetries.isDefined && numberAttempts >= policy.maxRetries.get) {
        return None
      }
      numberAttempts += 1

      var currentWait = nextWait
      nextWait = nextWait * policy.backoffMultiplier
      if (policy.maxBackoff.isDefined) {
        nextWait = nextWait min policy.maxBackoff.get
      }

      if (policy.recognizeServerRetryDelay) {
        extractRetryDelay(e).foreach { retryDelay =>
          val retryDelayLimited = retryDelay min policy.maxServerRetryDelay.getOrElse(retryDelay)
          currentWait = currentWait max retryDelayLimited
        }
      }

      if (currentWait >= policy.minJitterThreshold) {
        currentWait += policy.jitter * Random.nextDouble()
      }
      Some(currentWait)
    }

    def canRetry(throwable: Throwable): Boolean = policy.canRetry(throwable)
    def getName: String = policy.getName
  }

  /** Default retryable predicate: gRPC UNAVAILABLE, an interrupted cursor, or a `RetryInfo`. */
  private[client] def defaultPolicyRetryException(e: Throwable): Boolean = e match {
    case e: StatusRuntimeException =>
      val statusCode: Status.Code = e.getStatus.getCode
      if (
        statusCode == Status.Code.INTERNAL && e.toString.contains("INVALID_CURSOR.DISCONNECTED")
      ) {
        true
      } else if (statusCode == Status.Code.UNAVAILABLE) {
        true
      } else {
        extractRetryInfo(e).isDefined
      }
    case _ => false
  }

  private def extractRetryInfo(e: Throwable): Option[RetryInfo] = e match {
    case e: StatusRuntimeException =>
      Option(StatusProto.fromThrowable(e)).flatMap { status =>
        status.getDetailsList.asScala
          .find(_.is(classOf[RetryInfo]))
          .map(_.unpack(classOf[RetryInfo]))
      }
    case _ => None
  }

  private def extractRetryDelay(e: Throwable): Option[FiniteDuration] =
    extractRetryInfo(e)
      .flatMap(info => Option(info.getRetryDelay))
      .map(d => FiniteDuration(d.getSeconds, "s") + FiniteDuration(d.getNanos.toLong, "ns"))
}
