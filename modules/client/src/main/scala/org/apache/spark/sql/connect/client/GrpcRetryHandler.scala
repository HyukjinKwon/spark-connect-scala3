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
import scala.util.control.NonFatal

/**
 * Retries a function with exponential backoff according to the configured [[RetryPolicy]]s. Ported
 * from Apache Spark's `org.apache.spark.sql.connect.client.GrpcRetryHandler`.
 *
 * @param maxRetryExceptionElapsedTime
 *   the maximum cumulative wall-clock time to keep retrying a [[GrpcRetryHandler.RetryException]]
 *   (which is otherwise retried unconditionally, without consulting any [[RetryPolicy]]) before
 *   giving up, measured from the first such exception seen by one retry loop.
 */
private[sql] class GrpcRetryHandler(
    private val policies: Seq[RetryPolicy],
    private val sleep: Long => Unit = Thread.sleep,
    private val maxRetryExceptionElapsedTime: FiniteDuration =
      GrpcRetryHandler.DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
    private val nowNanos: () => Long = () => System.nanoTime()
) {

  def this(policy: RetryPolicy, sleep: Long => Unit) = this(List(policy), sleep)
  def this(policy: RetryPolicy) = this(policy, Thread.sleep)

  /** Retries `fn` with exponential backoff per the client's retry policies. */
  def retry[T](fn: => T): T =
    new GrpcRetryHandler.Retrying(policies, sleep, fn, maxRetryExceptionElapsedTime, nowNanos)
      .retry()
}

private[sql] object GrpcRetryHandler {

  /** Default cumulative bound for RetryException-driven retries (matches Apache Spark 4.3). */
  val DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME: FiniteDuration = 1.hour

  /** Manages the retrying state for a single retryable block. */
  class Retrying[T](
      retryPolicies: Seq[RetryPolicy],
      sleep: Long => Unit,
      fn: => T,
      maxRetryExceptionElapsedTime: FiniteDuration = DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
      nowNanos: () => Long = () => System.nanoTime()
  ) {
    private var currentRetryNum: Int = 0
    private var exceptionList: Seq[Throwable] = Seq.empty
    private val policies: Seq[RetryPolicy.RetryPolicyState] = retryPolicies.map(_.toState)

    // Set when the first RetryException is observed; bounds the time spent in such retries.
    private var retryExceptionStartNanos: Option[Long] = None

    def canRetry(throwable: Throwable): Boolean =
      throwable.isInstanceOf[RetryException] || policies.exists(_.canRetry(throwable))

    def makeAttempt(): Option[T] =
      try Some(fn)
      catch {
        case NonFatal(e) if canRetry(e) =>
          currentRetryNum += 1
          exceptionList = e +: exceptionList
          None
      }

    def waitAfterAttempt(): Unit = {
      val lastException = exceptionList.head

      if (lastException.isInstanceOf[RetryException]) {
        // A RetryException is retried immediately without consulting any policy, as long as the
        // cumulative time spent on such retries stays within maxRetryExceptionElapsedTime.
        val start = retryExceptionStartNanos.getOrElse {
          val now = nowNanos()
          retryExceptionStartNanos = Some(now)
          now
        }
        if (nowNanos() - start >= maxRetryExceptionElapsedTime.toNanos) {
          // Surface the underlying cause (attached via addSuppressed) rather than the marker.
          throw lastException.getSuppressed.headOption.getOrElse(lastException)
        }
        return
      }

      val matchedPolicyOpt = policies.find(_.canRetry(lastException))
      if (matchedPolicyOpt.isDefined) {
        val time = matchedPolicyOpt.get.nextAttempt(lastException)
        if (time.isDefined) {
          sleep(time.get.toMillis)
          return
        }
      }
      // Out of retries: rethrow the last error.
      throw lastException
    }

    def retry(): T = {
      var result = makeAttempt()
      while (result.isEmpty) {
        waitAfterAttempt()
        result = makeAttempt()
      }
      result.get
    }
  }

  /** An exception thrown inside a retry block that is always retryable, ignoring policies. */
  class RetryException extends Throwable
}
