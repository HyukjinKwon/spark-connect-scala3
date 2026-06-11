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

import scala.util.control.NonFatal

/**
 * Retries a function with exponential backoff according to the configured [[RetryPolicy]]s. Ported
 * from Apache Spark's `org.apache.spark.sql.connect.client.GrpcRetryHandler`.
 */
private[sql] class GrpcRetryHandler(
    private val policies: Seq[RetryPolicy],
    private val sleep: Long => Unit = Thread.sleep
) {

  def this(policy: RetryPolicy, sleep: Long => Unit) = this(List(policy), sleep)
  def this(policy: RetryPolicy) = this(policy, Thread.sleep)

  /** Retries `fn` with exponential backoff per the client's retry policies. */
  def retry[T](fn: => T): T = new GrpcRetryHandler.Retrying(policies, sleep, fn).retry()
}

private[sql] object GrpcRetryHandler {

  /** Manages the retrying state for a single retryable block. */
  class Retrying[T](retryPolicies: Seq[RetryPolicy], sleep: Long => Unit, fn: => T) {
    private var currentRetryNum: Int = 0
    private var exceptionList: Seq[Throwable] = Seq.empty
    private val policies: Seq[RetryPolicy.RetryPolicyState] = retryPolicies.map(_.toState)

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

      // A RetryException is always retriable, with no backoff and no policy needed.
      if (lastException.isInstanceOf[RetryException]) return

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
