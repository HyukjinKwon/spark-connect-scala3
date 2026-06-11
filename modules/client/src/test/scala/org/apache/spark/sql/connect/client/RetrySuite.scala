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

import scala.concurrent.duration.FiniteDuration

import io.grpc.{Status, StatusRuntimeException}

class RetryPolicySuite extends munit.FunSuite {

  test("default policy retries UNAVAILABLE") {
    assert(RetryPolicy.defaultPolicyRetryException(Status.UNAVAILABLE.asRuntimeException()))
  }

  test("default policy does not retry INVALID_ARGUMENT") {
    assert(!RetryPolicy.defaultPolicyRetryException(Status.INVALID_ARGUMENT.asRuntimeException()))
  }

  test("default policy does not retry non-gRPC exceptions") {
    assert(!RetryPolicy.defaultPolicyRetryException(new RuntimeException("boom")))
  }

  test("default policy retries INTERNAL with INVALID_CURSOR.DISCONNECTED") {
    val e = Status.INTERNAL.withDescription("INVALID_CURSOR.DISCONNECTED").asRuntimeException()
    assert(RetryPolicy.defaultPolicyRetryException(e))
  }

  test("RetryPolicyState yields backoffs until maxRetries then stops") {
    val policy = RetryPolicy(
      maxRetries = Some(3),
      initialBackoff = FiniteDuration(1, "ms"),
      maxBackoff = Some(FiniteDuration(4, "ms")),
      backoffMultiplier = 2.0,
      name = "test"
    )
    val state = policy.toState
    val e: StatusRuntimeException = Status.UNAVAILABLE.asRuntimeException()
    assert(state.nextAttempt(e).isDefined)
    assert(state.nextAttempt(e).isDefined)
    assert(state.nextAttempt(e).isDefined)
    assert(state.nextAttempt(e).isEmpty, "should stop after maxRetries")
  }
}

class GrpcRetryHandlerSuite extends munit.FunSuite {

  private val fastPolicy = RetryPolicy(
    maxRetries = Some(5),
    initialBackoff = FiniteDuration(1, "ms"),
    maxBackoff = Some(FiniteDuration(1, "ms")),
    name = "test"
  )

  // No-op sleep so the test is fast and deterministic.
  private def handler = new GrpcRetryHandler(List(fastPolicy), _ => ())

  test("retries a transient failure and then succeeds") {
    var calls = 0
    val result = handler.retry {
      calls += 1
      if (calls <= 3) throw Status.UNAVAILABLE.asRuntimeException()
      "ok"
    }
    assertEquals(result, "ok")
    assertEquals(calls, 4)
  }

  test("does not retry a non-retryable error") {
    var calls = 0
    intercept[StatusRuntimeException] {
      handler.retry {
        calls += 1
        throw Status.INVALID_ARGUMENT.asRuntimeException()
      }
    }
    assertEquals(calls, 1)
  }

  test("gives up and rethrows after exceeding maxRetries") {
    var calls = 0
    intercept[StatusRuntimeException] {
      handler.retry {
        calls += 1
        throw Status.UNAVAILABLE.asRuntimeException()
      }
    }
    assertEquals(calls, 6) // 1 initial attempt + 5 retries
  }
}
