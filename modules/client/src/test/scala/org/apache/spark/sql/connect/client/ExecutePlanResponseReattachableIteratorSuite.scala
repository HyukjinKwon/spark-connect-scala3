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

import java.util.concurrent.CopyOnWriteArrayList

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import io.grpc.Status

import org.apache.spark.connect.proto

class ExecutePlanResponseReattachableIteratorSuite extends munit.FunSuite {

  private def resp(id: String, complete: Boolean): proto.ExecutePlanResponse = {
    val base = proto.ExecutePlanResponse(responseId = id, sessionId = "s", serverSideSessionId = "")
    if (complete) {
      base.copy(responseType =
        proto.ExecutePlanResponse.ResponseType.ResultComplete(
          proto.ExecutePlanResponse.ResultComplete()
        )
      )
    } else base
  }

  private val baseRequest = proto.ExecutePlanRequest(
    sessionId = "s",
    userContext = Some(proto.UserContext(userId = "u")),
    plan = Some(proto.Plan())
  )

  // No-op sleep so retries are instant.
  private def handler =
    new GrpcRetryHandler(
      List(
        RetryPolicy(maxRetries = Some(5), initialBackoff = FiniteDuration(1, "ms"), name = "test")
      ),
      _ => ()
    )

  /** A programmable [[ReattachableStub]] recording reattach/release requests. */
  private class FakeStub(
      first: () => Iterator[proto.ExecutePlanResponse],
      second: () => Iterator[proto.ExecutePlanResponse]
  ) extends ReattachableStub {
    @volatile var executeCount = 0
    val reattachRequests = new CopyOnWriteArrayList[proto.ReattachExecuteRequest]()
    val releaseRequests = new CopyOnWriteArrayList[proto.ReleaseExecuteRequest]()
    override def executePlan(r: proto.ExecutePlanRequest) = { executeCount += 1; first() }
    override def reattachExecute(r: proto.ReattachExecuteRequest) = {
      reattachRequests.add(r); second()
    }
    override def releaseExecute(r: proto.ReleaseExecuteRequest) = {
      releaseRequests.add(r); proto.ReleaseExecuteResponse()
    }
  }

  test("reattaches when the stream ends without ResultComplete") {
    val stub = new FakeStub(
      first = () => Iterator(resp("1", complete = false)),
      second = () => Iterator(resp("2", complete = true))
    )
    val it = new ExecutePlanResponseReattachableIterator(baseRequest, stub, handler)

    assertEquals(it.map(_.responseId).toList, List("1", "2"))
    assertEquals(stub.executeCount, 1)
    assertEquals(stub.reattachRequests.size, 1)
    assertEquals(stub.reattachRequests.get(0).lastResponseId, Some("1"))
  }

  test("retries and reattaches after a transient mid-stream error") {
    val r1 = resp("1", complete = false)
    val flakyFirst = () =>
      new Iterator[proto.ExecutePlanResponse] {
        private var i = 0
        override def hasNext: Boolean = i match {
          case 0 => true
          case 1 => throw Status.UNAVAILABLE.asRuntimeException()
          case _ => false
        }
        override def next(): proto.ExecutePlanResponse = { i += 1; r1 }
      }
    val stub = new FakeStub(flakyFirst, () => Iterator(resp("2", complete = true)))
    val it = new ExecutePlanResponseReattachableIterator(baseRequest, stub, handler)

    assertEquals(it.map(_.responseId).toList, List("1", "2"))
    assert(stub.reattachRequests.size >= 1)
    assertEquals(stub.reattachRequests.get(0).lastResponseId, Some("1"))
  }

  test("issues a ReleaseExecute(ReleaseAll) once the result completes") {
    val stub = new FakeStub(
      first = () => Iterator(resp("1", complete = true)),
      second = () => Iterator.empty
    )
    val it = new ExecutePlanResponseReattachableIterator(baseRequest, stub, handler)
    it.toList

    // releaseExecute is fired asynchronously; allow a brief moment.
    val deadline = System.currentTimeMillis() + 2000
    while (stub.releaseRequests.isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(20)
    assert(
      stub.releaseRequests.asScala.exists(_.release.isReleaseAll),
      s"expected a ReleaseAll, got ${stub.releaseRequests.asScala.map(_.release).toList}"
    )
  }
}
