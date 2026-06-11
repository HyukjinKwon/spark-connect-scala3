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

import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory}

import scala.util.control.NonFatal

import io.grpc.{Channel, StatusRuntimeException}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.GrpcRetryHandler.RetryException

/** An iterator that can be closed to release server-side resources. */
private[sql] trait CloseableIterator[+T] extends Iterator[T] {
  def close(): Unit
}

/**
 * The subset of Spark Connect RPCs the reattachable iterator needs. Extracted as a trait so the
 * reattach/retry logic can be tested deterministically with a fake.
 */
private[sql] trait ReattachableStub {
  def executePlan(request: proto.ExecutePlanRequest): Iterator[proto.ExecutePlanResponse]
  def reattachExecute(request: proto.ReattachExecuteRequest): Iterator[proto.ExecutePlanResponse]
  def releaseExecute(request: proto.ReleaseExecuteRequest): proto.ReleaseExecuteResponse
}

/** Real [[ReattachableStub]] backed by the raw blocking gRPC stub (no retry/conversion). */
private[sql] class GrpcReattachableStub(channel: Channel) extends ReattachableStub {
  private val stub = proto.SparkConnectServiceGrpc.blockingStub(channel)
  override def executePlan(r: proto.ExecutePlanRequest): Iterator[proto.ExecutePlanResponse] =
    stub.executePlan(r)
  override def reattachExecute(
      r: proto.ReattachExecuteRequest
  ): Iterator[proto.ExecutePlanResponse] =
    stub.reattachExecute(r)
  override def releaseExecute(r: proto.ReleaseExecuteRequest): proto.ReleaseExecuteResponse =
    stub.releaseExecute(r)
}

/**
 * Retryable iterator over the `ExecutePlanResponse` stream of an `ExecutePlan` call. Ported from
 * Apache Spark's `ExecutePlanResponseReattachableIterator`. It handles:
 *   - a stream broken by a retryable network error (governed by the retry policy), and
 *   - a stream gracefully ended by the server without a `ResultComplete` (meaning there is more, so
 *     the client reattaches via `ReattachExecute` from the last seen `responseId`).
 *
 * It also sends asynchronous `ReleaseExecute` RPCs so the server can free buffered responses.
 */
private[sql] class ExecutePlanResponseReattachableIterator(
    request: proto.ExecutePlanRequest,
    stub: ReattachableStub,
    retryHandler: GrpcRetryHandler
) extends CloseableIterator[proto.ExecutePlanResponse] {

  import ExecutePlanResponseReattachableIterator._

  private def retry[T](fn: => T): T = retryHandler.retry(fn)

  private val operationId: String = request.operationId.getOrElse(UUID.randomUUID.toString)

  private val initialRequest: proto.ExecutePlanRequest = request.copy(
    requestOptions = request.requestOptions :+ proto.ExecutePlanRequest.RequestOption(
      requestOption = proto.ExecutePlanRequest.RequestOption.RequestOption
        .ReattachOptions(proto.ReattachOptions(reattachable = true))
    ),
    operationId = Some(operationId)
  )

  // responseId of the last response returned by next().
  private var lastReturnedResponseId: Option[String] = None

  // True once the server's ResultComplete was seen (or release-all was issued).
  private[client] var resultComplete: Boolean = false

  // Current underlying iterator; unset briefly when a reattach is needed.
  private[client] var iter: Option[Iterator[proto.ExecutePlanResponse]] =
    Some(stub.executePlan(initialRequest))

  private var serverSideSessionId: Option[String] = None

  override def next(): proto.ExecutePlanResponse = synchronized {
    if (!hasNext) throw new java.util.NoSuchElementException()
    try {
      val ret = retry(callIter(_.next()))
      serverSideSessionId match {
        case Some(id) if id != ret.serverSideSessionId =>
          throw new IllegalStateException(
            s"Server side session ID changed. Create a new SparkSession to continue. " +
              s"(Old: $id, New: ${ret.serverSideSessionId})"
          )
        case Some(_) => // unchanged
        case None => serverSideSessionId = Some(ret.serverSideSessionId)
      }
      lastReturnedResponseId = Some(ret.responseId)
      if (ret.responseType.resultComplete.isDefined) releaseAll()
      else releaseUntil(ret.responseId)
      ret
    } catch {
      case NonFatal(ex) =>
        releaseAll()
        throw ex
    }
  }

  override def hasNext: Boolean = synchronized {
    if (resultComplete) return false
    try
      retry {
        var has = callIter(_.hasNext)
        // Graceful reattach: stream ended without ResultComplete -> reattach for more.
        if (!has && !resultComplete) {
          while (!has) {
            iter = None // force a ReattachExecute in callIter
            has = callIter(_.hasNext)
          }
        }
        has
      }
    catch {
      case NonFatal(ex) =>
        releaseAll()
        throw ex
    }
  }

  override def close(): Unit = releaseAll()

  /** Asks the server to release buffered responses up to and including `untilResponseId`. */
  private def releaseUntil(untilResponseId: String): Unit =
    if (!resultComplete) {
      submitRelease(createReleaseExecuteRequest(Some(untilResponseId)))
    }

  /** Asks the server to release the whole execution (all results consumed, or errored). */
  private def releaseAll(): Unit =
    if (!resultComplete) {
      submitRelease(createReleaseExecuteRequest(None))
      resultComplete = true
    }

  // Fire-and-forget the release on a shared daemon executor so iteration is not blocked.
  private def submitRelease(releaseRequest: proto.ReleaseExecuteRequest): Unit =
    releaseExecutor.execute { () =>
      try retry(stub.releaseExecute(releaseRequest))
      catch { case NonFatal(_) => /* best effort; server reclaims abandoned executions */ }
    }

  /**
   * Calls `iterFun` on the current iterator, reattaching first if needed. If the operation is not
   * found on the server (the initial ExecutePlan never arrived), retries with a fresh ExecutePlan.
   */
  private def callIter[V](iterFun: Iterator[proto.ExecutePlanResponse] => V): V =
    try {
      if (iter.isEmpty) {
        iter = Some(stub.reattachExecute(createReattachExecuteRequest()))
      }
      iterFun(iter.get)
    } catch {
      case ex: StatusRuntimeException if isOperationNotFound(ex) =>
        if (lastReturnedResponseId.isDefined) {
          throw new IllegalStateException(
            "OPERATION_NOT_FOUND/SESSION_NOT_FOUND on the server but responses were already " +
              "received from it.",
            ex
          )
        }
        iter = Some(stub.executePlan(initialRequest))
        val error = new RetryException()
        error.addSuppressed(ex)
        throw error
      case NonFatal(e) =>
        iter = None // force a reattach after the retry
        throw e
    }

  private def isOperationNotFound(ex: StatusRuntimeException): Boolean = {
    val msg = Option(ex.getStatus.getDescription).getOrElse(ex.getMessage)
    msg != null &&
    (msg.contains("INVALID_HANDLE.OPERATION_NOT_FOUND") ||
      msg.contains("INVALID_HANDLE.SESSION_NOT_FOUND"))
  }

  private def createReattachExecuteRequest(): proto.ReattachExecuteRequest =
    proto.ReattachExecuteRequest(
      sessionId = initialRequest.sessionId,
      userContext = initialRequest.userContext,
      operationId = operationId,
      clientType = initialRequest.clientType,
      lastResponseId = lastReturnedResponseId
    )

  private def createReleaseExecuteRequest(
      untilResponseId: Option[String]
  ): proto.ReleaseExecuteRequest = {
    val release = untilResponseId match {
      case None =>
        proto.ReleaseExecuteRequest.Release.ReleaseAll(proto.ReleaseExecuteRequest.ReleaseAll())
      case Some(responseId) =>
        proto.ReleaseExecuteRequest.Release.ReleaseUntil(
          proto.ReleaseExecuteRequest.ReleaseUntil(responseId = responseId)
        )
    }
    proto.ReleaseExecuteRequest(
      sessionId = initialRequest.sessionId,
      userContext = initialRequest.userContext,
      operationId = operationId,
      clientType = initialRequest.clientType,
      release = release
    )
  }
}

private[sql] object ExecutePlanResponseReattachableIterator {
  // Shared daemon executor for fire-and-forget ReleaseExecute RPCs across all queries.
  private val releaseExecutor =
    Executors.newCachedThreadPool(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "spark-connect-release-execute")
        t.setDaemon(true)
        t
      }
    })
}
