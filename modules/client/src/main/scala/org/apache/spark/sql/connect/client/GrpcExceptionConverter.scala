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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Translates the `io.grpc.StatusRuntimeException`s thrown by the gRPC stub into the Spark exception
 * hierarchy ([[SparkException]] / [[AnalysisException]] / [[ParseException]]), so callers never see
 * raw gRPC types.
 *
 * This mirrors the design of the upstream Apache Spark Connect client: when the server attaches an
 * error id, we call the `FetchErrorDetails` RPC to retrieve the full server-side error chain (the
 * exception class hierarchy, message, stack trace, and nested causes) and rebuild a faithful
 * `Throwable` -- including the server stack trace and cause chain -- rather than surfacing only the
 * one-line gRPC status message. If the details RPC is unavailable, we fall back to the inline
 * `ErrorInfo` metadata and the error class embedded in the message (e.g.
 * `[TABLE_OR_VIEW_NOT_FOUND]`).
 *
 * Unlike upstream we do not depend on Spark's internal `SparkThrowable` / error-class machinery
 * (this is a thin client), so we map the server class hierarchy onto our small exception set and
 * expose the error class as a string.
 *
 * @param fetchErrorDetails
 *   resolves an error id to the server's structured error details, or `None` if unavailable. It
 *   must never itself throw a gRPC error back into this converter (no recursion).
 */
private[sql] class GrpcExceptionConverter(
    fetchErrorDetails: String => Option[proto.FetchErrorDetailsResponse]
) {

  /** Runs `f`, converting any gRPC error into a Spark exception. */
  def convert[T](f: => T): T =
    try f
    catch {
      case e: StatusRuntimeException => throw toSparkThrowable(e)
    }

  /** Wraps an iterator so errors raised while consuming it are converted too. */
  def convertIterator[T](iter: Iterator[T]): Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = convert(iter.hasNext)
    override def next(): T = convert(iter.next())
  }

  private def toSparkThrowable(e: StatusRuntimeException): Throwable =
    extractErrorId(e)
      .flatMap(fetchErrorDetails)
      .flatMap(buildFromDetails)
      .getOrElse(fallback(e))

  // ---------------------------------------------------------------------------
  // Preferred path: reconstruct from FetchErrorDetails (server stack + causes).
  // ---------------------------------------------------------------------------

  private def buildFromDetails(resp: proto.FetchErrorDetailsResponse): Option[Throwable] = {
    val errors = resp.errors
    val rootIdx = resp.rootErrorIdx.getOrElse(0)
    if (errors.isEmpty || rootIdx < 0 || rootIdx >= errors.size) None
    else
      try Some(buildError(errors, rootIdx, Set.empty))
      catch { case NonFatal(_) => None }
  }

  private def buildError(
      errors: Seq[proto.FetchErrorDetailsResponse.Error],
      idx: Int,
      visited: Set[Int]
  ): Throwable = {
    val err = errors(idx)
    val cause = err.causeIdx
      .filter(i => i >= 0 && i < errors.size && !visited.contains(i))
      .map(i => buildError(errors, i, visited + idx))
      .orNull
    val errorClass = err.sparkThrowable.flatMap(_.errorClass)
    val throwable = newException(err.errorTypeHierarchy, err.message, cause, errorClass)
    val stack = err.stackTrace.map { s =>
      new StackTraceElement(s.declaringClass, s.methodName, s.fileName.orNull, s.lineNumber)
    }.toArray
    if (stack.nonEmpty) throwable.setStackTrace(stack)
    throwable
  }

  // ---------------------------------------------------------------------------
  // Fallback path: inline ErrorInfo metadata + message parsing.
  // ---------------------------------------------------------------------------

  private def fallback(e: StatusRuntimeException): Throwable = {
    val hierarchy = serverExceptionClasses(e)
    val rawMessage = Option(e.getStatus.getDescription).getOrElse(e.getMessage)
    val message = stripStatusCodePrefix(rawMessage)
    val errorClass = extractErrorClass(message)
    val parseHint = errorClass.exists(_.startsWith("PARSE"))
    newException(hierarchy, message, e, errorClass, parseHint)
  }

  // ---------------------------------------------------------------------------
  // Mapping the server class hierarchy onto our exception set.
  // ---------------------------------------------------------------------------

  private def newException(
      hierarchy: Seq[String],
      message: String,
      cause: Throwable,
      errorClass: Option[String],
      parseHint: Boolean = false
  ): Throwable = {
    val isParse = parseHint || hierarchy.exists(_.endsWith("ParseException"))
    val isAnalysis = hierarchy.exists(c =>
      c.endsWith("AnalysisException") ||
        c.contains("NoSuchTable") || c.contains("NoSuchDatabase") ||
        c.contains("NoSuchNamespace") || c.contains("NoSuchView") ||
        c.contains("TableAlreadyExists") || c.contains("NamespaceAlreadyExists") ||
        c.contains("ViewAlreadyExists")
    )
    if (isParse) new ParseException(message, cause, errorClass)
    else if (isAnalysis) new AnalysisException(message, cause, errorClass)
    else new SparkException(message, cause, errorClass)
  }

  // ---------------------------------------------------------------------------
  // gRPC status detail helpers.
  // ---------------------------------------------------------------------------

  /** The server-attached error id, used to fetch full details. */
  private def extractErrorId(e: StatusRuntimeException): Option[String] =
    errorInfo(e).flatMap(info => Option(info.getMetadataMap.get("errorId"))).filter(_.nonEmpty)

  /** The server's exception class chain from the inline gRPC error details, if present. */
  private def serverExceptionClasses(e: StatusRuntimeException): Seq[String] =
    errorInfo(e).toSeq.flatMap { info =>
      Option(info.getMetadataMap.get("classes")).toSeq.flatMap(parseJsonStringArray)
    }

  private def errorInfo(e: StatusRuntimeException): Option[ErrorInfo] =
    try
      Option(StatusProto.fromThrowable(e)).flatMap { status =>
        status.getDetailsList.asScala.collectFirst {
          case any if any.is(classOf[ErrorInfo]) => any.unpack(classOf[ErrorInfo])
        }
      }
    catch { case NonFatal(_) => None }

  // Strips a leading gRPC status code, e.g. "INTERNAL: real message" -> "real message".
  private def stripStatusCodePrefix(message: String): String = {
    val idx = message.indexOf(": ")
    if (idx > 0 && message.take(idx).matches("[A-Z_]+")) message.substring(idx + 2) else message
  }

  // Extracts a leading "[ERROR_CLASS]" token from a Spark error message, if any.
  private def extractErrorClass(message: String): Option[String] =
    "^\\[([A-Z0-9_.]+)\\]".r.findFirstMatchIn(message).map(_.group(1))

  // Minimal parse of a JSON array of strings such as ["a.B","c.D"].
  private def parseJsonStringArray(json: String): Seq[String] =
    "\"([^\"]*)\"".r.findAllMatchIn(json).map(_.group(1)).toSeq
}
