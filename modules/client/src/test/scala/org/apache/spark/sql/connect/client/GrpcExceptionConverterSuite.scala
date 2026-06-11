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

import com.google.protobuf.{Any => ProtoAny}
import com.google.rpc.{Code, ErrorInfo, Status => RpcStatus}
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Unit tests for [[GrpcExceptionConverter]] -- no server required. Builds `StatusRuntimeException`s
 * with the same error details a Spark Connect server attaches, and a fake `FetchErrorDetails`
 * fetcher, then asserts the reconstructed Spark exception.
 */
class GrpcExceptionConverterSuite extends munit.FunSuite {

  /** Builds a StatusRuntimeException carrying an ErrorInfo, like the server would. */
  private def statusError(
      message: String,
      errorId: Option[String] = None,
      classes: Option[String] = None
  ): StatusRuntimeException = {
    val info = ErrorInfo.newBuilder().setReason("INTERNAL_ERROR")
    errorId.foreach(id => info.putMetadata("errorId", id))
    classes.foreach(c => info.putMetadata("classes", c))
    val status = RpcStatus
      .newBuilder()
      .setCode(Code.INTERNAL.getNumber)
      .setMessage(message)
      .addDetails(ProtoAny.pack(info.build()))
      .build()
    StatusProto.toStatusRuntimeException(status)
  }

  private def noDetails: String => Option[proto.FetchErrorDetailsResponse] = _ => None

  private def intercepted(conv: GrpcExceptionConverter, e: StatusRuntimeException): Throwable =
    intercept[Throwable](conv.convert[Unit](throw e))

  test("fallback: analysis error class -> AnalysisException with stripped message") {
    val conv = new GrpcExceptionConverter(noDetails)
    val e = statusError(
      "INTERNAL: [TABLE_OR_VIEW_NOT_FOUND] The table or view `foo` cannot be found.",
      classes = Some("[\"org.apache.spark.sql.AnalysisException\"]")
    )
    val t = intercepted(conv, e)
    assert(t.isInstanceOf[AnalysisException], s"expected AnalysisException, got ${t.getClass}")
    assert(!t.getMessage.startsWith("INTERNAL"), s"status code leaked: ${t.getMessage}")
    assert(t.getMessage.startsWith("[TABLE_OR_VIEW_NOT_FOUND]"), t.getMessage)
    assertEquals(t.asInstanceOf[AnalysisException].getErrorClass, "TABLE_OR_VIEW_NOT_FOUND")
  }

  test("fallback: PARSE error class -> ParseException") {
    val conv = new GrpcExceptionConverter(noDetails)
    val e = statusError("INTERNAL: [PARSE_SYNTAX_ERROR] Syntax error at or near 'slect'.")
    val t = intercepted(conv, e)
    assert(t.isInstanceOf[ParseException], s"expected ParseException, got ${t.getClass}")
  }

  test("fallback: unknown error -> plain SparkException") {
    val conv = new GrpcExceptionConverter(noDetails)
    val t = intercepted(conv, statusError("INTERNAL: something broke"))
    assertEquals(t.getClass.getName, classOf[SparkException].getName)
    assertEquals(t.getMessage, "something broke")
  }

  test("FetchErrorDetails: rebuilds type, message, server stack trace, and cause chain") {
    val response = proto.FetchErrorDetailsResponse(
      rootErrorIdx = Some(0),
      errors = Seq(
        proto.FetchErrorDetailsResponse.Error(
          errorTypeHierarchy =
            Seq("org.apache.spark.sql.AnalysisException", "org.apache.spark.SparkException"),
          message = "Table or view not found: foo",
          stackTrace = Seq(
            proto.FetchErrorDetailsResponse.StackTraceElement(
              declaringClass = "org.apache.spark.sql.catalyst.analysis.CheckAnalysis",
              methodName = "failAnalysis",
              fileName = Some("CheckAnalysis.scala"),
              lineNumber = 42
            )
          ),
          causeIdx = Some(1),
          sparkThrowable = Some(
            proto.FetchErrorDetailsResponse
              .SparkThrowable(errorClass = Some("TABLE_OR_VIEW_NOT_FOUND"))
          )
        ),
        proto.FetchErrorDetailsResponse.Error(
          errorTypeHierarchy = Seq("java.lang.RuntimeException"),
          message = "the underlying cause"
        )
      )
    )

    val conv = new GrpcExceptionConverter(_ => Some(response))
    val t = intercepted(conv, statusError("INTERNAL: ignored", errorId = Some("err-1")))

    assert(t.isInstanceOf[AnalysisException], s"expected AnalysisException, got ${t.getClass}")
    assertEquals(t.getMessage, "Table or view not found: foo")
    assertEquals(t.asInstanceOf[AnalysisException].getErrorClass, "TABLE_OR_VIEW_NOT_FOUND")
    val top = t.getStackTrace.head
    assertEquals(top.getClassName, "org.apache.spark.sql.catalyst.analysis.CheckAnalysis")
    assertEquals(top.getMethodName, "failAnalysis")
    assertEquals(top.getLineNumber, 42)
    assert(t.getCause != null, "expected a reconstructed cause")
    assertEquals(t.getCause.getMessage, "the underlying cause")
  }

  test("FetchErrorDetails: a cyclic cause chain terminates") {
    val response = proto.FetchErrorDetailsResponse(
      rootErrorIdx = Some(0),
      errors = Seq(
        proto.FetchErrorDetailsResponse
          .Error(message = "a", causeIdx = Some(1)),
        proto.FetchErrorDetailsResponse
          .Error(message = "b", causeIdx = Some(0))
      )
    ) // points back to 0
    val conv = new GrpcExceptionConverter(_ => Some(response))
    val t = intercepted(conv, statusError("INTERNAL: ignored", errorId = Some("err-2")))
    assertEquals(t.getMessage, "a")
    assertEquals(t.getCause.getMessage, "b")
    assert(t.getCause.getCause == null, "cycle must be broken")
  }

  test("falls back when the details fetch yields nothing") {
    // errorId present, but the fetcher returns None (e.g. RPC unavailable): use the inline path.
    val conv = new GrpcExceptionConverter(_ => None)
    val e = statusError(
      "INTERNAL: [SCHEMA_NOT_FOUND] The schema `s` cannot be found.",
      errorId = Some("err-3"),
      classes = Some("[\"org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException\"]")
    )
    val t = intercepted(conv, e)
    assert(t.isInstanceOf[AnalysisException], s"expected AnalysisException, got ${t.getClass}")
  }

  test("convertIterator converts errors raised during iteration") {
    val conv = new GrpcExceptionConverter(noDetails)
    val failing = new Iterator[Int] {
      override def hasNext: Boolean = true
      override def next(): Int = throw statusError("INTERNAL: boom while streaming")
    }
    val wrapped = conv.convertIterator(failing)
    val t = intercept[SparkException](wrapped.next())
    assertEquals(t.getMessage, "boom while streaming")
  }
}
