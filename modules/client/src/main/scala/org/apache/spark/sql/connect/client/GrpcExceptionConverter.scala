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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Translates the `io.grpc.StatusRuntimeException`s thrown by the gRPC stub into the Spark exception
 * hierarchy ([[SparkException]] / [[AnalysisException]] / [[ParseException]]), so callers never see
 * raw gRPC types. When the server attaches structured error info (the exception class chain), the
 * most specific matching Spark type is used; otherwise the error class embedded in the message
 * (e.g. `[TABLE_OR_VIEW_NOT_FOUND]`) is used as a hint.
 */
private[sql] object GrpcExceptionConverter {

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

  private def toSparkThrowable(e: StatusRuntimeException): Throwable = {
    val classes = serverExceptionClasses(e)
    val rawMessage = Option(e.getStatus.getDescription).getOrElse(e.getMessage)
    val message = stripStatusCodePrefix(rawMessage)
    val errorClass = extractErrorClass(message)

    val isParse = classes.exists(_.endsWith("ParseException")) ||
      errorClass.exists(_.startsWith("PARSE"))
    val isAnalysis = classes.exists(_.endsWith("AnalysisException")) ||
      classes.exists(c =>
        c.contains("NoSuchTable") || c.contains("NoSuchDatabase") ||
          c.contains("NoSuchNamespace") || c.contains("TableAlreadyExists")
      )

    if (isParse) new ParseException(message, e, errorClass)
    else if (isAnalysis) new AnalysisException(message, e, errorClass)
    else new SparkException(message, e, errorClass)
  }

  /** Reads the server's exception class chain from the gRPC error details, if present. */
  private def serverExceptionClasses(e: StatusRuntimeException): Seq[String] =
    try
      Option(StatusProto.fromThrowable(e)).toSeq.flatMap { status =>
        status.getDetailsList.asScala.flatMap { any =>
          if (any.is(classOf[ErrorInfo])) {
            val info = any.unpack(classOf[ErrorInfo])
            Option(info.getMetadataMap.get("classes")).toSeq.flatMap(parseJsonStringArray)
          } else {
            Seq.empty[String]
          }
        }.toSeq
      }
    catch {
      case NonFatal(_) => Seq.empty
    }

  // Strips a leading gRPC status code, e.g. "INTERNAL: real message" -> "real message".
  private def stripStatusCodePrefix(message: String): String = {
    val idx = message.indexOf(": ")
    if (idx > 0 && message.take(idx).matches("[A-Z_]+")) message.substring(idx + 2) else message
  }

  // Extracts a leading "[ERROR_CLASS]" token from a Spark error message, if any.
  private def extractErrorClass(message: String): Option[String] = {
    val m = "^\\[([A-Z0-9_.]+)\\]".r.findFirstMatchIn(message)
    m.map(_.group(1))
  }

  // Minimal parse of a JSON array of strings such as ["a.B","c.D"].
  private def parseJsonStringArray(json: String): Seq[String] =
    "\"([^\"]*)\"".r.findAllMatchIn(json).map(_.group(1)).toSeq
}
