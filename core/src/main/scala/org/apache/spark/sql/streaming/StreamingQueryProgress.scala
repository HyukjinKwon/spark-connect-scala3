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
package org.apache.spark.sql.streaming

import org.apache.spark.connect.proto

/**
 * Reports information about the instantaneous status of a streaming query. The Spark Connect server
 * returns this as a structured result, so the fields are exposed directly. A best-effort JSON
 * rendering is also available via [[json]] / [[prettyJson]].
 */
class StreamingQueryStatus private[sql] (
    val message: String,
    val isDataAvailable: Boolean,
    val isTriggerActive: Boolean,
    val isActive: Boolean
) {

  /** The compact JSON representation of this status. */
  def json: String =
    s"""{"message":${StreamingQueryProgress.quote(message)},""" +
      s""""isDataAvailable":$isDataAvailable,""" +
      s""""isTriggerActive":$isTriggerActive}"""

  /** A pretty (multi-line) JSON representation of this status. */
  def prettyJson: String =
    s"""{
       |  "message" : ${StreamingQueryProgress.quote(message)},
       |  "isDataAvailable" : $isDataAvailable,
       |  "isTriggerActive" : $isTriggerActive
       |}""".stripMargin

  override def toString: String = prettyJson
}

private[sql] object StreamingQueryStatus {
  def apply(s: proto.StreamingQueryCommandResult.StatusResult): StreamingQueryStatus =
    new StreamingQueryStatus(
      message = s.statusMessage,
      isDataAvailable = s.isDataAvailable,
      isTriggerActive = s.isTriggerActive,
      isActive = s.isActive
    )
}

/**
 * Information about the progress made in the execution of a single [[StreamingQuery]] trigger.
 *
 * The Spark Connect server reports progress as a JSON string. To avoid pulling in a JSON parser
 * dependency, this wrapper preserves the raw JSON (accessible through [[json]]) and exposes a few
 * cheaply-extracted fields parsed directly from the JSON text.
 */
class StreamingQueryProgress private[sql] (val json: String) {

  /** The progress information rendered as a (pretty) JSON string. */
  def prettyJson: String = json

  /** The unique id of this query, if present in the progress JSON. */
  def id: Option[String] = StreamingQueryProgress.stringField(json, "id")

  /** The id of this run of the query, if present in the progress JSON. */
  def runId: Option[String] = StreamingQueryProgress.stringField(json, "runId")

  /** The user-specified name of the query, if any. */
  def name: Option[String] = StreamingQueryProgress.stringField(json, "name")

  /** The timestamp at which this trigger was kicked off, if present. */
  def timestamp: Option[String] = StreamingQueryProgress.stringField(json, "timestamp")

  /** The unique id of the batch this progress refers to, if present. */
  def batchId: Option[Long] = StreamingQueryProgress.longField(json, "batchId")

  override def toString: String = json
}

private[sql] object StreamingQueryProgress {

  def apply(json: String): StreamingQueryProgress = new StreamingQueryProgress(json)

  /** Quote and escape a string as a JSON string literal. */
  private[streaming] def quote(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '"' => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c => sb.append(c)
    }
    sb.append("\"").toString
  }

  /**
   * Best-effort extraction of a top-level string field from a flat JSON object. Returns `None` when
   * the field is absent or not a simple string value. This intentionally avoids a full JSON parser;
   * nested objects are not traversed.
   */
  private[streaming] def stringField(json: String, field: String): Option[String] = {
    val key = "\"" + field + "\""
    val keyIdx = json.indexOf(key)
    if (keyIdx < 0) return None
    var i = keyIdx + key.length
    // Skip whitespace and the colon.
    while (i < json.length && json(i).isWhitespace) i += 1
    if (i >= json.length || json(i) != ':') return None
    i += 1
    while (i < json.length && json(i).isWhitespace) i += 1
    if (i >= json.length || json(i) != '"') return None
    i += 1
    val sb = new StringBuilder
    while (i < json.length && json(i) != '"') {
      if (json(i) == '\\' && i + 1 < json.length) {
        i += 1
        json(i) match {
          case 'n' => sb.append('\n')
          case 'r' => sb.append('\r')
          case 't' => sb.append('\t')
          case other => sb.append(other)
        }
      } else {
        sb.append(json(i))
      }
      i += 1
    }
    Some(sb.toString)
  }

  /** Best-effort extraction of a top-level numeric field from a flat JSON object. */
  private[streaming] def longField(json: String, field: String): Option[Long] = {
    val key = "\"" + field + "\""
    val keyIdx = json.indexOf(key)
    if (keyIdx < 0) return None
    var i = keyIdx + key.length
    while (i < json.length && json(i).isWhitespace) i += 1
    if (i >= json.length || json(i) != ':') return None
    i += 1
    while (i < json.length && json(i).isWhitespace) i += 1
    val start = i
    while (i < json.length && (json(i).isDigit || json(i) == '-')) i += 1
    if (i == start) None else json.substring(start, i).toLongOption
  }
}
