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

/**
 * Interface for listening to streaming query lifecycle events. Register an implementation with
 * `spark.streams.addListener(...)`.
 *
 * Over Spark Connect the callbacks run on the client: the server forwards serialized events on a
 * dedicated stream and this client dispatches them to the registered listeners. No user code runs
 * on the server, so listeners (unlike `foreach`/`foreachBatch`) are fully supported.
 *
 * {{{
 *   spark.streams.addListener(new StreamingQueryListener {
 *     def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = println(s"started ${e.id}")
 *     def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = println(e.json)
 *     def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = println(s"done ${e.id}")
 *   })
 * }}}
 */
abstract class StreamingQueryListener extends Serializable {

  /** Called when a query is started. */
  def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit

  /** Called when there is some status update (e.g. ingestion rate). */
  def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit

  /** Called when the query has no data to process (overridable; default no-op). */
  def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {}

  /** Called when a query is stopped, normally or with an error. */
  def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit
}

object StreamingQueryListener {

  /** Base type of all streaming-query listener events. Carries the raw event JSON. */
  sealed trait Event extends Serializable {

    /** The full event payload as serialized by the server (Spark's event JSON). */
    def json: String
  }

  /** Event fired when a query starts. */
  final class QueryStartedEvent private[sql] (val json: String) extends Event {
    def id: String = Event.field(json, "id")
    def runId: String = Event.field(json, "runId")
    def name: String = Event.field(json, "name")
    def timestamp: String = Event.field(json, "timestamp")
    override def toString: String = s"QueryStartedEvent(id=$id, runId=$runId, name=$name)"
  }

  /** Event fired on a status update; `json` is Spark's `StreamingQueryProgress` JSON. */
  final class QueryProgressEvent private[sql] (val json: String) extends Event {
    override def toString: String = s"QueryProgressEvent($json)"
  }

  /** Event fired when a query has no data to process. */
  final class QueryIdleEvent private[sql] (val json: String) extends Event {
    def id: String = Event.field(json, "id")
    def runId: String = Event.field(json, "runId")
    override def toString: String = s"QueryIdleEvent(id=$id, runId=$runId)"
  }

  /** Event fired when a query stops, with an optional exception message. */
  final class QueryTerminatedEvent private[sql] (val json: String) extends Event {
    def id: String = Event.field(json, "id")
    def runId: String = Event.field(json, "runId")
    def exception: Option[String] = Event.optField(json, "exception")
    override def toString: String =
      s"QueryTerminatedEvent(id=$id, runId=$runId, exception=$exception)"
  }

  private object Event {
    // Minimal extraction of a top-level JSON string field; avoids a JSON dependency. The full,
    // structured payload is always available via `event.json`.
    private def regex(key: String) = ("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"").r
    def optField(json: String, key: String): Option[String] =
      regex(key).findFirstMatchIn(json).map(_.group(1))
    def field(json: String, key: String): String = optField(json, key).getOrElse("")
  }
}
