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

import scala.collection.mutable

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession

/**
 * Client-side dispatcher for [[StreamingQueryListener]]s. When the first listener is registered it
 * opens a long-running `StreamingQueryListenerBusCommand` stream; a daemon thread reads the
 * serialized events the server forwards and invokes the registered listeners. The stream is closed
 * when the last listener is removed (or the session stops).
 */
private[sql] class StreamingQueryListenerBus(session: SparkSession) {

  private val listenerSet = mutable.LinkedHashSet.empty[StreamingQueryListener]
  @volatile private var busThread: Thread = null
  @volatile private var running = false

  def listeners: Array[StreamingQueryListener] = synchronized(listenerSet.toArray)

  def add(listener: StreamingQueryListener): Unit = synchronized {
    val first = listenerSet.isEmpty
    listenerSet.add(listener)
    if (first) start()
  }

  def remove(listener: StreamingQueryListener): Unit = synchronized {
    listenerSet.remove(listener)
    if (listenerSet.isEmpty) stop()
  }

  /** Stops the bus and clears listeners (called when the session closes). */
  def shutdown(): Unit = synchronized {
    if (listenerSet.nonEmpty || running) {
      listenerSet.clear()
      stop()
    }
  }

  private def start(): Unit = {
    running = true
    val responses = session.client.execute(busPlan(add = true))
    val t = new Thread(s"spark-connect-listener-bus-${session.sessionId}") {
      override def run(): Unit =
        try
          while (running && responses.hasNext) {
            val response = responses.next()
            response.responseType.streamingQueryListenerEventsResult.foreach { result =>
              result.events.foreach(dispatch)
            }
          }
        catch {
          case _: InterruptedException => // bus stopped
          case _: Throwable => // stream ended or was cancelled
        }
    }
    t.setDaemon(true)
    busThread = t
    t.start()
  }

  private def stop(): Unit = {
    running = false
    try {
      val responses = session.client.execute(busPlan(add = false))
      while (responses.hasNext) responses.next()
    } catch {
      case _: Throwable => // best effort
    }
    Option(busThread).foreach(_.interrupt())
    busThread = null
  }

  private def dispatch(event: proto.StreamingQueryListenerEvent): Unit = {
    val snapshot = synchronized(listenerSet.toArray)
    val json = event.eventJson
    event.eventType match {
      case proto.StreamingQueryEventType.QUERY_PROGRESS_EVENT =>
        snapshot.foreach(_.onQueryProgress(new StreamingQueryListener.QueryProgressEvent(json)))
      case proto.StreamingQueryEventType.QUERY_TERMINATED_EVENT =>
        snapshot.foreach(_.onQueryTerminated(new StreamingQueryListener.QueryTerminatedEvent(json)))
      case proto.StreamingQueryEventType.QUERY_IDLE_EVENT =>
        snapshot.foreach(_.onQueryIdle(new StreamingQueryListener.QueryIdleEvent(json)))
      case _ =>
        snapshot.foreach(_.onQueryStarted(new StreamingQueryListener.QueryStartedEvent(json)))
    }
  }

  private def busPlan(add: Boolean): proto.Plan = {
    val command =
      if (add) proto.StreamingQueryListenerBusCommand.Command.AddListenerBusListener(true)
      else proto.StreamingQueryListenerBusCommand.Command.RemoveListenerBusListener(true)
    proto.Plan(
      proto.Plan.OpType.Command(
        proto.Command(
          commandType = proto.Command.CommandType.StreamingQueryListenerBusCommand(
            proto.StreamingQueryListenerBusCommand(command = command)
          )
        )
      )
    )
  }
}
