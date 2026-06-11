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

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicLong

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkResult}

/**
 * The entry point to programming Spark with the DataFrame API over Spark Connect.
 *
 * {{{
 *   val spark = SparkSession.builder
 *     .remote("sc://localhost:15002")
 *     .appName("my-app")
 *     .getOrCreate()
 *
 *   spark.range(10).filter(col("id") % 2 === 0).show()
 *   spark.stop()
 * }}}
 *
 * A session owns the [[SparkConnectClient]], a monotonic plan-id allocator (so every relation is
 * uniquely identifiable to the server), an Arrow allocator for decoding results, and the
 * [[RuntimeConfig]] facade.
 */
class SparkSession private[sql] (
    private[sql] val client: SparkConnectClient)
    extends AutoCloseable {

  private[sql] val allocator: BufferAllocator = new RootAllocator()
  private val planIdGenerator = new AtomicLong(0L)

  /** The client session id (a UUID). */
  def sessionId: String = client.sessionId

  /** Runtime configuration for Spark. */
  lazy val conf: RuntimeConfig = new RuntimeConfig(client)

  // -- Plan plumbing (used by Dataset / functions) ---------------------------

  private[sql] def nextPlanId(): Long = planIdGenerator.getAndIncrement()

  private[sql] def newRelation(relType: proto.Relation.RelType): proto.Relation =
    proto.Relation(
      common = Some(proto.RelationCommon(planId = Some(nextPlanId()))),
      relType = relType)

  private[sql] def newDataFrame(relType: proto.Relation.RelType): DataFrame =
    new Dataset(this, newRelation(relType))

  private[sql] def execute(plan: proto.Plan): SparkResult =
    new SparkResult(client.execute(plan), allocator)

  /** Sends a command plan to the server and drains the response stream. */
  private[sql] def executeCommand(command: proto.Command): Unit = {
    val plan = proto.Plan(proto.Plan.OpType.Command(command))
    val responses = client.execute(plan)
    while (responses.hasNext) responses.next()
  }

  // -- Relation entry points -------------------------------------------------

  /** Creates a [[DataFrame]] with a single `id` column of `[0, end)`. */
  def range(end: Long): DataFrame = range(0, end, 1, None)

  /** Creates a [[DataFrame]] with a single `id` column of `[start, end)`. */
  def range(start: Long, end: Long): DataFrame = range(start, end, 1, None)

  def range(start: Long, end: Long, step: Long): DataFrame = range(start, end, step, None)

  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame =
    range(start, end, step, Some(numPartitions))

  private def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): DataFrame =
    newDataFrame(
      proto.Relation.RelType.Range(
        proto.Range(
          start = Some(start),
          end = end,
          step = step,
          numPartitions = numPartitions)))

  /** Executes a SQL query and returns a lazy [[DataFrame]] over its result. */
  def sql(query: String): DataFrame =
    newDataFrame(proto.Relation.RelType.Sql(proto.SQL(query = query)))

  /** Executes a SQL query with positional parameters bound into the query. */
  def sql(query: String, args: Array[Any]): DataFrame =
    newDataFrame(
      proto.Relation.RelType.Sql(
        proto.SQL(query = query, posArguments = args.toSeq.map(a => Column.lit(a).expr))))

  /** Executes a SQL query with named parameters bound into the query. */
  def sql(query: String, args: Map[String, Any]): DataFrame =
    newDataFrame(
      proto.Relation.RelType.Sql(
        proto.SQL(
          query = query,
          namedArguments = args.map { case (k, v) => k -> Column.lit(v).expr })))

  /** Returns a [[DataFrame]] with no rows or columns. */
  def emptyDataFrame: DataFrame =
    newDataFrame(
      proto.Relation.RelType.LocalRelation(proto.LocalRelation(schema = Some("struct<>"))))

  /** The version of Spark on which the connected server is running. */
  def version: String = client.sparkVersion

  /** Make this the active session for the current thread. */
  def setActive(): SparkSession = { SparkSession.setActiveSession(this); this }

  /** Releases the server-side session resources and closes the channel. */
  def stop(): Unit = close()

  override def close(): Unit = {
    try client.shutdown()
    finally allocator.close()
    SparkSession.clearIfActive(this)
  }
}

object SparkSession {

  private val activeSession = new ThreadLocal[SparkSession]()
  @volatile private var defaultSession: Option[SparkSession] = None

  def builder: Builder = new Builder()

  def setActiveSession(session: SparkSession): Unit = activeSession.set(session)
  def getActiveSession: Option[SparkSession] = Option(activeSession.get())
  def getDefaultSession: Option[SparkSession] = defaultSession
  def clearActiveSession(): Unit = activeSession.remove()

  private def clearIfActive(session: SparkSession): Unit = {
    if (activeSession.get() eq session) activeSession.remove()
    synchronized {
      if (defaultSession.contains(session)) defaultSession = None
    }
  }

  /** Builder for [[SparkSession]]. */
  class Builder private[sql] () {
    private val clientBuilder = SparkConnectClient.builder()
    private val options = scala.collection.mutable.LinkedHashMap.empty[String, String]
    private var appNameOpt: Option[String] = None

    /** Configures the connection from a Spark Connect connection string (`sc://...`). */
    def remote(connectionString: String): Builder = {
      clientBuilder.connectionString(connectionString)
      this
    }

    def appName(name: String): Builder = { appNameOpt = Some(name); this }

    def config(key: String, value: String): Builder = { options(key) = value; this }
    def config(key: String, value: Boolean): Builder = config(key, value.toString)
    def config(key: String, value: Long): Builder = config(key, value.toString)
    def config(key: String, value: Double): Builder = config(key, value.toString)

    /** Sets the user agent reported to the server. */
    def userAgent(agent: String): Builder = { clientBuilder.userAgent(agent); this }

    /** Builds a brand-new session. */
    def create(): SparkSession = build()

    /** Returns the active session if present, otherwise builds and registers a new one. */
    def getOrCreate(): SparkSession =
      getActiveSession.getOrElse {
        val session = build()
        setActiveSession(session)
        session
      }

    private def build(): SparkSession = {
      appNameOpt.foreach(name => options.getOrElseUpdate("spark.app.name", name))
      val session = new SparkSession(clientBuilder.build())
      if (options.nonEmpty) {
        options.foreach { case (k, v) => session.conf.set(k, v) }
      }
      synchronized { if (defaultSession.isEmpty) defaultSession = Some(session) }
      session
    }
  }
}
