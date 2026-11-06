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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{
  ArrowSerializer,
  CloseableIterator,
  SparkConnectClient,
  SparkResult
}
import org.apache.spark.sql.types.StructType

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
class SparkSession private[sql] (private[sql] val client: SparkConnectClient)
    extends AutoCloseable {

  private[sql] val allocator: BufferAllocator = new RootAllocator()
  private val planIdGenerator = new AtomicLong(0L)

  // Observations awaiting their metrics, keyed by the plan id of their CollectMetrics relation.
  private val observationRegistry = new ConcurrentHashMap[Long, Observation]()

  // Whether close() releases the server-side session.
  private[sql] var releaseSessionOnClose: Boolean = true

  /** The client session id (a UUID). */
  def sessionId: String = client.sessionId

  /** Runtime configuration for Spark. */
  lazy val conf: RuntimeConfig = new RuntimeConfig(client)

  /** Client-side dispatcher backing `spark.streams.addListener(...)`. */
  private[sql] val streamingQueryListenerBus = new streaming.StreamingQueryListenerBus(this)

  /** Implicit conversions (`$"col"`, `Seq(...).toDF(...)`); use `import spark.implicits.*`. */
  object implicits extends SQLImplicits(this)

  // -- Plan plumbing (used by Dataset / functions) ---------------------------

  private[sql] def nextPlanId(): Long = planIdGenerator.getAndIncrement()

  /**
   * Wraps a `RelType` into a fresh [[proto.Relation]] tagged with a unique plan id.
   *
   * This is a low-level building block intended for Spark Connect plugin authors who need to
   * construct custom relations (for example, a relation handled by a server-side
   * `org.apache.spark.sql.connect.plugin.RelationPlugin`). It is a stable extension point but not
   * part of the day-to-day DataFrame API; ordinary code should use [[range]], [[sql]],
   * [[createDataFrame]], and friends instead.
   */
  def newRelation(relType: proto.Relation.RelType): proto.Relation =
    proto.Relation(
      common = Some(proto.RelationCommon(planId = Some(nextPlanId()))),
      relType = relType
    )

  /**
   * Like [[newRelation]], but when `cols` reference subqueries (`Dataset.scalar`, `Dataset.exists`,
   * `Column.isin(Dataset)`) the relation is wrapped in a `WithRelations` relation that carries the
   * referenced plans, as Apache Spark does.
   */
  private[sql] def newRelation(
      relType: proto.Relation.RelType,
      cols: Seq[Column]
  ): proto.Relation = {
    val root = newRelation(relType)
    val references = Column.mergeRelations(cols)
    if (references.isEmpty) root
    else
      newRelation(
        proto.Relation.RelType.WithRelations(
          proto.WithRelations(root = Some(root), references = references)
        )
      )
  }

  /**
   * Creates a [[DataFrame]] from an arbitrary `RelType`, tagging it with a unique plan id.
   *
   * This is the primary extension point for Spark Connect plugins: build any relation the server
   * understands -- including a custom `extension` relation -- and turn it into a [[DataFrame]]. For
   * the common case of a packed plugin message, prefer the
   * [[newDataFrame(extension:com\.google\.protobuf\.any\.Any)* extension overload]].
   */
  def newDataFrame(relType: proto.Relation.RelType): DataFrame =
    new Dataset(this, newRelation(relType), Encoder.rowEncoder)

  /** A [[DataFrame]] over `relType` whose expressions are `cols` (see [[newRelation]]). */
  private[sql] def newDataFrame(relType: proto.Relation.RelType, cols: Seq[Column]): DataFrame =
    new Dataset(this, newRelation(relType, cols), Encoder.rowEncoder)

  /** A typed [[Dataset]] over `relType` whose expressions are `cols` (see [[newRelation]]). */
  private[sql] def newDataset[T](
      relType: proto.Relation.RelType,
      encoder: Encoder[T],
      cols: Seq[Column] = Nil
  ): Dataset[T] =
    new Dataset(this, newRelation(relType, cols), encoder)

  /**
   * Creates a [[DataFrame]] backed by an `extension` relation carrying an already packed protobuf
   * message. This is the Scala counterpart of the PySpark client's
   * {{{plan.extension.Pack(message); DataFrame(plan, session)}}} and targets server-side
   * `org.apache.spark.sql.connect.plugin.RelationPlugin` implementations.
   *
   * {{{
   *   import com.google.protobuf.any.Any as ProtoAny
   *
   *   // `myPluginMessage` is a ScalaPB message generated from the plugin's own .proto.
   *   // Existing DataFrames can be embedded via their `relation` (see Dataset.relation):
   *   //   val payload = MyPlugin(input = Some(existingDf.relation))
   *   val payload: ProtoAny = ProtoAny.pack(myPluginMessage)
   *   val df = spark.newDataFrame(payload)
   * }}}
   */
  def newDataFrame(extension: com.google.protobuf.any.Any): DataFrame =
    newDataFrame(proto.Relation.RelType.Extension(extension))

  /**
   * Executes `plan`, handing observed metrics that belong to a registered [[Observation]] to it as
   * the responses stream in.
   */
  private[sql] def executeInternal(plan: proto.Plan): CloseableIterator[proto.ExecutePlanResponse] =
    client.execute(plan).mapClosable { response =>
      if (response.observedMetrics.nonEmpty) processObservedMetrics(response.observedMetrics)
      response
    }

  private[sql] def execute(plan: proto.Plan): SparkResult =
    new SparkResult(executeInternal(plan), allocator)

  /** Sends a command plan to the server and drains the response stream. */
  private[sql] def executeCommand(command: proto.Command): Unit = {
    val responses = executeInternal(proto.Plan(proto.Plan.OpType.Command(command)))
    try while (responses.hasNext) responses.next()
    finally responses.close()
  }

  /**
   * Sends a command that yields a relation (a SQL command or an external command) and returns a
   * [[DataFrame]] over the relation the server sends back.
   */
  private def executeCommandWithDataFrameReturn(command: proto.Command): DataFrame = {
    val responses = executeInternal(proto.Plan(proto.Plan.OpType.Command(command)))
    val collected =
      try responses.toSeq
      finally responses.close()
    val result = collected
      .flatMap(_.responseType.sqlCommandResult)
      .headOption
      .getOrElse(throw new IllegalStateException("SQLCommandResult must be present"))
    newDataFrame(result.getRelation.relType)
  }

  private[sql] def registerObservation(planId: Option[Long], observation: Observation): Unit =
    planId.foreach(id => observationRegistry.putIfAbsent(id, observation))

  private def processObservedMetrics(
      metrics: Seq[proto.ExecutePlanResponse.ObservedMetrics]
  ): Unit =
    metrics.foreach { metric =>
      // Servers before Spark 4.0 do not report the plan id; fall back to the observation name.
      val observation =
        if (metric.planId != 0L) Option(observationRegistry.remove(metric.planId))
        else {
          val byName = observationRegistry.asScala.collectFirst {
            case (id, o) if o.name == metric.name => id -> o
          }
          byName.foreach { case (id, _) => observationRegistry.remove(id) }
          byName.map(_._2)
        }
      observation.foreach(_.setMetricsAndNotify(Observation.transformObservedMetrics(metric)))
    }

  // -- Relation entry points -------------------------------------------------

  /** Creates a `Dataset` with a single `id` column of `[0, end)`. */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end, 1, None)

  /** Creates a `Dataset` with a single `id` column of `[start, end)`. */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = range(start, end, 1, None)

  /** Creates a `Dataset` with a single `id` column of `[start, end)` stepping by `step`. */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] =
    range(start, end, step, None)

  /** Creates a `Dataset` with a single `id` column of `[start, end)` in `numPartitions`. */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] =
    range(start, end, step, Some(numPartitions))

  private def range(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]
  ): Dataset[java.lang.Long] =
    newDataset(
      proto.Relation.RelType.Range(
        proto.Range(start = Some(start), end = end, step = step, numPartitions = numPartitions)
      ),
      Encoder.boxedLongEncoder
    )

  /**
   * Executes a SQL statement, returning its result as a [[DataFrame]]. Like Apache Spark, the
   * statement is sent to the server right away: commands (DDL / DML such as `CREATE TABLE` or
   * `INSERT`) run immediately, while queries stay lazy until an action.
   */
  def sql(sqlText: String): DataFrame = sql(sqlText, Array.empty[Any])

  /** Executes a SQL statement with positional parameters bound into the query. */
  def sql(sqlText: String, args: Array[?]): DataFrame = {
    val literals = args.toSeq.map(a => Column.lit(a))
    sqlCommand(
      sqlText,
      proto.SQL(query = sqlText, posArguments = literals.map(_.expr)),
      posArgs = literals,
      namedArgs = Map.empty
    )
  }

  /** Executes a SQL statement with named parameters bound into the query. */
  def sql(sqlText: String, args: Map[String, Any]): DataFrame = {
    val literals = args.map { case (k, v) => k -> Column.lit(v) }
    sqlCommand(
      sqlText,
      proto.SQL(query = sqlText, namedArguments = literals.map { case (k, c) => k -> c.expr }),
      posArgs = Nil,
      namedArgs = literals
    )
  }

  /** (Java-specific) Executes a SQL statement with named parameters bound into the query. */
  def sql(sqlText: String, args: java.util.Map[String, Any]): DataFrame =
    sql(sqlText, args.asScala.toMap)

  // Sends the statement once as a SqlCommand. Spark 4.0+ servers read the `input` relation;
  // Spark 3.5 servers only understand the legacy fields, which carry literal-only arguments.
  @nowarn("cat=deprecation")
  private def sqlCommand(
      sqlText: String,
      input: proto.SQL,
      posArgs: Seq[Column],
      namedArgs: Map[String, Column]
  ): DataFrame = {
    def literals(cols: Seq[Column]): Option[Seq[proto.Expression.Literal]] =
      if (cols.forall(_.expr.exprType.isLiteral)) Some(cols.map(_.expr.getLiteral)) else None
    val legacyPos = literals(posArgs).getOrElse(Nil)
    val legacyNamed = literals(namedArgs.values.toSeq)
      .map(namedArgs.keys.zip(_).toMap)
      .getOrElse(Map.empty)
    val command = proto.SqlCommand(
      sql = sqlText,
      args = legacyNamed,
      posArgs = legacyPos,
      input = Some(newRelation(proto.Relation.RelType.Sql(input), posArgs ++ namedArgs.values))
    )
    executeCommandWithDataFrameReturn(
      proto.Command(commandType = proto.Command.CommandType.SqlCommand(command))
    )
  }

  /**
   * Runs `command` in an external execution engine (`runner`, e.g. a JDBC data source) rather than
   * Spark, returning the command's output, if any, as a [[DataFrame]].
   */
  def executeCommand(runner: String, command: String, options: Map[String, String]): DataFrame =
    executeCommandWithDataFrameReturn(
      proto.Command(commandType =
        proto.Command.CommandType.ExecuteExternalCommand(
          proto.ExecuteExternalCommand(runner = runner, command = command, options = options)
        )
      )
    )

  /** Returns a [[DataFrame]] with no rows or columns. */
  def emptyDataFrame: DataFrame =
    newDataFrame(
      proto.Relation.RelType.LocalRelation(proto.LocalRelation(schema = Some("struct<>")))
    )

  /** Returns an empty `Dataset` of type `T`. */
  def emptyDataset[T](using enc: Encoder[T]): Dataset[T] =
    newDataset(
      proto.Relation.RelType.LocalRelation(proto.LocalRelation(schema = Some(enc.schema.toDDL))),
      enc
    )

  /** Creates a [[DataFrame]] from a local sequence of [[Row]]s using the given schema. */
  def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame = {
    val data = rows.map(_.toSeq)
    val bytes = ArrowSerializer.serialize(data, schema, allocator)
    val ddl = schema.simpleString.stripPrefix("struct<").stripSuffix(">")
    newDataFrame(
      proto.Relation.RelType.LocalRelation(
        proto.LocalRelation(data = Some(ByteString.copyFrom(bytes)), schema = Some(ddl))
      )
    )
  }

  /** Creates a [[DataFrame]] from a Java list of [[Row]]s using the given schema. */
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame =
    createDataFrame(rows.asScala.toSeq, schema)

  /** Creates a [[DataFrame]] from a local sequence of case classes or tuples. */
  def createDataFrame[A <: Product](data: Seq[A])(using enc: Encoder[A]): DataFrame =
    createDataset(data).toDF()

  /**
   * Creates a typed [[Dataset]] from a local sequence of `T` using its [[Encoder]]. The data is
   * serialized via the encoder and shipped as a local relation; no server-side closure is involved.
   */
  def createDataset[T](data: Seq[T])(using enc: Encoder[T]): Dataset[T] = {
    val rows = data.map(enc.toRow)
    createDataFrame(rows, enc.schema).as[T]
  }

  /** Creates a typed [[Dataset]] from a Java list of `T` using its [[Encoder]]. */
  def createDataset[T](data: java.util.List[T])(using enc: Encoder[T]): Dataset[T] =
    createDataset(data.asScala.toSeq)

  // -- Reading / writing / catalog / streaming -------------------------------

  /** Returns a [[DataFrameReader]] that can be used to read non-streaming data as a DataFrame. */
  def read: DataFrameReader = new DataFrameReader(this)

  /** Returns a [[org.apache.spark.sql.streaming.DataStreamReader]] for reading streaming data. */
  def readStream: streaming.DataStreamReader = new streaming.DataStreamReader(this)

  /** Returns a [[org.apache.spark.sql.streaming.StreamingQueryManager]] for this session. */
  lazy val streams: streaming.StreamingQueryManager = new streaming.StreamingQueryManager(this)

  /** Returns the [[org.apache.spark.sql.catalog.Catalog]] interface for this session. */
  lazy val catalog: org.apache.spark.sql.catalog.Catalog =
    new org.apache.spark.sql.catalog.Catalog(this)

  /** Returns the table-valued functions, e.g. `spark.tvf.explode(...)`. */
  def tvf: TableValuedFunction = new TableValuedFunction(this)

  /** Returns the named table or view as a [[DataFrame]]. */
  def table(tableName: String): DataFrame = read.table(tableName)

  /** The version of Spark on which the connected server is running. */
  def version: String = client.sparkVersion

  /**
   * Starts a new independent session against the same endpoint: a fresh server-side session with
   * its own configuration and temporary views.
   */
  def newSession(): SparkSession = new SparkSession(client.copy())

  /**
   * Clones this session on the server (Spark 4.1+): the new session starts with a copy of this
   * session's SQL configurations, temporary views, registered functions, and catalog state, and is
   * independent afterwards.
   */
  def cloneSession(): SparkSession = new SparkSession(client.cloneSession())

  /** Like [[cloneSession()]], with the given session id (a UUID) for the clone. */
  def cloneSession(sessionId: String): SparkSession =
    new SparkSession(client.cloneSession(Some(sessionId)))

  /**
   * Creates a new Spark Declarative Pipeline (a dataflow graph) in this session. Available on Spark
   * 4.1 and later servers.
   */
  def pipeline(
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      sqlConf: Map[String, String] = Map.empty
  ): pipelines.Pipeline =
    pipelines.Pipeline.create(this, defaultCatalog, defaultDatabase, sqlConf)

  // -- Operation tags & interruption ------------------------------------------

  /**
   * Adds a tag to every operation started by the current thread (and threads it creates later), so
   * they can be interrupted together with [[interruptTag]]. A tag must be non-empty and must not
   * contain `,`.
   */
  def addTag(tag: String): Unit = client.addTag(tag)

  /** Removes a tag previously added by [[addTag]] on the current thread. */
  def removeTag(tag: String): Unit = client.removeTag(tag)

  /** The tags the current thread attaches to the operations it starts. */
  def getTags(): Set[String] = client.getTags()

  /** Removes all of the current thread's operation tags. */
  def clearTags(): Unit = client.clearTags()

  /** Interrupts all operations of this session; returns the ids of interrupted operations. */
  def interruptAll(): Seq[String] = client.interruptAll()

  /** Interrupts the operations with the given tag; returns the ids of interrupted operations. */
  def interruptTag(tag: String): Seq[String] = client.interruptTag(tag)

  /** Interrupts the operation with the given id; returns the ids of interrupted operations. */
  def interruptOperation(operationId: String): Seq[String] =
    client.interruptOperation(operationId)

  // -- Misc ------------------------------------------------------------------

  /** Runs `f`, printing the time it took to the console, and returns its result. */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

  /** Runs `block` with this session as the active session, restoring the previous one after. */
  def withActive[T](block: => T): T = {
    val old = SparkSession.getActiveSession.orNull
    SparkSession.setActiveSession(this)
    try block
    finally SparkSession.setActiveSession(old)
  }

  /** Make this the active session for the current thread. */
  def setActive(): SparkSession = { SparkSession.setActiveSession(this); this }

  /** Releases the server-side session resources and closes the channel. */
  def stop(): Unit = close()

  override def close(): Unit = {
    try {
      streamingQueryListenerBus.shutdown()
      if (releaseSessionOnClose && !client.channel.isShutdown) {
        // Best effort: the server also expires abandoned sessions on its own.
        try client.releaseSession()
        catch { case NonFatal(_) => }
      }
      client.shutdown()
    } finally allocator.close()
    SparkSession.clearIfActive(this)
  }
}

object SparkSession {

  private val activeSession = new ThreadLocal[SparkSession]()
  @volatile private var defaultSession: Option[SparkSession] = None

  /** Creates a [[Builder]]; `SparkSession.builder()` (with parentheses) works as well. */
  def builder: Builder = new Builder()

  def setActiveSession(session: SparkSession): Unit = activeSession.set(session)
  def getActiveSession: Option[SparkSession] = Option(activeSession.get())
  def clearActiveSession(): Unit = activeSession.remove()

  def setDefaultSession(session: SparkSession): Unit = synchronized {
    defaultSession = Option(session)
  }
  def getDefaultSession: Option[SparkSession] = defaultSession
  def clearDefaultSession(): Unit = synchronized { defaultSession = None }

  /** The active session of the current thread, or else the default session. */
  def active: SparkSession =
    getActiveSession.getOrElse(
      getDefaultSession.getOrElse(
        throw new IllegalStateException("No active or default Spark session found")
      )
    )

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

    /** Returns this builder, so that `SparkSession.builder()` reads like Apache Spark. */
    def apply(): Builder = this

    /** Configures the connection from a Spark Connect connection string (`sc://...`). */
    def remote(connectionString: String): Builder = {
      clientBuilder.connectionString(connectionString)
      this
    }

    def appName(name: String): Builder = { appNameOpt = Some(name); this }

    /**
     * Has no effect over Spark Connect (the server decides the master); use [[remote]] to choose
     * the server. Kept for source compatibility with Apache Spark.
     */
    @deprecated("master does not work in Spark Connect, please use remote instead")
    def master(master: String): Builder = this

    /**
     * Has no effect over Spark Connect (the server decides its catalog implementation). Kept for
     * source compatibility with Apache Spark.
     */
    @deprecated("enableHiveSupport does not work in Spark Connect")
    def enableHiveSupport(): Builder = this

    def config(key: String, value: String): Builder = { options(key) = value; this }
    def config(key: String, value: Boolean): Builder = config(key, value.toString)
    def config(key: String, value: Long): Builder = config(key, value.toString)
    def config(key: String, value: Double): Builder = config(key, value.toString)

    /** Sets several configurations at once. */
    def config(map: Map[String, Any]): Builder = {
      map.foreach { case (k, v) => config(k, String.valueOf(v)) }
      this
    }

    /** (Java-specific) Sets several configurations at once. */
    def config(map: java.util.Map[String, Any]): Builder = config(map.asScala.toMap)

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
      synchronized(if (defaultSession.isEmpty) defaultSession = Some(session))
      session
    }
  }
}
