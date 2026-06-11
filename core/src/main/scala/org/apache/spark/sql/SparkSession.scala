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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer
import org.apache.spark.sql.types.StructType

/**
 * The entry point to programming Spark with the Dataset and DataFrame API, over a Spark Connect
 * gRPC connection.
 *
 * {{{
 *   val spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .getOrCreate()
 *
 *   spark.range(10).filter("id % 2 == 0").show()
 * }}}
 *
 * A `SparkSession` is a thin client: every transformation builds a protobuf plan and the work
 * happens on the connected Spark Connect server.
 */
class SparkSession private[sql] (
    private[sql] val client: SparkConnectClient,
    private[sql] val planIdGenerator: AtomicLong
) extends AutoCloseable {

  /** Off-heap allocator shared by Arrow readers/writers created for this session. */
  private[sql] val allocator: BufferAllocator = new RootAllocator()

  /** The client-side session id. */
  private[sql] def sessionId: String = client.sessionId

  /** A fresh plan id for relations built in this session (used for `df("col")` resolution). */
  private[sql] def newPlanId(): Long = planIdGenerator.incrementAndGet()

  /** The Apache Spark version reported by the connected server. */
  lazy val version: String =
    client
      .analyze(proto.AnalyzePlanRequest().withSparkVersion(proto.AnalyzePlanRequest.SparkVersion()))
      .getSparkVersion
      .version

  /** Runtime configuration interface for Spark. */
  val conf: RuntimeConfig = new RuntimeConfig(client)

  private[sql] def timeZoneId: String =
    conf.getOption("spark.sql.session.timeZone").getOrElse("UTC")

  // ---------------------------------------------------------------------------
  // Plan execution plumbing (called by the dataset / results lanes)
  // ---------------------------------------------------------------------------

  /** Execute a plan and return the streamed responses. */
  private[sql] def execute(plan: proto.Plan): java.util.Iterator[proto.ExecutePlanResponse] =
    client.execute(plan)

  /** Execute a side-effecting command, consuming (and discarding) the response stream. */
  private[sql] def execute(command: proto.Command): Unit = {
    val responses = client.execute(proto.Plan().withCommand(command))
    while (responses.hasNext) responses.next()
  }

  /**
   * Run an `AnalyzePlan` request. Callers set only the `analyze` oneof on `request`; identity
   * fields (session id, user context, client type) are injected by the client.
   *
   * NOTE: ScalaPB messages are immutable case classes, so this takes a fully-built request rather
   * than a Java-protobuf `Builder` callback.
   */
  private[sql] def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse =
    client.analyze(request)

  /** Convenience: analyze from just the `analyze` oneof. */
  private[sql] def analyze(analyze: proto.AnalyzePlanRequest.Analyze): proto.AnalyzePlanResponse =
    client.analyze(analyze)

  /** Wrap a relation in a [[DataFrame]], assigning it a fresh plan id. */
  private[sql] def newDataFrame(relation: proto.Relation): DataFrame =
    Dataset.ofRows(this, withPlanId(relation))

  private def withPlanId(relation: proto.Relation): proto.Relation =
    relation.update(_.common.planId := newPlanId())

  // ---------------------------------------------------------------------------
  // Entry points for building DataFrames
  // ---------------------------------------------------------------------------

  /** Returns a [[DataFrame]] with no rows or columns. */
  def emptyDataFrame: DataFrame =
    createDataFrame(Seq.empty[Row], StructType(Array.empty[org.apache.spark.sql.types.StructField]))

  /** Executes a SQL query, returning the result as a [[DataFrame]]. */
  def sql(query: String): DataFrame = sql(query, Map.empty[String, Any])

  /** Executes a SQL query with named parameters. */
  def sql(query: String, args: Map[String, Any]): DataFrame = {
    val sqlRelation = proto
      .Relation()
      .withSql(
        proto.SQL(
          query = query,
          namedArguments = args.map { case (k, v) => k -> functions.lit(v).expr }
        )
      )
    runSqlCommand(sqlRelation)
  }

  /** Executes a SQL query with positional parameters. */
  def sql(query: String, args: Array[Any]): DataFrame = {
    val sqlRelation = proto
      .Relation()
      .withSql(proto.SQL(query = query, posArguments = args.toSeq.map(v => functions.lit(v).expr)))
    runSqlCommand(sqlRelation)
  }

  /**
   * SQL (including DDL/DML) is run eagerly on the server via a `SqlCommand`; the resulting relation
   * is wrapped as the returned DataFrame.
   */
  private def runSqlCommand(sqlRelation: proto.Relation): DataFrame = {
    val command = proto.Command().withSqlCommand(proto.SqlCommand(input = Some(sqlRelation)))
    val responses = client.execute(proto.Plan().withCommand(command))
    try {
      val it = responses.asScala
      val resultRelation = it
        .find(_.responseType.isSqlCommandResult)
        .map(_.getSqlCommandResult.getRelation)
        .getOrElse(sqlRelation)
      newDataFrame(resultRelation)
    } finally while (responses.hasNext) responses.next()
  }

  /** Creates a [[DataFrame]] from a range of numbers `[0, end)`. */
  def range(end: Long): DataFrame = range(0, end, 1, None)

  /** Creates a [[DataFrame]] from a range of numbers `[start, end)`. */
  def range(start: Long, end: Long): DataFrame = range(start, end, 1, None)

  /** Creates a [[DataFrame]] from a range of numbers `[start, end)` with the given step. */
  def range(start: Long, end: Long, step: Long): DataFrame = range(start, end, step, None)

  /** Creates a [[DataFrame]] from a range with the given step and number of partitions. */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame =
    range(start, end, step, Some(numPartitions))

  private def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): DataFrame =
    newDataFrame(
      proto
        .Relation()
        .withRange(
          proto.Range(start = Some(start), end = end, step = step, numPartitions = numPartitions)
        )
    )

  /** Returns the specified table/view as a [[DataFrame]]. */
  def table(tableName: String): DataFrame = read.table(tableName)

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a DataFrame.
   */
  def read: DataFrameReader = new DataFrameReader(this)

  /** Creates a [[DataFrame]] from a list of [[Row]]s with the given schema. */
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame =
    createDataFrame(rows.asScala.toSeq, schema)

  /** Creates a [[DataFrame]] from a sequence of [[Row]]s with the given schema. */
  def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame = {
    val local =
      if (rows.isEmpty) {
        proto.LocalRelation(schema = Some(schema.json))
      } else {
        val bytes = ArrowSerializer.serialize(rows, schema, allocator, timeZoneId)
        proto.LocalRelation(data = Some(ByteString.copyFrom(bytes)), schema = Some(schema.json))
      }
    newDataFrame(proto.Relation().withLocalRelation(local))
  }

  /**
   * Builds a [[DataFrame]] from a local sequence of values, tuples, or case classes, inferring the
   * column types from the first row. Backs `Seq(...).toDF(...)` (see [[implicits]]).
   */
  private[sql] def createDataFrameFromLocalSeq(data: Seq[?], colNames: Seq[String]): DataFrame = {
    import org.apache.spark.sql.types._
    val rows = data.map {
      case row: Row => row
      case p: Product => Row.fromSeq(p.productIterator.toIndexedSeq)
      case v => Row(v)
    }
    val arity = rows.headOption.map(_.length).getOrElse(colNames.length)
    val names = if (colNames.nonEmpty) colNames else (1 to arity).map(i => s"_$i")
    val first = rows.headOption
    val fields = names.zipWithIndex.map { case (name, i) =>
      val dt = first.map(r => SparkSession.inferDataType(r.get(i))).getOrElse(StringType)
      StructField(name, dt, nullable = true)
    }
    createDataFrame(rows, StructType(fields.toArray))
  }

  /** Implicit conversions (`$"col"`, `Seq(...).toDF(...)`); use `import spark.implicits.*`. */
  object implicits extends SQLImplicits(this)

  /** Interface through which the user may access the catalog. */
  lazy val catalog: Catalog = new Catalog(this)

  // ---------------------------------------------------------------------------
  // Interruption / tags
  // ---------------------------------------------------------------------------

  def interruptAll(): Seq[String] = client.interruptAll().interruptedIds
  def interruptTag(tag: String): Seq[String] = client.interruptTag(tag).interruptedIds
  def interruptOperation(operationId: String): Seq[String] =
    client.interruptOperation(operationId).interruptedIds

  def addTag(tag: String): Unit = client.addTag(tag)
  def removeTag(tag: String): Unit = client.removeTag(tag)
  def getTags: Set[String] = client.getTags
  def clearTags(): Unit = client.clearTags()

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  def isUsable: Boolean = client.isSessionValid

  /** Synonym for [[close]]. */
  def stop(): Unit = close()

  override def close(): Unit = {
    try client.releaseSession()
    catch { case scala.util.control.NonFatal(_) => }
    try client.shutdown()
    catch { case scala.util.control.NonFatal(_) => }
    allocator.close()
    SparkSession.clearDefaultSession(this)
  }
}

object SparkSession {

  private val defaultSession = new AtomicReference[SparkSession]()

  /** Creates a [[Builder]] for constructing a [[SparkSession]]. */
  def builder(): Builder = new Builder()

  /** Returns the default session set with [[Builder.getOrCreate]], if any. */
  def getDefaultSession: Option[SparkSession] = Option(defaultSession.get())

  private[sql] def setDefaultSession(session: SparkSession): Unit =
    defaultSession.compareAndSet(null, session)

  private[sql] def clearDefaultSession(session: SparkSession): Unit =
    defaultSession.compareAndSet(session, null)

  /** Best-effort schema inference from a runtime value, used by `Seq(...).toDF(...)`. */
  private[sql] def inferDataType(value: Any): org.apache.spark.sql.types.DataType = {
    import org.apache.spark.sql.types._
    value match {
      case null => StringType
      case _: Boolean => BooleanType
      case _: Byte => ByteType
      case _: Short => ShortType
      case _: Int => IntegerType
      case _: Long => LongType
      case _: Float => FloatType
      case _: Double => DoubleType
      case _: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
      case _: BigDecimal => DecimalType.SYSTEM_DEFAULT
      case _: Array[Byte] => BinaryType
      case _: String => StringType
      case _: java.sql.Date => DateType
      case _: java.time.LocalDate => DateType
      case _: java.sql.Timestamp => TimestampType
      case _: java.time.Instant => TimestampType
      case _ => StringType
    }
  }

  class Builder {
    private var connectionString: Option[String] = None
    private var prebuiltClient: Option[SparkConnectClient] = None
    private val options = scala.collection.mutable.LinkedHashMap.empty[String, String]

    /** Sets the Spark Connect server to connect to, e.g. `sc://localhost:15002`. */
    def remote(connectionString: String): this.type = {
      this.connectionString = Some(connectionString)
      this
    }

    /** Use a pre-built [[SparkConnectClient]] (advanced / testing). */
    def client(client: SparkConnectClient): this.type = {
      this.prebuiltClient = Some(client)
      this
    }

    def config(key: String, value: String): this.type = { options(key) = value; this }
    def config(key: String, value: Boolean): this.type = config(key, value.toString)
    def config(key: String, value: Long): this.type = config(key, value.toString)
    def config(key: String, value: Double): this.type = config(key, value.toString)

    private def buildClient(): SparkConnectClient =
      prebuiltClient.getOrElse {
        val builder = SparkConnectClient.builder().loadFromEnvironment()
        connectionString.foreach(builder.connectionString)
        builder.build()
      }

    private def newSession(): SparkSession = {
      val session = new SparkSession(buildClient(), new AtomicLong(0L))
      options.foreach { case (k, v) =>
        try session.conf.set(k, v)
        catch { case scala.util.control.NonFatal(_) => }
      }
      session
    }

    /** Always creates a new [[SparkSession]]. */
    def create(): SparkSession = {
      val session = newSession()
      setDefaultSession(session)
      session
    }

    /** Alias for [[create]] (deprecated in upstream; kept for familiarity). */
    def build(): SparkSession = create()

    /** Returns the default session if one exists, otherwise creates a new one. */
    def getOrCreate(): SparkSession =
      getDefaultSession.filter(_.isUsable).getOrElse(create())
  }
}
