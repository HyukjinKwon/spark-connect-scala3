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
package org.apache.spark.sql.pipelines

import org.apache.spark.connect.proto
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.client.{DataTypeProtoConverter, SparkConnectException}
import org.apache.spark.sql.types.DataType

/** A timestamped event emitted by the server during a pipeline run. */
final case class PipelineEvent(timestampMs: Long, message: String)

/**
 * A Spark Declarative Pipeline (SDP) dataflow graph.
 *
 * A pipeline is built by registering outputs (tables, materialized views, temporary views, or
 * sinks) and the flows that populate them, then started with [[startRun]]. Each flow is defined by
 * a [[org.apache.spark.sql.DataFrame]] (an unresolved relation), so flows are composed with the
 * same API used for ordinary queries.
 *
 * Create one with [[org.apache.spark.sql.SparkSession.pipeline]].
 *
 * {{{
 *   val pipe = spark.pipeline()
 *   pipe.createMaterializedView("bronze", spark.read.json("/data/raw"))
 *   pipe.createTable("silver", pipe.read("bronze").filter($"ok"))
 *   val events = pipe.startRun()
 * }}}
 *
 * `foreach`/`foreachBatch` flows and Python/Scala query-function evaluation are not supported (they
 * require user-defined functions); define each flow with a relation instead.
 */
class Pipeline private[sql] (
    sparkSession: SparkSession,
    defaultCatalog: Option[String],
    defaultDatabase: Option[String],
    sqlConf: Map[String, String]
) {

  /** The server-assigned dataflow graph id. */
  val graphId: String = {
    val cmd = proto.PipelineCommand.CreateDataflowGraph(
      defaultCatalog = defaultCatalog,
      defaultDatabase = defaultDatabase,
      sqlConf = sqlConf
    )
    dispatch(proto.PipelineCommand().withCreateDataflowGraph(cmd))
      .flatMap(_.resultType.createDataflowGraphResult)
      .flatMap(_.dataflowGraphId)
      .getOrElse(throw new SparkConnectException("Server did not return a dataflow graph id"))
  }

  /**
   * Reference a dataset defined in this pipeline as a [[DataFrame]] (read from earlier outputs).
   */
  def read(name: String): DataFrame = sparkSession.read.table(name)

  /** Defines a published table and (optionally) the flow that populates it. */
  def createTable(
      name: String,
      query: Dataset[?] = null,
      comment: String = null,
      format: String = null,
      partitionCols: Seq[String] = Nil,
      clusteringColumns: Seq[String] = Nil,
      tableProperties: Map[String, String] = Map.empty,
      schema: DataType = null
  ): String =
    defineTableOutput(
      name,
      proto.OutputType.TABLE,
      Option(query),
      Option(comment),
      Option(format),
      partitionCols,
      clusteringColumns,
      tableProperties,
      Option(schema)
    )

  /** Defines a materialized view and (optionally) the flow that populates it. */
  def createMaterializedView(
      name: String,
      query: Dataset[?] = null,
      comment: String = null,
      format: String = null,
      partitionCols: Seq[String] = Nil,
      clusteringColumns: Seq[String] = Nil,
      tableProperties: Map[String, String] = Map.empty,
      schema: DataType = null
  ): String =
    defineTableOutput(
      name,
      proto.OutputType.MATERIALIZED_VIEW,
      Option(query),
      Option(comment),
      Option(format),
      partitionCols,
      clusteringColumns,
      tableProperties,
      Option(schema)
    )

  /** Defines a (non-published) temporary view and its flow. */
  def createTemporaryView(name: String, query: Dataset[?] = null, comment: String = null): String =
    defineTableOutput(
      name,
      proto.OutputType.TEMPORARY_VIEW,
      Option(query),
      Option(comment),
      None,
      Nil,
      Nil,
      Map.empty,
      None
    )

  /** Defines a streaming sink and the flow that feeds it. */
  def createSink(
      name: String,
      query: Dataset[?],
      format: String = null,
      options: Map[String, String] = Map.empty
  ): String = {
    val sink =
      proto.PipelineCommand.DefineOutput.SinkDetails(options = options, format = Option(format))
    defineOutput(name, proto.OutputType.SINK, None, _.withSinkDetails(sink))
    defineFlow(name, query, target = name)
    name
  }

  /** Defines a flow that writes the contents of `query` into `target` (defaults to `name`). */
  def defineFlow(
      name: String,
      query: Dataset[?],
      target: String = null,
      once: Boolean = false,
      sqlConf: Map[String, String] = Map.empty
  ): String = {
    var flow = proto.PipelineCommand
      .DefineFlow(
        dataflowGraphId = Some(graphId),
        flowName = Some(name),
        targetDatasetName = Some(Option(target).getOrElse(name)),
        sqlConf = sqlConf
      )
      .withRelationFlowDetails(
        proto.PipelineCommand.DefineFlow.WriteRelationFlowDetails(relation = Some(query.relation))
      )
    if (once) flow = flow.copy(once = Some(true))
    dispatch(proto.PipelineCommand().withDefineFlow(flow))
      .flatMap(_.resultType.defineFlowResult)
      .flatMap(_.resolvedIdentifier)
      .map(identifierString)
      .getOrElse(name)
  }

  /** Registers datasets and flows from a SQL definition. */
  def defineSql(sqlText: String, sqlFilePath: String = null): Unit = {
    val el = proto.PipelineCommand.DefineSqlGraphElements(
      dataflowGraphId = Some(graphId),
      sqlText = Some(sqlText),
      sqlFilePath = Option(sqlFilePath)
    )
    dispatch(proto.PipelineCommand().withDefineSqlGraphElements(el))
  }

  /**
   * Resolves the graph and runs a pipeline update. Blocks until the update completes, returning the
   * events emitted during the run.
   */
  def startRun(
      fullRefresh: Seq[String] = Nil,
      fullRefreshAll: Boolean = false,
      refresh: Seq[String] = Nil,
      dry: Boolean = false,
      storage: String = null
  ): Seq[PipelineEvent] = {
    val run = proto.PipelineCommand.StartRun(
      dataflowGraphId = Some(graphId),
      fullRefreshSelection = fullRefresh,
      fullRefreshAll = Some(fullRefreshAll),
      refreshSelection = refresh,
      dry = Some(dry),
      storage = Option(storage)
    )
    sparkSession
      .executeCommandResponses(
        proto.Command().withPipelineCommand(proto.PipelineCommand().withStartRun(run))
      )
      .flatMap(_.responseType.pipelineEventResult)
      .flatMap(_.event)
      .map { e =>
        val ts = e.timestamp.map(t => t.seconds * 1000L + t.nanos / 1000000L).getOrElse(0L)
        PipelineEvent(ts, e.message.getOrElse(""))
      }
  }

  /** Drops this dataflow graph and stops any attached flows. */
  def drop(): Unit = {
    val cmd = proto.PipelineCommand.DropDataflowGraph(dataflowGraphId = Some(graphId))
    dispatch(proto.PipelineCommand().withDropDataflowGraph(cmd))
  }

  // -------------------------------------------------------------------------
  // internals
  // -------------------------------------------------------------------------

  private def defineTableOutput(
      name: String,
      outputType: proto.OutputType,
      query: Option[Dataset[?]],
      comment: Option[String],
      format: Option[String],
      partitionCols: Seq[String],
      clusteringColumns: Seq[String],
      tableProperties: Map[String, String],
      schema: Option[DataType]
  ): String = {
    var details = proto.PipelineCommand.DefineOutput.TableDetails(
      tableProperties = tableProperties,
      partitionCols = partitionCols,
      clusteringColumns = clusteringColumns,
      format = format
    )
    schema.foreach(s =>
      details = details.withSchemaDataType(DataTypeProtoConverter.toConnectProtoType(s))
    )
    val resolved = defineOutput(name, outputType, comment, _.withTableDetails(details))
    query.foreach(q => defineFlow(name, q, target = name))
    resolved
  }

  private def defineOutput(
      name: String,
      outputType: proto.OutputType,
      comment: Option[String],
      setDetails: proto.PipelineCommand.DefineOutput => proto.PipelineCommand.DefineOutput
  ): String = {
    val output = setDetails(
      proto.PipelineCommand.DefineOutput(
        dataflowGraphId = Some(graphId),
        outputName = Some(name),
        outputType = Some(outputType),
        comment = comment
      )
    )
    dispatch(proto.PipelineCommand().withDefineOutput(output))
      .flatMap(_.resultType.defineOutputResult)
      .flatMap(_.resolvedIdentifier)
      .map(identifierString)
      .getOrElse(name)
  }

  private def dispatch(cmd: proto.PipelineCommand): Option[proto.PipelineCommandResult] =
    sparkSession
      .executeCommandResponses(proto.Command().withPipelineCommand(cmd))
      .iterator
      .flatMap(_.responseType.pipelineCommandResult)
      .nextOption()

  private def identifierString(id: proto.ResolvedIdentifier): String =
    (Seq(id.catalogName) ++ id.namespace :+ id.tableName).filter(_.nonEmpty).mkString(".")
}
