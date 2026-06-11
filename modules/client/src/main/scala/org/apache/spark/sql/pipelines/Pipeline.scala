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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.DataType

/**
 * A timestamped event emitted by the server during a pipeline run.
 *
 * @param timestamp
 *   the time the event occurred, or `None` if the server did not provide one.
 * @param message
 *   the human readable message for the event, or `None` if absent.
 */
final case class PipelineEvent(
    timestamp: Option[com.google.protobuf.timestamp.Timestamp],
    message: Option[String]
)

/**
 * The type of output registered in a [[Pipeline]] dataflow graph.
 *
 * Mirrors `spark.connect.OutputType` but only exposes the kinds that can be defined without
 * user-defined functions.
 */
sealed abstract class OutputType(private[pipelines] val toProto: proto.OutputType)

object OutputType {

  /** A table which is published to the catalog. */
  case object Table extends OutputType(proto.OutputType.TABLE)

  /** A materialized view which is published to the catalog. */
  case object MaterializedView extends OutputType(proto.OutputType.MATERIALIZED_VIEW)

  /** A view which is not published to the catalog. */
  case object TemporaryView extends OutputType(proto.OutputType.TEMPORARY_VIEW)

  /** A sink which is not published to the catalog. */
  case object Sink extends OutputType(proto.OutputType.SINK)
}

/**
 * A Spark Declarative Pipeline (SDP) dataflow graph.
 *
 * A pipeline is built by registering outputs (tables, materialized views, temporary views, or
 * sinks) and the flows that populate them, then started with [[startRun]]. Each flow is defined by
 * a [[DataFrame]] (an unresolved relation), so flows are composed with the same API used for
 * ordinary queries.
 *
 * Create one with [[Pipeline.create]].
 *
 * @note
 *   `foreach`/`foreachBatch` flows and query-function evaluation are not supported (they require
 *   user-defined functions); define each flow with a relation instead.
 *
 * @example
 *   {{{
 *   val pipe = Pipeline.create(spark, storage = Some("/tmp/pipeline_storage"))
 *   pipe.createMaterializedView("bronze", Some(spark.read.json("/data/raw")))
 *   pipe.createTable("silver", Some(pipe.read("bronze").filter(col("ok"))))
 *   val events = pipe.startRun()
 *   }}}
 */
class Pipeline private (
    private val session: SparkSession,
    /** The server-assigned dataflow graph id. */
    val graphId: String
) {

  /**
   * Reference a dataset defined in this pipeline as a [[DataFrame]] (so later flows can read from
   * earlier outputs).
   *
   * @param name
   *   the dataset name.
   * @return
   *   a [[DataFrame]] reading the named dataset.
   */
  def read(name: String): DataFrame =
    session.newDataFrame(
      proto.Relation.RelType.Read(
        proto.Read(
          readType =
            proto.Read.ReadType.NamedTable(proto.Read.NamedTable(unparsedIdentifier = name))
        )
      )
    )

  /**
   * Define a published table and the flow that populates it.
   *
   * @param name
   *   the table name.
   * @param df
   *   the query that populates the table (a flow), or `None` for a table with no flow.
   * @return
   *   the resolved output identifier.
   */
  def createTable(
      name: String,
      df: Option[DataFrame] = None,
      comment: Option[String] = None,
      format: Option[String] = None,
      partitionCols: Seq[String] = Seq.empty,
      clusteringColumns: Seq[String] = Seq.empty,
      tableProperties: Map[String, String] = Map.empty,
      schema: Option[Either[String, DataType]] = None
  ): String =
    defineTableOutput(
      name,
      OutputType.Table,
      df,
      comment,
      format,
      partitionCols,
      clusteringColumns,
      tableProperties,
      schema
    )

  /**
   * Define a materialized view and the flow that populates it.
   *
   * @return
   *   the resolved output identifier.
   */
  def createMaterializedView(
      name: String,
      df: Option[DataFrame] = None,
      comment: Option[String] = None,
      format: Option[String] = None,
      partitionCols: Seq[String] = Seq.empty,
      clusteringColumns: Seq[String] = Seq.empty,
      tableProperties: Map[String, String] = Map.empty,
      schema: Option[Either[String, DataType]] = None
  ): String =
    defineTableOutput(
      name,
      OutputType.MaterializedView,
      df,
      comment,
      format,
      partitionCols,
      clusteringColumns,
      tableProperties,
      schema
    )

  /**
   * Define a (non-published) temporary view and its flow.
   *
   * @return
   *   the resolved output identifier.
   */
  def createTemporaryView(
      name: String,
      df: Option[DataFrame] = None,
      comment: Option[String] = None
  ): String =
    defineTableOutput(
      name,
      OutputType.TemporaryView,
      df,
      comment,
      format = None,
      partitionCols = Seq.empty,
      clusteringColumns = Seq.empty,
      tableProperties = Map.empty,
      schema = None
    )

  /**
   * Define a streaming sink and the flow that feeds it.
   *
   * @param name
   *   the sink name.
   * @param df
   *   the flow feeding the sink.
   * @param format
   *   the streaming write format.
   * @param options
   *   the streaming write options.
   * @return
   *   the resolved output identifier.
   */
  def createSink(
      name: String,
      df: DataFrame,
      format: Option[String] = None,
      options: Map[String, String] = Map.empty
  ): String = {
    val sink = proto.PipelineCommand.DefineOutput.SinkDetails(options = options, format = format)
    val resolved = defineOutput(
      name,
      OutputType.Sink,
      details = proto.PipelineCommand.DefineOutput.Details.SinkDetails(sink)
    )
    defineFlow(name, df, target = Some(name))
    resolved
  }

  /**
   * Define a flow that writes the contents of `df` into `target`.
   *
   * @param name
   *   the flow name.
   * @param df
   *   the relation defining the flow.
   * @param target
   *   the dataset the flow writes to (defaults to `name`).
   * @param once
   *   define as a one-time (batch) flow.
   * @param sqlConf
   *   SQL configurations set when running this flow.
   * @return
   *   the resolved flow name.
   */
  def defineFlow(
      name: String,
      df: DataFrame,
      target: Option[String] = None,
      once: Boolean = false,
      sqlConf: Map[String, String] = Map.empty
  ): String = {
    // `once` is optional: only set it when true, since the server rejects the
    // option being present at all for non-one-time flows (e.g. MV flows).
    val flow = proto.PipelineCommand.DefineFlow(
      dataflowGraphId = Some(graphId),
      flowName = Some(name),
      targetDatasetName = Some(target.getOrElse(name)),
      sqlConf = sqlConf,
      details = proto.PipelineCommand.DefineFlow.Details.RelationFlowDetails(
        proto.PipelineCommand.DefineFlow.WriteRelationFlowDetails(relation = Some(df.relation))
      ),
      once = if (once) Some(true) else None
    )
    val result = dispatch(proto.PipelineCommand(proto.PipelineCommand.CommandType.DefineFlow(flow)))
    val resolved = result
      .flatMap(_.resultType.defineFlowResult)
      .flatMap(_.resolvedIdentifier)
    identifierString(resolved).getOrElse(name)
  }

  /**
   * Register datasets and flows from a SQL definition.
   *
   * @param sqlText
   *   the SQL source.
   * @param sqlFilePath
   *   the originating SQL file path, if any.
   */
  def defineSql(sqlText: String, sqlFilePath: Option[String] = None): Unit = {
    val el = proto.PipelineCommand.DefineSqlGraphElements(
      dataflowGraphId = Some(graphId),
      sqlFilePath = sqlFilePath,
      sqlText = Some(sqlText)
    )
    dispatch(proto.PipelineCommand(proto.PipelineCommand.CommandType.DefineSqlGraphElements(el)))
  }

  /**
   * Resolve the graph and run a pipeline update. Blocks until the update completes, returning the
   * events emitted during the run.
   *
   * @param fullRefresh
   *   datasets to reset and recompute.
   * @param fullRefreshAll
   *   reset and recompute everything.
   * @param refresh
   *   datasets to update.
   * @param dry
   *   validate the graph without executing flows.
   * @param storage
   *   checkpoint/metadata storage location.
   * @return
   *   the events emitted during the run.
   */
  def startRun(
      fullRefresh: Seq[String] = Seq.empty,
      fullRefreshAll: Boolean = false,
      refresh: Seq[String] = Seq.empty,
      dry: Boolean = false,
      storage: Option[String] = None
  ): Seq[PipelineEvent] = {
    val run = proto.PipelineCommand.StartRun(
      dataflowGraphId = Some(graphId),
      fullRefreshSelection = fullRefresh,
      fullRefreshAll = Some(fullRefreshAll),
      refreshSelection = refresh,
      dry = Some(dry),
      storage = storage
    )
    val responses = executeAll(
      proto.PipelineCommand(proto.PipelineCommand.CommandType.StartRun(run))
    )
    responses.flatMap { response =>
      response.responseType.pipelineEventResult.flatMap(_.event).map { e =>
        PipelineEvent(e.timestamp, e.message)
      }
    }
  }

  /** Drop this dataflow graph and stop any attached flows. */
  def drop(): Unit = {
    val cmd = proto.PipelineCommand.DropDataflowGraph(dataflowGraphId = Some(graphId))
    dispatch(proto.PipelineCommand(proto.PipelineCommand.CommandType.DropDataflowGraph(cmd)))
  }

  // -- Internals --------------------------------------------------------------

  private def defineTableOutput(
      name: String,
      outputType: OutputType,
      df: Option[DataFrame],
      comment: Option[String],
      format: Option[String],
      partitionCols: Seq[String],
      clusteringColumns: Seq[String],
      tableProperties: Map[String, String],
      schema: Option[Either[String, DataType]]
  ): String = {
    val details =
      if (outputType != OutputType.TemporaryView || format.isDefined || schema.isDefined) {
        Some(
          proto.PipelineCommand.DefineOutput.TableDetails(
            tableProperties = tableProperties,
            partitionCols = partitionCols,
            format = format,
            schema = schema
              .map(schemaProto)
              .getOrElse(proto.PipelineCommand.DefineOutput.TableDetails.Schema.Empty),
            clusteringColumns = clusteringColumns
          )
        )
      } else {
        None
      }
    val outputDetails = details
      .map(proto.PipelineCommand.DefineOutput.Details.TableDetails(_))
      .getOrElse(proto.PipelineCommand.DefineOutput.Details.Empty)
    val resolved = defineOutput(name, outputType, comment = comment, details = outputDetails)
    df.foreach(defineFlow(name, _, target = Some(name)))
    resolved
  }

  private def defineOutput(
      name: String,
      outputType: OutputType,
      comment: Option[String] = None,
      details: proto.PipelineCommand.DefineOutput.Details =
        proto.PipelineCommand.DefineOutput.Details.Empty
  ): String = {
    val output = proto.PipelineCommand.DefineOutput(
      dataflowGraphId = Some(graphId),
      outputName = Some(name),
      outputType = Some(outputType.toProto),
      comment = comment,
      details = details
    )
    val result = dispatch(
      proto.PipelineCommand(proto.PipelineCommand.CommandType.DefineOutput(output))
    )
    val resolved = result
      .flatMap(_.resultType.defineOutputResult)
      .flatMap(_.resolvedIdentifier)
    identifierString(resolved).getOrElse(name)
  }

  private def schemaProto(
      schema: Either[String, DataType]
  ): proto.PipelineCommand.DefineOutput.TableDetails.Schema =
    schema match {
      case Left(s) =>
        proto.PipelineCommand.DefineOutput.TableDetails.Schema.SchemaString(s)
      case Right(dt) =>
        proto.PipelineCommand.DefineOutput.TableDetails.Schema
          .SchemaDataType(DataTypeProtoConverter.toConnectProtoType(dt))
    }

  /**
   * Dispatch a single pipeline command and return its [[proto.PipelineCommandResult]], if the
   * server emitted one.
   */
  private def dispatch(
      pipelineCommand: proto.PipelineCommand
  ): Option[proto.PipelineCommandResult] =
    executeAll(pipelineCommand)
      .flatMap(_.responseType.pipelineCommandResult)
      .headOption

  /**
   * Send a pipeline command to the server and drain the full response stream into memory so all
   * results (command results and events) can be inspected.
   */
  private def executeAll(pipelineCommand: proto.PipelineCommand): Seq[proto.ExecutePlanResponse] = {
    val command =
      proto.Command(commandType = proto.Command.CommandType.PipelineCommand(pipelineCommand))
    val plan = proto.Plan(proto.Plan.OpType.Command(command))
    session.client.execute(plan).toSeq
  }

  private def identifierString(resolved: Option[proto.ResolvedIdentifier]): Option[String] =
    resolved
      .map { id =>
        val parts = (id.catalogName +: id.namespace :+ id.tableName).filter(_.nonEmpty)
        parts.mkString(".")
      }
      .filter(_.nonEmpty)
}

object Pipeline {

  /**
   * Create a new dataflow graph on the server and return a [[Pipeline]] bound to it.
   *
   * @param session
   *   the session to run the pipeline against.
   * @param defaultCatalog
   *   the default catalog for datasets in this graph.
   * @param defaultDatabase
   *   the default database for datasets in this graph.
   * @param sqlConf
   *   SQL configurations for all flows in this graph.
   * @return
   *   a [[Pipeline]] bound to the newly created graph.
   */
  def create(
      session: SparkSession,
      defaultCatalog: Option[String] = None,
      defaultDatabase: Option[String] = None,
      sqlConf: Map[String, String] = Map.empty
  ): Pipeline = {
    val cmd = proto.PipelineCommand.CreateDataflowGraph(
      defaultCatalog = defaultCatalog,
      defaultDatabase = defaultDatabase,
      sqlConf = sqlConf
    )
    val command = proto.Command(
      commandType = proto.Command.CommandType.PipelineCommand(
        proto.PipelineCommand(proto.PipelineCommand.CommandType.CreateDataflowGraph(cmd))
      )
    )
    val plan = proto.Plan(proto.Plan.OpType.Command(command))
    val responses = session.client.execute(plan).toSeq
    val graphId = responses
      .flatMap(_.responseType.pipelineCommandResult)
      .flatMap(_.resultType.createDataflowGraphResult)
      .flatMap(_.dataflowGraphId)
      .headOption
      .getOrElse(
        throw new IllegalStateException(
          "Server did not return a dataflow graph id for CreateDataflowGraph."
        )
      )
    new Pipeline(session, graphId)
  }
}
