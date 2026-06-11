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
import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.Dataset

/**
 * Interface used to write a streaming [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this. Mirrors
 * `org.apache.spark.sql.streaming.DataStreamWriter`.
 *
 * {{{
 *   val query = df.writeStream
 *     .format("console")
 *     .outputMode("append")
 *     .trigger(Trigger.ProcessingTime("1 second"))
 *     .start()
 *   query.stop()
 * }}}
 *
 * `foreach` and `foreachBatch` are intentionally unsupported: they require user-defined functions,
 * whose Spark Connect protobuf definitions are not yet finalized in this client.
 */
final class DataStreamWriter[T] private[sql] (ds: Dataset[T]) {

  private var source: Option[String] = None
  private var outputModeValue: Option[String] = None
  private var queryNameValue: Option[String] = None
  private var trigger: Option[Trigger] = None
  private var partitioningColumns: Seq[String] = Nil
  private val extraOptions = mutable.LinkedHashMap.empty[String, String]

  /** Specifies the underlying output data source. */
  def format(source: String): this.type = { this.source = Some(source); this }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   *
   * @param outputMode
   *   one of [[OutputMode.Append]], [[OutputMode.Complete]] or [[OutputMode.Update]].
   */
  def outputMode(outputMode: OutputMode): this.type = {
    this.outputModeValue = Some(outputMode.name)
    this
  }

  /** Specifies the output mode by name: `"append"`, `"complete"` or `"update"`. */
  def outputMode(outputMode: String): this.type = {
    this.outputModeValue = Some(OutputMode.fromString(outputMode).name)
    this
  }

  /** Sets the trigger for the stream query. */
  def trigger(trigger: Trigger): this.type = { this.trigger = Some(trigger); this }

  /** Specifies the name of the [[StreamingQuery]] that can be started with `start()`. */
  def queryName(queryName: String): this.type = { this.queryNameValue = Some(queryName); this }

  /** Adds an output option. */
  def option(key: String, value: String): this.type = { extraOptions(key) = value; this }
  def option(key: String, value: Boolean): this.type = option(key, value.toString)
  def option(key: String, value: Long): this.type = option(key, value.toString)
  def option(key: String, value: Double): this.type = option(key, value.toString)

  /** Adds output options. */
  def options(options: scala.collection.Map[String, String]): this.type = {
    extraOptions ++= options
    this
  }

  def options(options: java.util.Map[String, String]): this.type = {
    extraOptions ++= options.asScala
    this
  }

  /** Partitions the output by the given columns on the file system. */
  def partitionBy(colNames: String*): this.type = {
    this.partitioningColumns = colNames.toSeq
    this
  }

  /**
   * Sets the output of the streaming query to be processed using the provided function. Not
   * supported by this Spark Connect client.
   */
  def foreach(writer: Any): this.type =
    throw new UnsupportedOperationException(
      "foreach is not supported by the Scala Spark Connect client because it requires shipping " +
        "user-defined code to the server, which is not yet supported."
    )

  /**
   * Sets the output of the streaming query to be processed using the provided function that is
   * applied to each micro-batch. Not supported by this Spark Connect client.
   */
  def foreachBatch(function: Any): this.type =
    throw new UnsupportedOperationException(
      "foreachBatch is not supported by the Scala Spark Connect client because it requires " +
        "shipping user-defined code to the server, which is not yet supported."
    )

  /** Starts the execution of the streaming query, which will continually output results. */
  def start(): StreamingQuery = startInternal(None)

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives.
   */
  def start(path: String): StreamingQuery = startInternal(Some(path))

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * table as new data arrives.
   */
  def toTable(tableName: String): StreamingQuery = {
    val op = buildOperation(_.withTableName(tableName))
    run(op)
  }

  private def startInternal(path: Option[String]): StreamingQuery = {
    val op = buildOperation(start => path.fold(start)(start.withPath))
    run(op)
  }

  private def buildOperation(
      setSink: proto.WriteStreamOperationStart => proto.WriteStreamOperationStart
  ): proto.WriteStreamOperationStart = {
    var op = proto.WriteStreamOperationStart(
      input = Some(ds.relation),
      format = source.getOrElse(""),
      options = extraOptions.toMap,
      partitioningColumnNames = partitioningColumns
    )
    outputModeValue.foreach(m => op = op.withOutputMode(m))
    queryNameValue.foreach(n => op = op.withQueryName(n))
    trigger.foreach {
      case Trigger.ProcessingTimeTrigger(interval) =>
        op = op.withProcessingTimeInterval(interval)
      case Trigger.OnceTrigger =>
        op = op.withOnce(true)
      case Trigger.AvailableNowTrigger =>
        op = op.withAvailableNow(true)
      case Trigger.ContinuousTrigger(interval) =>
        op = op.withContinuousCheckpointInterval(interval)
    }
    setSink(op)
  }

  private def run(op: proto.WriteStreamOperationStart): StreamingQuery = {
    val plan = proto.Plan().withCommand(proto.Command().withWriteStreamOperationStart(op))
    val responses = ds.sparkSession.execute(plan)
    try {
      val result = responses.asScala
        .find(_.responseType.isWriteStreamOperationStartResult)
        .map(_.getWriteStreamOperationStartResult)
        .getOrElse(
          throw new IllegalStateException(
            "Server did not return a streaming query handle for the started query."
          )
        )
      new StreamingQuery(
        ds.sparkSession,
        result.queryId.getOrElse(proto.StreamingQueryInstanceId()),
        result.name
      )
    } finally while (responses.hasNext) responses.next()
  }
}
