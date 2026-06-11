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
import org.apache.spark.sql.Dataset

/**
 * Writes a streaming [[Dataset]] to an external sink and starts the streaming query. Use
 * `Dataset.writeStream` to access this.
 *
 * Mirrors the public surface of `org.apache.spark.sql.streaming.DataStreamWriter` over the Spark
 * Connect protocol. Calling [[start]] or [[toTable]] builds a `WriteStreamOperationStart` command,
 * sends it to the server, and returns a [[StreamingQuery]] handle built from the returned query id,
 * run id and name.
 *
 * `foreach` and `foreachBatch` are intentionally unsupported: they require user-defined functions,
 * whose Spark Connect protobuf definitions are not handled here.
 *
 * @example
 *   {{{
 *   val query = df.writeStream
 *     .format("console")
 *     .outputMode("append")
 *     .trigger(Trigger.ProcessingTime("1 second"))
 *     .start()
 *   query.stop()
 *   }}}
 */
class DataStreamWriter private[sql] (ds: Dataset) {

  private var source: Option[String] = None
  private var mode: Option[String] = None
  private var name: Option[String] = None
  private var trigger: proto.WriteStreamOperationStart.Trigger =
    proto.WriteStreamOperationStart.Trigger.Empty
  private val extraOptions = mutable.Map.empty[String, String]
  private var partitioning: Seq[String] = Seq.empty

  /**
   * Specifies the output mode (`"append"`, `"complete"`, or `"update"`).
   *
   * @return this writer, for chaining.
   */
  def outputMode(outputMode: String): DataStreamWriter = {
    this.mode = Option(outputMode)
    this
  }

  /**
   * Specifies the sink format (e.g. `"console"`, `"memory"`, `"kafka"`).
   *
   * @return this writer, for chaining.
   */
  def format(source: String): DataStreamWriter = {
    this.source = Option(source)
    this
  }

  /**
   * Adds an output option for the underlying sink.
   *
   * @return this writer, for chaining.
   */
  def option(key: String, value: String): DataStreamWriter = {
    extraOptions += (key -> value)
    this
  }

  /** Adds a boolean output option. @return this writer, for chaining. */
  def option(key: String, value: Boolean): DataStreamWriter = option(key, value.toString)

  /** Adds a long output option. @return this writer, for chaining. */
  def option(key: String, value: Long): DataStreamWriter = option(key, value.toString)

  /** Adds a double output option. @return this writer, for chaining. */
  def option(key: String, value: Double): DataStreamWriter = option(key, value.toString)

  /**
   * Adds multiple output options.
   *
   * @return this writer, for chaining.
   */
  def options(options: Map[String, String]): DataStreamWriter = {
    extraOptions ++= options
    this
  }

  /**
   * Specifies the name of the [[StreamingQuery]] that can be used with
   * [[StreamingQueryManager#get]] to look it up. The name is required by the memory sink.
   *
   * @return this writer, for chaining.
   */
  def queryName(queryName: String): DataStreamWriter = {
    this.name = Option(queryName)
    this
  }

  /**
   * Partitions the output by the given columns on the file system.
   *
   * @return this writer, for chaining.
   */
  def partitionBy(colNames: String*): DataStreamWriter = {
    this.partitioning = colNames
    this
  }

  /**
   * Sets the trigger for the streaming query. Exactly one trigger is in effect; calling this
   * method again replaces any previously set trigger.
   *
   * @return this writer, for chaining.
   */
  def trigger(trigger: Trigger): DataStreamWriter = {
    this.trigger = trigger.toProto
    this
  }

  /**
   * Starts the execution of the streaming query, writing the result to the given path. This is
   * shorthand for setting the `path` option and calling [[start()]].
   *
   * @return the [[StreamingQuery]] handle for the started query.
   */
  def start(path: String): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.Path(path))

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * configured sink as new data arrives.
   *
   * @return the [[StreamingQuery]] handle for the started query.
   */
  def start(): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.Empty)

  /**
   * Starts the execution of the streaming query, writing the result into the given table.
   *
   * @return the [[StreamingQuery]] handle for the started query.
   */
  def toTable(tableName: String): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.TableName(tableName))

  private def run(
      sinkDestination: proto.WriteStreamOperationStart.SinkDestination): StreamingQuery = {
    val op = proto.WriteStreamOperationStart(
      input = Some(ds.relation),
      format = source.getOrElse(""),
      options = extraOptions.toMap,
      partitioningColumnNames = partitioning,
      trigger = trigger,
      outputMode = mode.getOrElse(""),
      queryName = name.getOrElse(""),
      sinkDestination = sinkDestination)
    val commandProto =
      proto.Command(commandType = proto.Command.CommandType.WriteStreamOperationStart(op))
    val plan = proto.Plan(proto.Plan.OpType.Command(commandProto))
    val responses = ds.sparkSession.client.execute(plan)
    var result: Option[proto.WriteStreamOperationStartResult] = None
    while (responses.hasNext) {
      val response = responses.next()
      if (result.isEmpty && response.responseType.writeStreamOperationStartResult.isDefined) {
        result = response.responseType.writeStreamOperationStartResult
      }
    }
    val wsr = result.getOrElse(
      throw new IllegalStateException(
        "Server did not return a streaming query handle"))
    val instanceId = wsr.getQueryId
    new StreamingQuery(
      ds.sparkSession,
      instanceId.id,
      instanceId.runId,
      if (wsr.name.isEmpty) null else wsr.name)
  }
}

/**
 * Policy used to indicate how often results should be produced by a [[StreamingQuery]]. Mirrors
 * `org.apache.spark.sql.streaming.Trigger`. Pass an instance to [[DataStreamWriter#trigger]].
 */
sealed trait Trigger {

  /** Converts this trigger to its protobuf representation. */
  private[sql] def toProto: proto.WriteStreamOperationStart.Trigger
}

/** Factory methods for the supported [[Trigger]] policies. */
object Trigger {

  /**
   * A trigger policy that runs a micro-batch query periodically based on the processing time
   * interval (e.g. `"10 seconds"`).
   */
  def ProcessingTime(interval: String): Trigger = new Trigger {
    private[sql] def toProto: proto.WriteStreamOperationStart.Trigger =
      proto.WriteStreamOperationStart.Trigger.ProcessingTimeInterval(interval)
  }

  /**
   * A trigger that processes all available data in a single micro-batch and then stops the query.
   */
  @deprecated("Use AvailableNow instead.", "")
  def Once(): Trigger = new Trigger {
    private[sql] def toProto: proto.WriteStreamOperationStart.Trigger =
      proto.WriteStreamOperationStart.Trigger.Once(true)
  }

  /**
   * A trigger that processes all available data in (possibly) multiple micro-batches and then
   * stops the query.
   */
  def AvailableNow(): Trigger = new Trigger {
    private[sql] def toProto: proto.WriteStreamOperationStart.Trigger =
      proto.WriteStreamOperationStart.Trigger.AvailableNow(true)
  }

  /**
   * A trigger that runs a continuous query, checkpointing at the given interval (e.g.
   * `"1 second"`).
   */
  def Continuous(interval: String): Trigger = new Trigger {
    private[sql] def toProto: proto.WriteStreamOperationStart.Trigger =
      proto.WriteStreamOperationStart.Trigger.ContinuousCheckpointInterval(interval)
  }
}
