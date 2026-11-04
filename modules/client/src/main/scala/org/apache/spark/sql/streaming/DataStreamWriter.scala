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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

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
class DataStreamWriter private[sql] (ds: Dataset[?]) {

  private var source: Option[String] = None
  private var mode: Option[String] = None
  private var name: Option[String] = None
  private var trigger: proto.WriteStreamOperationStart.Trigger =
    proto.WriteStreamOperationStart.Trigger.Empty
  private val extraOptions = mutable.Map.empty[String, String]
  private var partitioning: Seq[String] = Seq.empty
  private var clustering: Seq[String] = Seq.empty

  /**
   * Specifies the output mode (`"append"`, `"complete"`, or `"update"`).
   *
   * @return
   *   this writer, for chaining.
   */
  def outputMode(outputMode: String): DataStreamWriter = {
    this.mode = Option(outputMode)
    this
  }

  /** Specifies how data of a streaming DataFrame is written to the sink. */
  def outputMode(outputMode: OutputMode): DataStreamWriter =
    this.outputMode(outputMode.toString.toLowerCase(java.util.Locale.ROOT))

  /**
   * Specifies the sink format (e.g. `"console"`, `"memory"`, `"kafka"`).
   *
   * @return
   *   this writer, for chaining.
   */
  def format(source: String): DataStreamWriter = {
    this.source = Option(source)
    this
  }

  /**
   * Adds an output option for the underlying sink.
   *
   * @return
   *   this writer, for chaining.
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
   * @return
   *   this writer, for chaining.
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter = {
    extraOptions ++= options
    this
  }

  /** (Java-specific) Adds multiple output options. */
  def options(options: java.util.Map[String, String]): DataStreamWriter =
    this.options(options.asScala)

  /**
   * Specifies the name of the [[StreamingQuery]] that can be used with
   * [[StreamingQueryManager#get]] to look it up. The name is required by the memory sink.
   *
   * @return
   *   this writer, for chaining.
   */
  def queryName(queryName: String): DataStreamWriter = {
    this.name = Option(queryName)
    this
  }

  /**
   * Partitions the output by the given columns on the file system.
   *
   * @return
   *   this writer, for chaining.
   */
  def partitionBy(colNames: String*): DataStreamWriter = {
    this.partitioning = colNames
    this
  }

  /** Clusters the output by the given columns, for sinks that support clustering. */
  def clusterBy(colNames: String*): DataStreamWriter = {
    this.clustering = colNames
    this
  }

  /**
   * Sets the trigger for the streaming query. Exactly one trigger is in effect; calling this method
   * again replaces any previously set trigger.
   *
   * @return
   *   this writer, for chaining.
   */
  def trigger(trigger: Trigger): DataStreamWriter = {
    this.trigger = trigger.toProto
    this
  }

  /**
   * Starts the execution of the streaming query, writing the result to the given path. This is
   * shorthand for setting the `path` option and calling [[start()]].
   *
   * @return
   *   the [[StreamingQuery]] handle for the started query.
   */
  def start(path: String): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.Path(path))

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * configured sink as new data arrives.
   *
   * @return
   *   the [[StreamingQuery]] handle for the started query.
   */
  def start(): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.Empty)

  /**
   * Starts the execution of the streaming query, writing the result into the given table.
   *
   * @return
   *   the [[StreamingQuery]] handle for the started query.
   */
  def toTable(tableName: String): StreamingQuery =
    run(proto.WriteStreamOperationStart.SinkDestination.TableName(tableName))

  private def run(
      sinkDestination: proto.WriteStreamOperationStart.SinkDestination
  ): StreamingQuery = {
    val op = proto.WriteStreamOperationStart(
      input = Some(ds.relation),
      format = source.getOrElse(""),
      options = extraOptions.toMap,
      partitioningColumnNames = partitioning,
      clusteringColumnNames = clustering,
      trigger = trigger,
      outputMode = mode.getOrElse(""),
      queryName = name.getOrElse(""),
      sinkDestination = sinkDestination
    )
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
      throw new IllegalStateException("Server did not return a streaming query handle")
    )
    val instanceId = wsr.getQueryId
    new StreamingQuery(
      ds.sparkSession,
      instanceId.id,
      instanceId.runId,
      if (wsr.name.isEmpty) null else wsr.name
    )
  }
}

/**
 * A policy for how often a streaming query produces results, set with
 * `DataStreamWriter.trigger(...)`. Mirrors `org.apache.spark.sql.streaming.Trigger`.
 *
 * {{{
 *   df.writeStream.trigger(Trigger.ProcessingTime("10 seconds"))
 *   df.writeStream.trigger(Trigger.AvailableNow())
 *   df.writeStream.trigger(Trigger.RealTime("5 minutes"))
 * }}}
 */
sealed trait Trigger {

  private[sql] def toProto: proto.WriteStreamOperationStart.Trigger
}

object Trigger {

  import proto.WriteStreamOperationStart.{Trigger => T}

  private def trigger(t: T): Trigger = new Trigger {
    private[sql] def toProto: T = t
  }

  // Durations are sent as "<n> milliseconds", like the Apache Spark Connect client.
  private def millis(ms: Long, what: String, allowZero: Boolean = true): String = {
    require(
      if (allowZero) ms >= 0 else ms > 0,
      if (allowZero) s"the interval of $what should not be negative"
      else s"the $what should not be negative"
    )
    s"$ms milliseconds"
  }

  /** Runs a micro-batch every `interval` of processing time (e.g. `"10 seconds"`). */
  def ProcessingTime(interval: String): Trigger = trigger(T.ProcessingTimeInterval(interval))

  /** Runs a micro-batch every `intervalMs` milliseconds of processing time. */
  def ProcessingTime(intervalMs: Long): Trigger =
    trigger(T.ProcessingTimeInterval(millis(intervalMs, "trigger")))

  /** Runs a micro-batch every `interval` (in `timeUnit`) of processing time. */
  def ProcessingTime(interval: Long, timeUnit: TimeUnit): Trigger =
    ProcessingTime(timeUnit.toMillis(interval))

  /** Runs a micro-batch every `interval` of processing time. */
  def ProcessingTime(interval: Duration): Trigger = ProcessingTime(interval.toMillis)

  /** Processes all available data in one micro-batch, then stops. */
  @deprecated("Use AvailableNow instead.", "3.4.0")
  def Once(): Trigger = trigger(T.Once(true))

  /** Processes all available data, possibly in multiple micro-batches, then stops. */
  def AvailableNow(): Trigger = trigger(T.AvailableNow(true))

  /** Continuous processing with the given checkpoint interval (e.g. `"1 second"`). */
  def Continuous(interval: String): Trigger = trigger(T.ContinuousCheckpointInterval(interval))

  /** Continuous processing with the given checkpoint interval in milliseconds. */
  def Continuous(intervalMs: Long): Trigger =
    trigger(T.ContinuousCheckpointInterval(millis(intervalMs, "trigger")))

  /** Continuous processing with the given checkpoint interval (in `timeUnit`). */
  def Continuous(interval: Long, timeUnit: TimeUnit): Trigger =
    Continuous(timeUnit.toMillis(interval))

  /** Continuous processing with the given checkpoint interval. */
  def Continuous(interval: Duration): Trigger = Continuous(interval.toMillis)

  /**
   * Real-time mode (Spark 4.2+) with batches of the given duration (e.g. `"10 seconds"`): records
   * are processed as they arrive, for sources and sinks that support it.
   */
  def RealTime(batchDuration: String): Trigger = trigger(T.RealTimeBatchDuration(batchDuration))

  /** Real-time mode with batches of `batchDurationMs` milliseconds. */
  def RealTime(batchDurationMs: Long): Trigger =
    trigger(T.RealTimeBatchDuration(millis(batchDurationMs, "batch duration", allowZero = false)))

  /** Real-time mode with batches of the given duration (in `timeUnit`). */
  def RealTime(batchDuration: Long, timeUnit: TimeUnit): Trigger =
    RealTime(timeUnit.toMillis(batchDuration))

  /** Real-time mode with batches of the given duration. */
  def RealTime(batchDuration: Duration): Trigger = RealTime(batchDuration.toMillis)

  /** Real-time mode with the default batch duration of 5 minutes. */
  def RealTime(): Trigger = RealTime(5L, TimeUnit.MINUTES)
}

/**
 * How the rows of a streaming DataFrame are written to the sink. Mirrors
 * `org.apache.spark.sql.streaming.OutputMode`.
 */
sealed abstract class OutputMode private (name: String) {
  override def toString: String = name
}

object OutputMode {
  private case object AppendMode extends OutputMode("Append")
  private case object CompleteMode extends OutputMode("Complete")
  private case object UpdateMode extends OutputMode("Update")

  /** Only new rows are written to the sink. */
  def Append(): OutputMode = AppendMode

  /** All rows are written to the sink every time there are updates. */
  def Complete(): OutputMode = CompleteMode

  /** Only rows that were updated are written to the sink every time there are updates. */
  def Update(): OutputMode = UpdateMode
}
