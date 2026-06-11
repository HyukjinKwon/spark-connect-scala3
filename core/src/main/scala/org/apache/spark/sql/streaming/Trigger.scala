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
 * Policy used to indicate how often results should be produced by a streaming query. The
 * [[DataStreamWriter]] maps the returned marker onto the corresponding `trigger` oneof field of the
 * `WriteStreamOperationStart` command.
 */
sealed trait Trigger

object Trigger {

  /** A trigger that runs a micro-batch as soon as the previous one completes. */
  def ProcessingTime(interval: String): Trigger =
    ProcessingTimeTrigger(interval)

  /**
   * A trigger that fires a micro-batch every `intervalMs` milliseconds. A value of `0` runs the
   * next batch as soon as the previous one finishes.
   */
  def ProcessingTime(intervalMs: Long): Trigger =
    ProcessingTimeTrigger(s"$intervalMs milliseconds")

  /**
   * A trigger that processes all available data in a single micro-batch, then stops the query.
   *
   * @deprecated
   *   use [[AvailableNow]] instead, which processes all available data in (possibly) multiple
   *   batches.
   */
  def Once(): Trigger = OnceTrigger

  /**
   * A trigger that processes all available data at the start of the query in one or more
   * micro-batches, then stops the query.
   */
  def AvailableNow(): Trigger = AvailableNowTrigger

  /** A trigger that runs a continuous query with a given checkpoint interval. */
  def Continuous(interval: String): Trigger =
    ContinuousTrigger(interval)

  /** A trigger that runs a continuous query with a given checkpoint interval in milliseconds. */
  def Continuous(intervalMs: Long): Trigger =
    ContinuousTrigger(s"$intervalMs milliseconds")

  private[sql] case class ProcessingTimeTrigger(interval: String) extends Trigger
  private[sql] case object OnceTrigger extends Trigger
  private[sql] case object AvailableNowTrigger extends Trigger
  private[sql] case class ContinuousTrigger(interval: String) extends Trigger
}
