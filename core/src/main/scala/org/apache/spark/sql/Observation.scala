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

import java.util.UUID

/**
 * Captures named aggregate metrics computed as a side effect of an action, without an extra pass
 * over the data. Attach an `Observation` to a [[Dataset]] with [[Dataset.observe]], run an action
 * (e.g. `collect`, `count`, `write`), then read the metrics with [[get]].
 *
 * {{{
 *   val obs = Observation("my_metrics")
 *   val df = spark.range(100).observe(obs, count(lit(1)).as("rows"), max(col("id")).as("max_id"))
 *   df.collect()
 *   obs.get  // Map("rows" -> 100, "max_id" -> 99)
 * }}}
 *
 * An `Observation` is single-use: it captures the metrics of the first action on the observed
 * Dataset.
 */
class Observation(val name: String) {

  /** Create an `Observation` with a random, unique name. */
  def this() = this(UUID.randomUUID().toString)

  private val lock = new Object
  private var metricsOpt: Option[Map[String, Any]] = None

  private[sql] def setMetricsIfAbsent(values: Map[String, Any]): Unit = lock.synchronized {
    if (metricsOpt.isEmpty) {
      metricsOpt = Some(values)
      lock.notifyAll()
    }
  }

  /**
   * Blocks until the observed metrics are available (i.e. until an action has run on the observed
   * Dataset) and returns them keyed by the aggregate column name.
   */
  def get: Map[String, Any] = lock.synchronized {
    while (metricsOpt.isEmpty) lock.wait()
    metricsOpt.get
  }

  /** The observed metrics if already available, otherwise `None` (non-blocking). */
  def getOption: Option[Map[String, Any]] = lock.synchronized(metricsOpt)
}

object Observation {

  /** Create an `Observation` with the given name. */
  def apply(name: String): Observation = new Observation(name)

  /** Create an `Observation` with a random, unique name. */
  def apply(): Observation = new Observation()
}
