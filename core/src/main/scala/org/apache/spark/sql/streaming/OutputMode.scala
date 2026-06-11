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
 * OutputMode describes what data will be written to a streaming sink when there is new data
 * available in a streaming DataFrame/Dataset.
 */
sealed abstract class OutputMode(private[sql] val name: String) {
  override def toString: String = name
}

object OutputMode {

  /**
   * OutputMode in which only the new rows in the streaming DataFrame/Dataset will be written to the
   * sink.
   */
  case object Append extends OutputMode("append")

  /**
   * OutputMode in which all the rows in the streaming DataFrame/Dataset will be written to the sink
   * every time there are some updates.
   */
  case object Complete extends OutputMode("complete")

  /**
   * OutputMode in which only the rows that were updated in the streaming DataFrame/Dataset will be
   * written to the sink every time there are some updates.
   */
  case object Update extends OutputMode("update")

  /** Parse an output mode from its string name (case-insensitive). */
  def fromString(mode: String): OutputMode = mode.toLowerCase(java.util.Locale.ROOT) match {
    case "append" => Append
    case "complete" => Complete
    case "update" => Update
    case _ =>
      throw new IllegalArgumentException(
        s"Unknown output mode $mode. Accepted output modes are 'append', 'complete', 'update'"
      )
  }
}
