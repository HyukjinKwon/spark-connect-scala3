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

import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Runtime configuration interface for Spark, accessible via `spark.conf`. Reads and writes are
 * delegated to the Spark Connect server's `Config` RPC.
 */
class RuntimeConfig private[sql] (client: SparkConnectClient) {

  /** Sets the given Spark runtime configuration property. */
  def set(key: String, value: String): Unit = client.setConf(key, value)
  def set(key: String, value: Boolean): Unit = client.setConf(key, value.toString)
  def set(key: String, value: Long): Unit = client.setConf(key, value.toString)

  /** Returns the value of the given key. Throws if the key is not set and has no default. */
  def get(key: String): String = client.getConf(key)

  /** Returns the value of the given key, or `default` if not set. */
  def get(key: String, default: String): String = client.getConfWithDefault(key, default)

  /** Returns the value of the given key as an `Option`. */
  def getOption(key: String): Option[String] = client.getConfOption(key)

  /** Returns all properties set in this configuration. */
  def getAll: Map[String, String] = client.getAllConfs

  /** Resets the given configuration property to its default value. */
  def unset(key: String): Unit = client.unsetConf(key)
}
