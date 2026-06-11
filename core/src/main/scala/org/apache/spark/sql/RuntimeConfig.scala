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

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Runtime configuration interface for Spark, backed by the Spark Connect `Config` RPC. Mirrors
 * `org.apache.spark.sql.RuntimeConfig`: get/set/unset session-scoped Spark SQL configurations.
 *
 * {{{
 *   spark.conf.set("spark.sql.shuffle.partitions", "8")
 *   spark.conf.get("spark.sql.shuffle.partitions")    // "8"
 * }}}
 */
class RuntimeConfig private[sql] (client: SparkConnectClient) {
  import RuntimeConfig._

  /** Sets the given Spark runtime configuration property. */
  def set(key: String, value: String): Unit =
    execute(operation(_.withSet(proto.ConfigRequest.Set(pairs = Seq(pair(key, value))))))

  def set(key: String, value: Boolean): Unit = set(key, value.toString)
  def set(key: String, value: Long): Unit = set(key, value.toString)

  /**
   * Returns the value of the Spark runtime configuration property for the given key. Throws if the
   * key is not set and has no default.
   */
  def get(key: String): String =
    single(execute(operation(_.withGet(proto.ConfigRequest.Get(keys = Seq(key))))), key)
      .getOrElse(throw new java.util.NoSuchElementException(key))

  /** Returns the value of the property, or `default` if unset. */
  def get(key: String, default: String): String =
    single(
      execute(
        operation(
          _.withGetWithDefault(proto.ConfigRequest.GetWithDefault(pairs = Seq(pair(key, default))))
        )
      ),
      key
    ).getOrElse(default)

  /** Returns the value of the property as an `Option`. */
  def getOption(key: String): Option[String] =
    single(execute(operation(_.withGetOption(proto.ConfigRequest.GetOption(keys = Seq(key))))), key)

  /** Returns all properties set in this session. */
  def getAll: Map[String, String] =
    execute(operation(_.withGetAll(proto.ConfigRequest.GetAll()))).pairs.map { kv =>
      kv.key -> kv.value.getOrElse(null)
    }.toMap

  /** Resets the configuration property for the given key. */
  def unset(key: String): Unit =
    execute(operation(_.withUnset(proto.ConfigRequest.Unset(keys = Seq(key)))))

  /**
   * Indicates whether the configuration property with the given key is modifiable in the session.
   */
  def isModifiable(key: String): Boolean =
    single(
      execute(operation(_.withIsModifiable(proto.ConfigRequest.IsModifiable(keys = Seq(key))))),
      key
    ).contains("true")

  // -- internals --

  private def execute(op: proto.ConfigRequest.Operation): proto.ConfigResponse = client.config(op)

  private def single(response: proto.ConfigResponse, key: String): Option[String] =
    response.pairs.find(_.key == key).flatMap(_.value)
}

private object RuntimeConfig {
  def pair(key: String, value: String): proto.KeyValue =
    proto.KeyValue(key = key, value = Some(value))

  def operation(
      f: proto.ConfigRequest.Operation => proto.ConfigRequest.Operation
  ): proto.ConfigRequest.Operation =
    f(proto.ConfigRequest.Operation())
}
