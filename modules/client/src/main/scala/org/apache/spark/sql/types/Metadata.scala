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

package org.apache.spark.sql.types

/**
 * Metadata is a wrapper over a map that can be used to associate extra information with a
 * [[StructField]]. The supported value types mirror Spark's `Metadata`: Long, Double, Boolean,
 * String, and arrays / nested metadata thereof.
 */
final class Metadata private[types] (private[types] val map: Map[String, Any])
    extends Serializable {

  def contains(key: String): Boolean = map.contains(key)

  def getLong(key: String): Long = get[Long](key)
  def getDouble(key: String): Double = get[Double](key)
  def getBoolean(key: String): Boolean = get[Boolean](key)
  def getString(key: String): String = get[String](key)
  def getMetadata(key: String): Metadata = get[Metadata](key)

  private def get[T](key: String): T =
    map.getOrElse(key, throw new NoSuchElementException(s"Key not found: $key")).asInstanceOf[T]

  def isEmpty: Boolean = map.isEmpty

  override def equals(other: Any): Boolean = other match {
    case that: Metadata => this.map == that.map
    case _ => false
  }

  override def hashCode(): Int = map.hashCode()

  /** Renders the metadata as a JSON object (simplified). */
  def json: String =
    map.toSeq
      .sortBy(_._1)
      .map { case (k, v) => s"\"$k\":${Metadata.toJson(v)}" }
      .mkString("{", ",", "}")

  override def toString: String = json
}

object Metadata {
  private val _empty = new Metadata(Map.empty)
  def empty: Metadata = _empty

  private def toJson(value: Any): String = value match {
    case s: String => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case b: Boolean => b.toString
    case m: Metadata => m.json
    case other => other.toString
  }
}

/** A builder for [[Metadata]]. */
final class MetadataBuilder {
  private var map = Map.empty[String, Any]

  def withMetadata(metadata: Metadata): MetadataBuilder = {
    map ++= metadata.map
    this
  }

  def putLong(key: String, value: Long): MetadataBuilder = put(key, value)
  def putDouble(key: String, value: Double): MetadataBuilder = put(key, value)
  def putBoolean(key: String, value: Boolean): MetadataBuilder = put(key, value)
  def putString(key: String, value: String): MetadataBuilder = put(key, value)
  def putMetadata(key: String, value: Metadata): MetadataBuilder = put(key, value)

  def remove(key: String): MetadataBuilder = {
    map -= key
    this
  }

  private def put(key: String, value: Any): MetadataBuilder = {
    map += (key -> value)
    this
  }

  def build(): Metadata = new Metadata(map)
}
