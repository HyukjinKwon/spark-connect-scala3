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

package org.apache.spark.ml.param

import scala.collection.mutable

/**
 * A map from parameters to values, mirroring `org.apache.spark.ml.param.ParamMap`. Values are
 * stored by parameter identity (parent uid and name).
 */
final class ParamMap private[ml] (private[ml] val map: mutable.LinkedHashMap[Param[Any], Any])
    extends Serializable {

  def this() = this(mutable.LinkedHashMap.empty[Param[Any], Any])

  /** Puts `value` for `param`, replacing any previous value. */
  def put[T](param: Param[T], value: T): this.type = {
    param.validate(value)
    map(param.asInstanceOf[Param[Any]]) = value
    this
  }

  /** Puts all the given pairs. */
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      put(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /** The value for `param`, if present. */
  def get[T](param: Param[T]): Option[T] =
    map.get(param.asInstanceOf[Param[Any]]).map(_.asInstanceOf[T])

  /** The value for `param`, or `default` when absent. */
  def getOrElse[T](param: Param[T], default: T): T = get(param).getOrElse(default)

  /** The value for `param`. Throws `NoSuchElementException` when absent. */
  def apply[T](param: Param[T]): T = get(param).getOrElse {
    throw new NoSuchElementException(s"Cannot find param ${param.name}.")
  }

  /** Whether the map has a value for `param`. */
  def contains(param: Param[_]): Boolean = map.contains(param.asInstanceOf[Param[Any]])

  /** Removes the value for `param`, returning it when present. */
  def remove[T](param: Param[T]): Option[T] =
    map.remove(param.asInstanceOf[Param[Any]]).map(_.asInstanceOf[T])

  /** The entries whose parameter belongs to `parent`. */
  def filter(parent: org.apache.spark.ml.util.Identifiable): ParamMap = {
    val filtered = map.filter { case (p, _) => p.parent == parent.uid }
    new ParamMap(mutable.LinkedHashMap.from(filtered))
  }

  /** A shallow copy of this map. */
  def copy: ParamMap = new ParamMap(mutable.LinkedHashMap.from(map))

  /** This map with the entries of `other` added (overwriting on conflict). */
  def ++(other: ParamMap): ParamMap = {
    val merged = copy
    merged.map ++= other.map
    merged
  }

  /** Adds the entries of `other` to this map in place. */
  def ++=(other: ParamMap): this.type = {
    map ++= other.map
    this
  }

  /** The entries as pairs. */
  def toSeq: Seq[ParamPair[_]] = map.toSeq.map { case (p, v) => ParamPair(p, v) }

  /** The number of entries. */
  def size: Int = map.size

  override def toString: String =
    map.iterator.map { case (p, v) => s"\t${p.parent}-${p.name}: $v" }.mkString("{\n", ",\n", "\n}")
}

object ParamMap {

  /** An empty map. */
  def empty: ParamMap = new ParamMap()

  /** A map of the given pairs. */
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    val map = new ParamMap()
    map.put(paramPairs*)
    map
  }
}
