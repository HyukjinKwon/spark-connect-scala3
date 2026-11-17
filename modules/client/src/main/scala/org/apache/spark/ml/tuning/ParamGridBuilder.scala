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
package org.apache.spark.ml.tuning

import scala.collection.mutable

import org.apache.spark.ml.param._

/**
 * Builds a grid of parameter maps for tuning: the cartesian product of the values added with
 * [[addGrid]], applied on top of the values given to [[baseOn]]. Mirrors
 * `org.apache.spark.ml.tuning.ParamGridBuilder`.
 *
 * The grid is kept in the order the parameters were added, so the resulting maps are deterministic
 * (Spark uses a hash map here, which has no stable order).
 */
class ParamGridBuilder {

  private val paramGrid = mutable.LinkedHashMap.empty[Param[Any], Iterable[Any]]

  /** Sets the base values that every map of the grid starts from. */
  def baseOn(paramMap: ParamMap): this.type = {
    baseOn(paramMap.toSeq*)
    this
  }

  /** Sets base values from `(param, value)` pairs. */
  def baseOn(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      addGrid(p.param.asInstanceOf[Param[Any]], Seq(p.value))
    }
    this
  }

  /** Adds `param` with the values to try. */
  def addGrid[T](param: Param[T], values: Iterable[T]): this.type = {
    paramGrid.put(param.asInstanceOf[Param[Any]], values.asInstanceOf[Iterable[Any]])
    this
  }

  /** Adds a double parameter with the values to try. */
  def addGrid(param: DoubleParam, values: Array[Double]): this.type =
    addGrid[Double](param, values.toSeq)

  /** Adds an int parameter with the values to try. */
  def addGrid(param: IntParam, values: Array[Int]): this.type = addGrid[Int](param, values.toSeq)

  /** Adds a long parameter with the values to try. */
  def addGrid(param: LongParam, values: Array[Long]): this.type =
    addGrid[Long](param, values.toSeq)

  /** Adds a float parameter with the values to try. */
  def addGrid(param: FloatParam, values: Array[Float]): this.type =
    addGrid[Float](param, values.toSeq)

  /** Adds a boolean parameter with the values `true` and `false`. */
  def addGrid(param: BooleanParam): this.type = addGrid[Boolean](param, Seq(true, false))

  /** The cartesian product of the grid, one map per combination. */
  def build(): Array[ParamMap] = {
    var paramMaps = Array(new ParamMap())
    paramGrid.foreach { case (param, values) =>
      val newParamMaps = values.flatMap { v =>
        paramMaps.map(_.copy.put(param, v))
      }
      paramMaps = newParamMaps.toArray
    }
    paramMaps
  }
}
