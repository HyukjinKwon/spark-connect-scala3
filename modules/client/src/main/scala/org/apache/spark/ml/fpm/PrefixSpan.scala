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
package org.apache.spark.ml.fpm

import org.apache.spark.ml.connect.RemoteOps
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * PrefixSpan: frequent sequential pattern mining, run on the server. Mirrors
 * `org.apache.spark.ml.fpm.PrefixSpan`. It is not an estimator: [[findFrequentSequentialPatterns]]
 * runs the mining once.
 */
class PrefixSpan(override val uid: String) extends Params {

  def this() = this(Identifiable.randomUID("prefixSpan"))

  /** The minimal support level of a sequential pattern, in [0, 1]. */
  final val minSupport: DoubleParam = new DoubleParam(
    this,
    "minSupport",
    "The minimal support level of the sequential pattern. Sequential pattern that appears more " +
      "than (minSupport * size-of-the-dataset) times will be output.",
    ParamValidators.inRange(0.0, 1.0)
  )

  /** The maximal pattern length. */
  final val maxPatternLength: IntParam = new IntParam(
    this,
    "maxPatternLength",
    "The maximal length of the sequential pattern.",
    ParamValidators.gt(0)
  )

  /** The maximal size of a projected database held in memory. */
  final val maxLocalProjDBSize: LongParam = new LongParam(
    this,
    "maxLocalProjDBSize",
    "The maximum number of items (including delimiters used in the internal storage format) " +
      "allowed in a projected database before local processing.",
    ParamValidators.gt(0L)
  )

  /** The sequence column. */
  final val sequenceCol: Param[String] =
    new Param[String](this, "sequenceCol", "The name of the sequence column in dataset.")

  setDefault(
    minSupport -> 0.1,
    maxPatternLength -> 10,
    maxLocalProjDBSize -> 32000000L,
    sequenceCol -> "sequence"
  )

  def setMinSupport(value: Double): this.type = set(minSupport, value)

  def setMaxPatternLength(value: Int): this.type = set(maxPatternLength, value)

  def setMaxLocalProjDBSize(value: Long): this.type = set(maxLocalProjDBSize, value)

  def setSequenceCol(value: String): this.type = set(sequenceCol, value)

  def getMinSupport: Double = $(minSupport)

  def getMaxPatternLength: Int = $(maxPatternLength)

  def getMaxLocalProjDBSize: Long = $(maxLocalProjDBSize)

  def getSequenceCol: String = $(sequenceCol)

  /** The frequent sequential patterns of `dataset`, with their frequencies. */
  def findFrequentSequentialPatterns(dataset: Dataset[_]): DataFrame =
    RemoteOps.fetchRelation(
      RemoteOps.helperRef(dataset.sparkSession),
      Seq("prefixSpanFindFrequentSequentialPatterns"),
      // The server reflects on the argument types and widens an Int to the long parameter.
      Seq(dataset, $(minSupport), $(maxPatternLength), $(maxLocalProjDBSize).toInt, $(sequenceCol))
    )
}
