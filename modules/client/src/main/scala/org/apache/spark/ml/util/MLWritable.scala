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
package org.apache.spark.ml.util

import org.apache.spark.ml.connect.RemoteIO
import org.apache.spark.ml.param.Params

/** An ML object that can be saved to a path. */
trait MLWritable {

  /** A writer for this object. */
  def write: MLWriter

  /** Saves this object to `path`, failing if the path already exists. */
  def save(path: String): Unit = write.save(path)
}

/**
 * Saves an ML object to a path, on the server. Mirrors `org.apache.spark.ml.util.MLWriter`.
 *
 * @param instance
 *   the object to save
 */
class MLWriter(protected val instance: Params) {

  private var shouldOverwrite = false
  private val optionMap = scala.collection.mutable.LinkedHashMap.empty[String, String]

  /** Overwrites the path if it already exists. */
  def overwrite(): this.type = {
    shouldOverwrite = true
    this
  }

  /** Whether [[overwrite]] was requested. */
  private[ml] def isOverwrite: Boolean = shouldOverwrite

  /** Sets a writer option. */
  def option(key: String, value: String): this.type = {
    optionMap(key) = value
    this
  }

  /** Saves the object to `path`. */
  def save(path: String): Unit = saveImpl(path)

  /** The save itself. Subclasses override this to write composite objects. */
  protected def saveImpl(path: String): Unit =
    RemoteIO.save(instance, path, shouldOverwrite, optionMap.toMap)
}

/** An ML object that can be loaded from a path. Mirrors `org.apache.spark.ml.util.MLReadable`. */
trait MLReadable[T] {

  /** Loads the object saved at `path`. */
  def load(path: String): T
}
