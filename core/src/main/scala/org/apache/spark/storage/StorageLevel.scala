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
package org.apache.spark.storage

import org.apache.spark.connect.proto

/**
 * Flags for controlling the storage of a Dataset when it is cached/persisted, mirroring
 * `org.apache.spark.storage.StorageLevel`. Use the named levels in the companion object (for
 * example `StorageLevel.MEMORY_AND_DISK`) with `Dataset.persist`.
 */
class StorageLevel private[spark] (
    val useDisk: Boolean,
    val useMemory: Boolean,
    val useOffHeap: Boolean,
    val deserialized: Boolean,
    val replication: Int = 1
) extends Serializable {

  private[spark] def toProto: proto.StorageLevel =
    proto.StorageLevel(
      useDisk = useDisk,
      useMemory = useMemory,
      useOffHeap = useOffHeap,
      deserialized = deserialized,
      replication = replication
    )

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      useDisk == s.useDisk && useMemory == s.useMemory && useOffHeap == s.useOffHeap &&
      deserialized == s.deserialized && replication == s.replication
    case _ => false
  }

  override def hashCode(): Int =
    Seq(useDisk, useMemory, useOffHeap, deserialized, replication).hashCode()

  override def toString: String =
    s"StorageLevel(disk=$useDisk, memory=$useMemory, offheap=$useOffHeap, " +
      s"deserialized=$deserialized, replication=$replication)"
}

object StorageLevel {
  val NONE: StorageLevel = new StorageLevel(false, false, false, false)
  val DISK_ONLY: StorageLevel = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2: StorageLevel = new StorageLevel(true, false, false, false, 2)
  val DISK_ONLY_3: StorageLevel = new StorageLevel(true, false, false, false, 3)
  val MEMORY_ONLY: StorageLevel = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2: StorageLevel = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER: StorageLevel = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2: StorageLevel = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK: StorageLevel = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2: StorageLevel = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER: StorageLevel = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2: StorageLevel = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP: StorageLevel = new StorageLevel(true, true, true, false)

  private[spark] def fromProto(p: proto.StorageLevel): StorageLevel =
    new StorageLevel(p.useDisk, p.useMemory, p.useOffHeap, p.deserialized, p.replication)
}
