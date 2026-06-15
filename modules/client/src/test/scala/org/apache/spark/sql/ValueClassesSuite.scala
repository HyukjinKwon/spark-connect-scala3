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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalog.{Database, Function, Table}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.storage.StorageLevel

/** Hermetic coverage for the small value/enum/exception classes that need no server. */
class ValueClassesSuite extends munit.FunSuite {

  test("SaveMode.fromString accepts the documented spellings") {
    assertEquals(SaveMode.fromString("append"), SaveMode.Append)
    assertEquals(SaveMode.fromString("APPEND"), SaveMode.Append)
    assertEquals(SaveMode.fromString("overwrite"), SaveMode.Overwrite)
    assertEquals(SaveMode.fromString("error"), SaveMode.ErrorIfExists)
    assertEquals(SaveMode.fromString("errorifexists"), SaveMode.ErrorIfExists)
    assertEquals(SaveMode.fromString("default"), SaveMode.ErrorIfExists)
    assertEquals(SaveMode.fromString("ignore"), SaveMode.Ignore)
  }

  test("SaveMode.fromString rejects an unknown mode") {
    intercept[IllegalArgumentException](SaveMode.fromString("bogus"))
  }

  test("StorageLevel named levels expose the expected flags") {
    assertEquals(StorageLevel.NONE.useDisk, false)
    assert(StorageLevel.DISK_ONLY.useDisk)
    assert(StorageLevel.MEMORY_ONLY.useMemory)
    assert(StorageLevel.MEMORY_ONLY.deserialized)
    assertEquals(StorageLevel.DISK_ONLY_2.replication, 2)
    assert(StorageLevel.OFF_HEAP.useOffHeap)
    assert(StorageLevel.MEMORY_AND_DISK.useDisk && StorageLevel.MEMORY_AND_DISK.useMemory)
  }

  test("StorageLevel equality, hashCode and proto round-trip") {
    val a = StorageLevel.MEMORY_AND_DISK_2
    assertEquals(a, StorageLevel.fromProto(a.toProto))
    assertEquals(a.hashCode(), StorageLevel.fromProto(a.toProto).hashCode())
    assertNotEquals(StorageLevel.DISK_ONLY: StorageLevel, StorageLevel.MEMORY_ONLY)
    assert(a.toString.contains("replication=2"))
  }

  test("SparkException exposes its error class") {
    val e = new SparkException("boom", null, Some("BOOM_CLASS"))
    assertEquals(e.getErrorClass, "BOOM_CLASS")
    assertEquals(e.getMessage, "boom")
    assertEquals(new SparkException("x").getErrorClass, null)
    assertEquals(new SparkException("x", new RuntimeException("c")).getCause.getMessage, "c")
  }

  test("AnalysisException and ParseException are SparkExceptions") {
    val ae = new AnalysisException("bad column")
    assert(ae.isInstanceOf[SparkException])
    assertEquals(ae.getMessage, "bad column")
    val pe = new ParseException("syntax error")
    assert(pe.isInstanceOf[SparkException])
    assertEquals(pe.getMessage, "syntax error")
  }

  test("catalog metadata toString renders optional fields") {
    val db = Database("default", "spark_catalog", "the default db", "/warehouse")
    assert(db.toString.contains("name='default'"))
    assert(db.toString.contains("catalog='spark_catalog'"))
    assert(db.toString.contains("path='/warehouse'"))

    val t = Table("t", "spark_catalog", Array("ns"), "desc", "MANAGED", isTemporary = false)
    assertEquals(t.database, "ns")
    assert(t.toString.contains("database='ns'"))
    assert(t.toString.contains("tableType='MANAGED'"))

    val tempView = Table("v", null, Array.empty, null, "TEMPORARY", isTemporary = true)
    assertEquals(tempView.database, null)
    assert(!tempView.toString.contains("catalog="))

    val f =
      Function("f", "spark_catalog", Array("ns"), "desc", "com.example.Fn", isTemporary = true)
    assert(f.toString.contains("className='com.example.Fn'"))
    assert(f.toString.contains("isTemporary='true'"))
  }
}
