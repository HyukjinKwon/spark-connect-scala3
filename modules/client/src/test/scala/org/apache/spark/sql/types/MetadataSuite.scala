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

/** Hermetic coverage for [[Metadata]] and [[MetadataBuilder]]: put/get, removal, JSON rendering. */
class MetadataSuite extends munit.FunSuite {

  test("empty metadata") {
    assert(Metadata.empty.isEmpty)
    assertEquals(Metadata.empty.json, "{}")
    assert(!Metadata.empty.contains("x"))
  }

  test("builder stores and reads back all supported value types") {
    val nested = new MetadataBuilder().putString("k", "v").build()
    val m = new MetadataBuilder()
      .putLong("l", 42L)
      .putDouble("d", 3.5)
      .putBoolean("b", true)
      .putString("s", "hi")
      .putMetadata("m", nested)
      .build()
    assertEquals(m.getLong("l"), 42L)
    assertEquals(m.getDouble("d"), 3.5)
    assertEquals(m.getBoolean("b"), true)
    assertEquals(m.getString("s"), "hi")
    assertEquals(m.getMetadata("m"), nested)
    assert(m.contains("l"))
    assert(!m.isEmpty)
  }

  test("getting a missing key throws") {
    intercept[NoSuchElementException](Metadata.empty.getString("nope"))
  }

  test("remove drops a key") {
    val m = new MetadataBuilder().putString("a", "1").putString("b", "2").remove("a").build()
    assert(!m.contains("a"))
    assert(m.contains("b"))
  }

  test("withMetadata copies existing entries") {
    val base = new MetadataBuilder().putLong("x", 1L).build()
    val m = new MetadataBuilder().withMetadata(base).putLong("y", 2L).build()
    assertEquals(m.getLong("x"), 1L)
    assertEquals(m.getLong("y"), 2L)
  }

  test("json renders keys sorted with escaping") {
    val m = new MetadataBuilder()
      .putString("name", "a\"b\\c")
      .putBoolean("flag", false)
      .build()
    // keys are emitted in sorted order: flag before name
    assertEquals(m.json, """{"flag":false,"name":"a\"b\\c"}""")
  }

  test("equals and hashCode are by content") {
    val a = new MetadataBuilder().putString("k", "v").build()
    val b = new MetadataBuilder().putString("k", "v").build()
    assertEquals(a, b)
    assertEquals(a.hashCode(), b.hashCode())
    assertNotEquals(a, new MetadataBuilder().putString("k", "w").build())
  }
}
