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

import org.apache.spark.sql.types.*

/**
 * Coverage for compile-time [[Encoder]] derivation, the typed half of `df.as[T]` and
 * `spark.createDataset`. Each case asserts both the derived `StructType` schema and a value
 * round-trip through `toRow`/`fromRow` (or `toValue`/`fromValue` for atomics), so a regression in
 * schema inference or in the value marshalling is caught without a server.
 */
class EncoderDerivationSuite extends munit.FunSuite {

  case class Person(name: String, age: Int)
  case class Account(id: Long, owner: Person, active: Boolean)
  case class WithOption(name: String, nickname: Option[String])
  case class WithCollections(tags: Seq[String], scores: Map[String, Int])

  test("atomic encoders expose the right data type and round-trip") {
    assertEquals(summon[Encoder[Int]].dataType, IntegerType: DataType)
    assertEquals(summon[Encoder[Long]].dataType, LongType: DataType)
    assertEquals(summon[Encoder[String]].dataType, StringType: DataType)
    assertEquals(summon[Encoder[Boolean]].dataType, BooleanType: DataType)
    assertEquals(summon[Encoder[Double]].dataType, DoubleType: DataType)

    val e = summon[Encoder[Int]]
    assertEquals(e.fromValue(e.toValue(42)), 42)
    val s = summon[Encoder[String]]
    assertEquals(s.fromValue(s.toValue("hi")), "hi")
  }

  test("case class derives a struct schema in declaration order") {
    val e = summon[Encoder[Person]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = false)
        )
      )
    )
  }

  test("case class round-trips through toRow/fromRow") {
    val e = summon[Encoder[Person]]
    val person = Person("Alice", 30)
    val row = e.toRow(person)
    assertEquals(row.getString(0), "Alice")
    assertEquals(row.getInt(1), 30)
    assertEquals(e.fromRow(row), person)
  }

  test("nested case class derives a nested struct and round-trips") {
    val e = summon[Encoder[Account]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("id", LongType, nullable = false),
          StructField(
            "owner",
            StructType(
              Array(
                StructField("name", StringType, nullable = true),
                StructField("age", IntegerType, nullable = false)
              )
            ),
            nullable = false
          ),
          StructField("active", BooleanType, nullable = false)
        )
      )
    )
    val account = Account(7L, Person("Bob", 25), active = true)
    assertEquals(e.fromRow(e.toRow(account)), account)
  }

  test("tuple derives a positional struct schema and round-trips") {
    val e = summon[Encoder[(String, Int, Boolean)]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("_1", StringType, nullable = true),
          StructField("_2", IntegerType, nullable = false),
          StructField("_3", BooleanType, nullable = false)
        )
      )
    )
    val t = ("x", 1, true)
    assertEquals(e.fromRow(e.toRow(t)), t)
  }

  test("Option encoder is nullable and round-trips Some and None") {
    val e = summon[Encoder[Option[String]]]
    assert(e.nullable)
    assertEquals(e.dataType, StringType: DataType)
    assertEquals(e.toValue(Some("v")), "v")
    assertEquals(e.toValue(None), null)
    assertEquals(e.fromValue("v"), Some("v"))
    assertEquals(e.fromValue(null), None)
  }

  test("case class with an Option field marks the field nullable") {
    val e = summon[Encoder[WithOption]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("name", StringType, nullable = true),
          StructField("nickname", StringType, nullable = true)
        )
      )
    )
    val a = WithOption("a", Some("ace"))
    val b = WithOption("b", None)
    assertEquals(e.fromRow(e.toRow(a)), a)
    assertEquals(e.fromRow(e.toRow(b)), b)
  }

  test("collection and map encoders derive array/map types and round-trip") {
    val seqE = summon[Encoder[Seq[String]]]
    assertEquals(seqE.dataType, ArrayType(StringType, containsNull = true): DataType)
    assertEquals(seqE.fromValue(seqE.toValue(Seq("a", "b"))), Seq("a", "b"))

    val mapE = summon[Encoder[Map[String, Int]]]
    assertEquals(
      mapE.dataType,
      MapType(StringType, IntegerType, valueContainsNull = false): DataType
    )
    assertEquals(mapE.fromValue(mapE.toValue(Map("a" -> 1))), Map("a" -> 1))
  }

  test("case class with collection fields derives and round-trips") {
    val e = summon[Encoder[WithCollections]]
    assertEquals(
      e.schema,
      StructType(
        Array(
          StructField("tags", ArrayType(StringType, containsNull = true), nullable = false),
          StructField(
            "scores",
            MapType(StringType, IntegerType, valueContainsNull = false),
            nullable = false
          )
        )
      )
    )
    val v = WithCollections(Seq("x", "y"), Map("a" -> 1, "b" -> 2))
    assertEquals(e.fromRow(e.toRow(v)), v)
  }
}
