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

import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.*

/**
 * Represents one row of output from a relational operator, mirroring `org.apache.spark.sql.Row`.
 *
 * Fields can be accessed positionally (`row.getInt(0)`) or, when a schema is attached, by name
 * (`row.getAs[Int]("age")`). Values use the standard Spell-to-JVM mapping:
 *
 *   - BooleanType -> Boolean
 *   - ByteType -> Byte, ShortType -> Short, IntegerType -> Int, LongType -> Long
 *   - FloatType -> Float, DoubleType -> Double
 *   - DecimalType -> java.math.BigDecimal
 *   - StringType -> String
 *   - BinaryType -> Array[Byte]
 *   - DateType -> java.sql.Date, TimestampType -> java.sql.Timestamp
 *   - ArrayType -> scala.collection.Seq
 *   - MapType -> scala.collection.Map
 *   - StructType -> Row
 */
trait Row extends Serializable:

  /** Number of elements in the row. */
  def size: Int = length

  /** Number of elements in the row. */
  def length: Int

  /** Schema for the row, or `null` if not known. */
  def schema: StructType = null

  /** Returns the value at position `i`. If the value is `null`, `null` is returned. */
  def apply(i: Int): Any = get(i)

  /** Returns the value at position `i`. */
  def get(i: Int): Any

  /** Checks whether the value at position `i` is null. */
  def isNullAt(i: Int): Boolean = get(i) == null

  def getBoolean(i: Int): Boolean = getAnyValAs[Boolean](i)
  def getByte(i: Int): Byte = getAnyValAs[Byte](i)
  def getShort(i: Int): Short = getAnyValAs[Short](i)
  def getInt(i: Int): Int = getAnyValAs[Int](i)
  def getLong(i: Int): Long = getAnyValAs[Long](i)
  def getFloat(i: Int): Float = getAnyValAs[Float](i)
  def getDouble(i: Int): Double = getAnyValAs[Double](i)
  def getString(i: Int): String = getAs[String](i)

  def getDecimal(i: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](i)
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)
  def getTimestamp(i: Int): java.sql.Timestamp = getAs[java.sql.Timestamp](i)
  def getInstant(i: Int): java.time.Instant = getAs[java.time.Instant](i)
  def getLocalDate(i: Int): java.time.LocalDate = getAs[java.time.LocalDate](i)

  def getSeq[T](i: Int): Seq[T] = getAs[scala.collection.Seq[T]](i).toSeq
  def getList[T](i: Int): java.util.List[T] = getSeq[T](i).asJava
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[scala.collection.Map[K, V]](i)
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] = getMap[K, V](i).asJava
  def getStruct(i: Int): Row = getAs[Row](i)

  /** Returns the value at position `i` cast to the requested type. */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /** Returns the value of the field with the given name. Requires a schema. */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))

  /** Returns the index of the named field. Requires a schema. */
  def fieldIndex(name: String): Int =
    if schema == null then
      throw new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
    else schema.fieldIndex(name)

  /** Returns a Map of the requested field names to their values. Requires a schema. */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] =
    fieldNames.map(n => n -> getAs[T](n)).toMap

  /** Whether the value at position `i` is null-safe equal across rows. */
  def anyNull: Boolean = (0 until length).exists(isNullAt)

  /** Returns the row as a Seq. */
  def toSeq: Seq[Any] = (0 until length).map(get)

  def copy(): Row = this

  def mkString: String = mkString("")
  def mkString(sep: String): String = mkString("", sep, "")
  def mkString(start: String, sep: String, end: String): String =
    toSeq.mkString(start, sep, end)

  /** Best-effort JSON serialization of this row. Requires a schema for field names. */
  def json: String =
    val names =
      if schema != null then schema.fieldNames else (0 until length).map(i => s"_$i").toArray
    names
      .zip(toSeq)
      .map { case (n, v) => s""""$n":${Row.valueToJson(v)}""" }
      .mkString("{", ",", "}")

  private def getAnyValAs[T](i: Int): T =
    if isNullAt(i) then throw new NullPointerException(s"Value at index $i is null")
    else getAs[T](i)

  override def toString: String = mkString("[", ",", "]")

  override def equals(o: Any): Boolean = o match
    case that: Row =>
      if that.length != this.length then false
      else
        (0 until length).forall { i =>
          (this.get(i), that.get(i)) match
            case (a: Array[?], b: Array[?]) => a.sameElements(b)
            case (a, b) => a == b
        }
    case _ => false

  override def hashCode(): Int = toSeq.hashCode()

object Row:
  /** Construct a row from the given values (no schema). */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /** Construct a row from a Seq of values (no schema). */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  /** Merge multiple rows into a single row. */
  def merge(rows: Row*): Row = new GenericRow(rows.flatMap(_.toSeq).toArray)

  /** The empty row. */
  val empty: Row = apply()

  private[sql] def valueToJson(v: Any): String = v match
    case null => "null"
    case s: String => s""""${s.replace("\\", "\\\\").replace("\"", "\\\"")}""""
    case b: Boolean => b.toString
    case n: Number => n.toString
    case r: Row => r.json
    case s: scala.collection.Seq[?] => s.map(valueToJson).mkString("[", ",", "]")
    case m: scala.collection.Map[?, ?] =>
      m.map { case (k, vv) => s""""$k":${valueToJson(vv)}""" }.mkString("{", ",", "}")
    case other => s""""${other.toString}""""

/** A [[Row]] backed by an array of values, with no schema. */
class GenericRow(protected val values: Array[Any]) extends Row:
  override def length: Int = values.length
  override def get(i: Int): Any = values(i)
  override def copy(): Row = this

/** A [[Row]] backed by an array of values with an attached schema. */
final class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
    extends GenericRow(values):
  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
