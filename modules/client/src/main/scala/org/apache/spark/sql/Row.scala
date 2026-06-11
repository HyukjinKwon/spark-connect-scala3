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

import scala.collection.mutable

import org.apache.spark.sql.types.StructType

/**
 * Represents one row of output from a relational operator. Mirrors `org.apache.spark.sql.Row`.
 */
trait Row extends Serializable {

  /** Number of elements in the row. */
  def length: Int

  /** Number of elements in the row. Alias for [[length]]. */
  def size: Int = length

  /** Schema for the row, or `null` if not available. */
  def schema: StructType = null

  /** Returns the value at position `i`. If the value is `null`, `null` is returned. */
  def get(i: Int): Any

  /** Returns the value at position `i` as a primitive, throwing if it is `null`. */
  def apply(i: Int): Any = get(i)

  def isNullAt(i: Int): Boolean = get(i) == null

  def getBoolean(i: Int): Boolean = getAs[Boolean](i)
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

  def getSeq[T](i: Int): Seq[T] = getAs[Seq[T]](i)
  def getList[T](i: Int): java.util.List[T] = {
    import scala.jdk.CollectionConverters._
    getSeq[T](i).asJava
  }
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[scala.collection.Map[K, V]](i)
  def getStruct(i: Int): Row = getAs[Row](i)

  /** Returns the value at position `i` cast to the expected type. */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /** Returns the value of a field by name (requires a schema). */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))

  /** Returns the index of a given field name (requires a schema). */
  def fieldIndex(name: String): Int =
    if (schema == null) {
      throw new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
    } else {
      schema.fieldIndex(name)
    }

  /** Returns true if there are any `null` values in this row. */
  def anyNull: Boolean = (0 until length).exists(isNullAt)

  /** Returns the row as a Scala Seq. */
  def toSeq: Seq[Any] = (0 until length).map(get)

  /** Returns a Map from field name to value (requires a schema). */
  def getValuesMap[T](fieldNames: Seq[String]): Map[T, Any] =
    fieldNames.map(n => n -> getAs[T](n).asInstanceOf[Any]).toMap.asInstanceOf[Map[T, Any]]

  /** Displayable string, e.g. `[1,abc,2.0]`. */
  def mkString: String = mkString("")
  def mkString(sep: String): String = mkString("", sep, "")
  def mkString(start: String, sep: String, end: String): String = {
    val sb = new mutable.StringBuilder(start)
    var i = 0
    while (i < length) {
      if (i > 0) sb.append(sep)
      sb.append(get(i))
      i += 1
    }
    sb.append(end).toString
  }

  private def getAnyValAs[T <: AnyVal](i: Int): T =
    if (isNullAt(i)) throw new NullPointerException(s"Value at index $i is null")
    else getAs[T](i)

  override def toString: String = this.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = o match {
    case that: Row =>
      if (that.length != this.length) false
      else this.toSeq == that.toSeq
    case _ => false
  }

  override def hashCode(): Int = toSeq.hashCode()

  def copy(): Row = this
}

object Row {

  /** Creates a Row from the given values (no schema). */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /** Creates a Row from a Seq of values. */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  /** An empty Row. */
  val empty: Row = apply()

  /** Extractor so callers can pattern-match on a Row's values. */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)
}

/** A [[Row]] backed by an `Array[Any]`. */
class GenericRow(protected val values: Array[Any]) extends Row {
  override def length: Int = values.length
  override def get(i: Int): Any = values(i)
  override def copy(): Row = this
}

/** A [[Row]] backed by an `Array[Any]` together with a schema. */
class GenericRowWithSchema(values: Array[Any], override val schema: StructType)
    extends GenericRow(values) {
  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}
