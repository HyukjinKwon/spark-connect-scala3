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
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] = {
    import scala.jdk.CollectionConverters._
    getMap[K, V](i).asJava
  }
  def getStruct(i: Int): Row = getAs[Row](i)
  def getGeometry(i: Int): org.apache.spark.sql.types.Geometry =
    getAs[org.apache.spark.sql.types.Geometry](i)
  def getGeography(i: Int): org.apache.spark.sql.types.Geography =
    getAs[org.apache.spark.sql.types.Geography](i)

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

  /** Returns a Map from field names to values cast to `T` (requires a schema). */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] =
    fieldNames.map(name => name -> getAs[T](name)).toMap

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

  /** The compact JSON representation of this row (requires a schema). */
  def json: String = RowJson.render(this, pretty = false)

  /** The pretty-printed JSON representation of this row (requires a schema). */
  def prettyJson: String = RowJson.render(this, pretty = true)

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

  /** Creates a Row from the elements of a tuple (or any product). */
  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  /** Merges multiple rows into a single row, one after another. */
  @deprecated("This method is deprecated and will be removed in future versions.", "3.0.0")
  def merge(rows: Row*): Row = new GenericRow(rows.flatMap(_.toSeq).toArray)
}

/** JSON rendering of rows, following the conventions of `Row.json` in Apache Spark. */
private object RowJson {
  import org.apache.spark.sql.types._

  def render(row: Row, pretty: Boolean): String = {
    require(row.schema != null, "JSON serialization requires a non-null schema.")
    val sb = new StringBuilder
    struct(sb, row, row.schema, pretty, 0)
    sb.toString
  }

  private def newline(sb: StringBuilder, pretty: Boolean, depth: Int): Unit =
    if (pretty) sb.append('\n').append("  " * depth)

  private def struct(sb: StringBuilder, r: Row, s: StructType, pretty: Boolean, d: Int): Unit = {
    sb.append('{')
    s.fields.zipWithIndex.foreach { case (f, i) =>
      if (i > 0) sb.append(',')
      newline(sb, pretty, d + 1)
      quote(sb, f.name)
      sb.append(if (pretty) " : " else ":")
      value(sb, r.get(i), f.dataType, pretty, d + 1)
    }
    if (s.fields.nonEmpty) newline(sb, pretty, d)
    sb.append('}')
  }

  private def value(sb: StringBuilder, v: Any, dt: DataType, pretty: Boolean, d: Int): Unit =
    (v, dt) match {
      case (null, _) => sb.append("null")
      case (b: Boolean, _) => sb.append(b)
      case (n: java.lang.Number, _: DecimalType) => sb.append(n.toString)
      case (n: BigDecimal, _) => sb.append(n.bigDecimal.toPlainString)
      case (n: java.math.BigDecimal, _) => sb.append(n.toPlainString)
      case (n: Float, _) if n.isNaN || n.isInfinite => quote(sb, n.toString)
      case (n: Double, _) if n.isNaN || n.isInfinite => quote(sb, n.toString)
      case (n: java.lang.Number, _) => sb.append(n.toString)
      case (b: Array[Byte], _) => quote(sb, java.util.Base64.getEncoder.encodeToString(b))
      case (r: Row, s: StructType) => struct(sb, r, s, pretty, d)
      case (m: scala.collection.Map[?, ?], MapType(StringType, vt, _)) =>
        sb.append('{')
        m.toSeq.zipWithIndex.foreach { case ((k, mv), i) =>
          if (i > 0) sb.append(',')
          quote(sb, String.valueOf(k))
          sb.append(':')
          value(sb, mv, vt, pretty, d)
        }
        sb.append('}')
      case (m: scala.collection.Map[?, ?], MapType(kt, vt, _)) =>
        sb.append('[')
        m.toSeq.zipWithIndex.foreach { case ((k, mv), i) =>
          if (i > 0) sb.append(',')
          sb.append("{\"key\":")
          value(sb, k, kt, pretty, d)
          sb.append(",\"value\":")
          value(sb, mv, vt, pretty, d)
          sb.append('}')
        }
        sb.append(']')
      case (seq: scala.collection.Seq[?], ArrayType(et, _)) => array(sb, seq, et, pretty, d)
      case (arr: Array[?], ArrayType(et, _)) => array(sb, arr.toSeq, et, pretty, d)
      case (other, _) => quote(sb, other.toString)
    }

  private def array(
      sb: StringBuilder,
      seq: scala.collection.Seq[?],
      et: DataType,
      pretty: Boolean,
      d: Int
  ): Unit = {
    sb.append('[')
    seq.zipWithIndex.foreach { case (e, i) =>
      if (i > 0) sb.append(',')
      value(sb, e, et, pretty, d)
    }
    sb.append(']')
  }

  private def quote(sb: StringBuilder, s: String): Unit = {
    sb.append('"')
    s.foreach {
      case '"' => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case '\b' => sb.append("\\b")
      case '\f' => sb.append("\\f")
      case c if c < ' ' => sb.append("\\u%04x".format(c.toInt))
      case c => sb.append(c)
    }
    sb.append('"')
  }
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
