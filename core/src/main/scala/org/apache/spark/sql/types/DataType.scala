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

/**
 * The base type of all Spark SQL data types.
 *
 * This mirrors the public surface of `org.apache.spark.sql.types.DataType` from Apache Spark so
 * that user code (and the rest of this client) can refer to schema types with the familiar names.
 */
abstract class DataType extends Serializable:

  /** The default size of a value of this type, used internally for size estimation. */
  private[sql] def defaultSize: Int

  /**
   * Name of the type used in JSON serialization, e.g. `"integer"`, `"string"`, `"array"`. Lower
   * camel case without the trailing `"Type"`.
   */
  def typeName: String =
    val n = this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type")
    n.substring(0, 1).toLowerCase + n.substring(1)

  /** Readable string representation, e.g. `"int"`, `"array<int>"`. */
  def simpleString: String = typeName

  /** String representation used in the catalog, identical to [[simpleString]] for most types. */
  def catalogString: String = simpleString

  /** SQL string representation, e.g. `"INT"`, `"ARRAY<INT>"`. */
  def sql: String = simpleString.toUpperCase

  /** A compact JSON representation of this data type. */
  def json: String = DataType.toJson(this)

  /** Pretty JSON. */
  def prettyJson: String = json

  override def toString: String = s"${getClass.getSimpleName.stripSuffix("$")}"

object DataType:

  /** Best-effort JSON serialization (matches Spark's format for the common cases). */
  private[types] def toJson(dt: DataType): String = dt match
    case s: StructType =>
      val fields = s.fields
        .map { f =>
          s"""{"name":"${f.name}","type":${toJson(
              f.dataType
            )},"nullable":${f.nullable},"metadata":${
              if f.metadata.json.isEmpty then "{}" else f.metadata.json
            }}"""
        }
        .mkString(",")
      s"""{"type":"struct","fields":[$fields]}"""
    case a: ArrayType =>
      s"""{"type":"array","elementType":${toJson(
          a.elementType
        )},"containsNull":${a.containsNull}}"""
    case m: MapType =>
      s"""{"type":"map","keyType":${toJson(m.keyType)},"valueType":${toJson(
          m.valueType
        )},"valueContainsNull":${m.valueContainsNull}}"""
    case d: DecimalType => s""""decimal(${d.precision},${d.scale})""""
    case other => s""""${other.typeName}""""

  /**
   * Parse a Spark DDL-formatted schema string such as `"a INT, b STRING"` or a single type such as
   * `"array<int>"`. This is a pragmatic local parser covering the common cases; for full fidelity
   * the server-side parser is authoritative when the type is sent as an `Unparsed` proto type.
   */
  def fromDDL(ddl: String): DataType = DDLParser.parse(ddl)

// ---------------------------------------------------------------------------
// Atomic types
// ---------------------------------------------------------------------------

/** Numeric data types. */
abstract class NumericType extends DataType

/** An internal type for integral numeric types. */
abstract class IntegralType extends NumericType

/** An internal type for fractional numeric types. */
abstract class FractionalType extends NumericType

case object NullType extends DataType:
  private[sql] def defaultSize: Int = 1
  override def simpleString: String = "void"
  override def sql: String = "VOID"

case object BooleanType extends DataType:
  private[sql] def defaultSize: Int = 1
  override def simpleString: String = "boolean"

case object ByteType extends IntegralType:
  private[sql] def defaultSize: Int = 1
  override def simpleString: String = "tinyint"
  override def sql: String = "TINYINT"

case object ShortType extends IntegralType:
  private[sql] def defaultSize: Int = 2
  override def simpleString: String = "smallint"
  override def sql: String = "SMALLINT"

case object IntegerType extends IntegralType:
  private[sql] def defaultSize: Int = 4
  override def simpleString: String = "int"
  override def sql: String = "INT"

case object LongType extends IntegralType:
  private[sql] def defaultSize: Int = 8
  override def simpleString: String = "bigint"
  override def sql: String = "BIGINT"

case object FloatType extends FractionalType:
  private[sql] def defaultSize: Int = 4
  override def simpleString: String = "float"
  override def sql: String = "FLOAT"

case object DoubleType extends FractionalType:
  private[sql] def defaultSize: Int = 8
  override def simpleString: String = "double"
  override def sql: String = "DOUBLE"

case object BinaryType extends DataType:
  private[sql] def defaultSize: Int = 100
  override def simpleString: String = "binary"

case object StringType extends DataType:
  private[sql] def defaultSize: Int = 20
  override def simpleString: String = "string"

case object DateType extends DataType:
  private[sql] def defaultSize: Int = 4
  override def simpleString: String = "date"

case object TimestampType extends DataType:
  private[sql] def defaultSize: Int = 8
  override def simpleString: String = "timestamp"

case object TimestampNTZType extends DataType:
  private[sql] def defaultSize: Int = 8
  override def typeName: String = "timestamp_ntz"
  override def simpleString: String = "timestamp_ntz"
  override def sql: String = "TIMESTAMP_NTZ"

case object CalendarIntervalType extends DataType:
  private[sql] def defaultSize: Int = 16
  override def simpleString: String = "interval"

case object VariantType extends DataType:
  private[sql] def defaultSize: Int = 2048
  override def simpleString: String = "variant"

case class DecimalType(precision: Int, scale: Int) extends FractionalType:
  private[sql] def defaultSize: Int = if precision <= DecimalType.MAX_INT_DIGITS then 4 else 8
  override def typeName: String = s"decimal($precision,$scale)"
  override def simpleString: String = s"decimal($precision,$scale)"
  override def sql: String = s"DECIMAL($precision,$scale)"

object DecimalType:
  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  val MAX_INT_DIGITS = 9
  val MAX_LONG_DIGITS = 18
  val SYSTEM_DEFAULT: DecimalType = DecimalType(38, 18)
  val USER_DEFAULT: DecimalType = DecimalType(10, 0)
  def apply(): DecimalType = USER_DEFAULT

case class CharType(length: Int) extends DataType:
  private[sql] def defaultSize: Int = length
  override def typeName: String = s"char($length)"
  override def simpleString: String = s"char($length)"
  override def sql: String = s"CHAR($length)"

case class VarcharType(length: Int) extends DataType:
  private[sql] def defaultSize: Int = length
  override def typeName: String = s"varchar($length)"
  override def simpleString: String = s"varchar($length)"
  override def sql: String = s"VARCHAR($length)"

case class YearMonthIntervalType(startField: Byte, endField: Byte) extends DataType:
  private[sql] def defaultSize: Int = 4
  override def typeName: String =
    val s = YearMonthIntervalType.fieldToString(startField)
    val e = YearMonthIntervalType.fieldToString(endField)
    if startField == endField then s"interval $s" else s"interval $s to $e"
  override def simpleString: String = typeName
  override def sql: String = typeName.toUpperCase

object YearMonthIntervalType:
  val YEAR: Byte = 0
  val MONTH: Byte = 1
  private[types] def fieldToString(b: Byte): String = b match
    case 0 => "year"
    case _ => "month"
  def apply(): YearMonthIntervalType = YearMonthIntervalType(YEAR, MONTH)
  def apply(field: Byte): YearMonthIntervalType = YearMonthIntervalType(field, field)

case class DayTimeIntervalType(startField: Byte, endField: Byte) extends DataType:
  private[sql] def defaultSize: Int = 8
  override def typeName: String =
    val s = DayTimeIntervalType.fieldToString(startField)
    val e = DayTimeIntervalType.fieldToString(endField)
    if startField == endField then s"interval $s" else s"interval $s to $e"
  override def simpleString: String = typeName
  override def sql: String = typeName.toUpperCase

object DayTimeIntervalType:
  val DAY: Byte = 0
  val HOUR: Byte = 1
  val MINUTE: Byte = 2
  val SECOND: Byte = 3
  private[types] def fieldToString(b: Byte): String = b match
    case 0 => "day"
    case 1 => "hour"
    case 2 => "minute"
    case _ => "second"
  def apply(): DayTimeIntervalType = DayTimeIntervalType(DAY, SECOND)
  def apply(field: Byte): DayTimeIntervalType = DayTimeIntervalType(field, field)

// ---------------------------------------------------------------------------
// Complex types
// ---------------------------------------------------------------------------

case class ArrayType(elementType: DataType, containsNull: Boolean = true) extends DataType:
  private[sql] def defaultSize: Int = 1 * elementType.defaultSize
  override def simpleString: String = s"array<${elementType.simpleString}>"
  override def catalogString: String = s"array<${elementType.catalogString}>"
  override def sql: String = s"ARRAY<${elementType.sql}>"

object ArrayType:
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)

case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean = true)
    extends DataType:
  private[sql] def defaultSize: Int = keyType.defaultSize + valueType.defaultSize
  override def simpleString: String = s"map<${keyType.simpleString},${valueType.simpleString}>"
  override def catalogString: String = s"map<${keyType.catalogString},${valueType.catalogString}>"
  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"

object MapType:
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType, valueType, valueContainsNull = true)

case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty
):
  def withComment(comment: String): StructField =
    copy(metadata = Metadata(s"""{"comment":"$comment"}"""))

case class StructType(fields: Array[StructField]) extends DataType:

  private lazy val nameToIndex: Map[String, Int] = fields.map(_.name).zipWithIndex.toMap

  private[sql] def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  def add(field: StructField): StructType = StructType(fields :+ field)

  def add(name: String, dataType: DataType): StructType =
    add(StructField(name, dataType, nullable = true))

  def add(name: String, dataType: DataType, nullable: Boolean): StructType =
    add(StructField(name, dataType, nullable))

  def add(name: String, dataType: String): StructType =
    add(StructField(name, DataType.fromDDL(dataType), nullable = true))

  def fieldNames: Array[String] = fields.map(_.name)
  def names: Array[String] = fieldNames

  def apply(name: String): StructField =
    nameToIndex
      .get(name)
      .map(fields(_))
      .getOrElse(throw new IllegalArgumentException(s"Field $name does not exist."))

  def apply(i: Int): StructField = fields(i)

  def fieldIndex(name: String): Int =
    nameToIndex.getOrElse(name, throw new IllegalArgumentException(s"Field $name does not exist."))

  def length: Int = fields.length
  def size: Int = fields.length
  def isEmpty: Boolean = fields.isEmpty
  def nonEmpty: Boolean = fields.nonEmpty
  def iterator: Iterator[StructField] = fields.iterator
  def foreach(f: StructField => Unit): Unit = fields.foreach(f)
  def map[B](f: StructField => B): Seq[B] = fields.toSeq.map(f)

  def treeString: String =
    val sb = new StringBuilder("root\n")
    fields.foreach { f =>
      sb.append(s" |-- ${f.name}: ${f.dataType.typeName} (nullable = ${f.nullable})\n")
    }
    sb.toString

  override def simpleString: String =
    fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString("struct<", ",", ">")

  override def catalogString: String =
    fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")

  override def sql: String =
    fields.map(f => s"${f.name}: ${f.dataType.sql}").mkString("STRUCT<", ", ", ">")

  override def equals(other: Any): Boolean = other match
    case that: StructType => this.fields.sameElements(that.fields)
    case _ => false

  override def hashCode(): Int = java.util.Arrays.hashCode(fields.asInstanceOf[Array[AnyRef]])

object StructType:
  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)
  def apply(fields: java.util.List[StructField]): StructType =
    import scala.jdk.CollectionConverters.*
    StructType(fields.asScala.toArray)
  val empty: StructType = StructType(Array.empty[StructField])
  def fromDDL(ddl: String): StructType = DataType.fromDDL(ddl) match
    case s: StructType => s
    case other => StructType(Array(StructField("col", other)))

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/**
 * Metadata is a wrapper over a JSON object attached to a [[StructField]]. The Connect protocol
 * transports it as an opaque JSON string, so we keep the raw JSON and expose minimal accessors.
 */
final class Metadata(val json: String) extends Serializable:
  def isEmpty: Boolean = json == null || json.isEmpty || json == "{}"
  override def toString: String = if isEmpty then "{}" else json
  override def equals(other: Any): Boolean = other match
    case m: Metadata => this.toString == m.toString
    case _ => false
  override def hashCode(): Int = toString.hashCode

object Metadata:
  val empty: Metadata = new Metadata("")
  def fromJson(json: String): Metadata = new Metadata(if json == null then "" else json)
  def apply(json: String): Metadata = new Metadata(json)
