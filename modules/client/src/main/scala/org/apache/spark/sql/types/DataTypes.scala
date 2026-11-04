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
 * This mirrors `org.apache.spark.sql.types.DataType` from Apache Spark closely enough that schemas
 * returned by a Spark Connect server can be represented faithfully on a pure Scala 3 client without
 * depending on `spark-catalyst`.
 */
abstract class DataType extends Serializable {

  /** The default size of a value of this data type, used internally for size estimation. */
  def defaultSize: Int

  /** Name of the type used in SQL, e.g. `INT`, `ARRAY<STRING>`. */
  def typeName: String =
    this.getClass.getSimpleName
      .stripSuffix("$")
      .stripSuffix("Type")
      .stripSuffix("UDT")
      .toLowerCase

  /** Readable string representation, e.g. for `df.printSchema()`. */
  def simpleString: String = typeName

  /** SQL representation of this type. */
  def sql: String = simpleString.toUpperCase

  /** A `catalogString` describing this type for catalog / DDL purposes. */
  def catalogString: String = simpleString

  /** Compares two types ignoring nullability of nested fields. */
  private[sql] def sameType(other: DataType): Boolean =
    DataType.equalsIgnoreNullability(this, other)

  def json: String = simpleString
}

object DataType {
  private[sql] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean =
    (left, right) match {
      case (ArrayType(l, _), ArrayType(r, _)) => equalsIgnoreNullability(l, r)
      case (MapType(lk, lv, _), MapType(rk, rv, _)) =>
        equalsIgnoreNullability(lk, rk) && equalsIgnoreNullability(lv, rv)
      case (StructType(lf), StructType(rf)) =>
        lf.length == rf.length && lf.zip(rf).forall { case (l, r) =>
          l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
        }
      case (l, r) => l == r
    }
}

/** Numeric data types. */
abstract class NumericType extends DataType

/** A type for which the values can be ordered and compared. */
abstract class AtomicType extends DataType

case object NullType extends DataType {
  override def defaultSize: Int = 1
  // Apache Spark names the NULL type `void` (e.g. in `printSchema` and DDL).
  override def typeName: String = "void"
}

case object BooleanType extends AtomicType { override def defaultSize: Int = 1 }

case object ByteType extends NumericType {
  override def defaultSize: Int = 1
  override def simpleString: String = "tinyint"
}

case object ShortType extends NumericType {
  override def defaultSize: Int = 2
  override def simpleString: String = "smallint"
}

case object IntegerType extends NumericType {
  override def defaultSize: Int = 4
  override def simpleString: String = "int"
}

case object LongType extends NumericType {
  override def defaultSize: Int = 8
  override def simpleString: String = "bigint"
}

case object FloatType extends NumericType { override def defaultSize: Int = 4 }

case object DoubleType extends NumericType { override def defaultSize: Int = 8 }

case object StringType extends AtomicType { override def defaultSize: Int = 20 }

/** A char(n) type. */
case class CharType(length: Int) extends AtomicType {
  override def defaultSize: Int = length
  override def simpleString: String = s"char($length)"
}

/** A varchar(n) type. */
case class VarcharType(length: Int) extends AtomicType {
  override def defaultSize: Int = length
  override def simpleString: String = s"varchar($length)"
}

case object BinaryType extends AtomicType { override def defaultSize: Int = 100 }

case object DateType extends AtomicType { override def defaultSize: Int = 4 }

case object TimestampType extends AtomicType { override def defaultSize: Int = 8 }

case object TimestampNTZType extends AtomicType {
  override def defaultSize: Int = 8
  override def simpleString: String = "timestamp_ntz"
}

/**
 * The TIME type: a time of day without a time zone, with `precision` digits of fractional seconds
 * (0 to 9). Values are represented as `java.time.LocalTime`. Mirrors
 * `org.apache.spark.sql.types.TimeType` (Spark 4.1+; the server gates it behind
 * `spark.sql.timeType.enabled`).
 */
case class TimeType(precision: Int) extends AtomicType {
  if (precision < TimeType.MIN_PRECISION || precision > TimeType.MAX_PRECISION) {
    throw org.apache.spark.SparkException.withErrorClass(
      "UNSUPPORTED_TIME_PRECISION",
      Map("precision" -> precision.toString)
    )
  }
  override def defaultSize: Int = 8
  override def typeName: String = s"time($precision)"
}

object TimeType {
  val MIN_PRECISION: Int = 0
  val MICROS_PRECISION: Int = 6
  val NANOS_PRECISION: Int = 9
  val MAX_PRECISION: Int = NANOS_PRECISION
  val DEFAULT_PRECISION: Int = MICROS_PRECISION

  def apply(): TimeType = new TimeType(DEFAULT_PRECISION)
}

/**
 * A timestamp without time zone with nanosecond-capable precision (7 to 9 fractional-second
 * digits). Values are represented as `java.time.LocalDateTime`. Mirrors
 * `org.apache.spark.sql.types.TimestampNTZNanosType` (Spark 4.3+, a preview feature the server
 * gates behind `spark.sql.timestampNanosTypes.enabled`).
 */
case class TimestampNTZNanosType(precision: Int) extends AtomicType {
  TimestampNanos.validatePrecision(precision, "TIMESTAMP_NTZ")
  override def defaultSize: Int = 10
  override def typeName: String = s"timestamp_ntz($precision)"
}

object TimestampNTZNanosType {
  val MIN_PRECISION: Int = TimestampNanos.MIN_PRECISION
  val MAX_PRECISION: Int = TimestampNanos.MAX_PRECISION
  val NANOS_PRECISION: Int = TimestampNanos.NANOS_PRECISION
  val DEFAULT_PRECISION: Int = NANOS_PRECISION

  def apply(): TimestampNTZNanosType = new TimestampNTZNanosType(DEFAULT_PRECISION)
}

/**
 * A timestamp with local time zone with nanosecond-capable precision (7 to 9 fractional-second
 * digits). Values are represented as `java.time.Instant`. Mirrors
 * `org.apache.spark.sql.types.TimestampLTZNanosType` (Spark 4.3+, a preview feature the server
 * gates behind `spark.sql.timestampNanosTypes.enabled`).
 */
case class TimestampLTZNanosType(precision: Int) extends AtomicType {
  TimestampNanos.validatePrecision(precision, "TIMESTAMP_LTZ")
  override def defaultSize: Int = 10
  override def typeName: String = s"timestamp_ltz($precision)"
}

object TimestampLTZNanosType {
  val MIN_PRECISION: Int = TimestampNanos.MIN_PRECISION
  val MAX_PRECISION: Int = TimestampNanos.MAX_PRECISION
  val NANOS_PRECISION: Int = TimestampNanos.NANOS_PRECISION
  val DEFAULT_PRECISION: Int = NANOS_PRECISION

  def apply(): TimestampLTZNanosType = new TimestampLTZNanosType(DEFAULT_PRECISION)
}

/** Shared precision bounds and validation for the nanosecond timestamp types. */
private[sql] object TimestampNanos {
  val MIN_PRECISION: Int = 7
  val MAX_PRECISION: Int = 9
  val NANOS_PRECISION: Int = 9

  def validatePrecision(precision: Int, typeName: String): Unit =
    if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
      throw org.apache.spark.SparkException.withErrorClass(
        "INVALID_TIMESTAMP_PRECISION",
        Map("precision" -> precision.toString, "type" -> typeName)
      )
    }
}

case object CalendarIntervalType extends DataType {
  override def defaultSize: Int = 16
  override def simpleString: String = "interval"
}

case object VariantType extends AtomicType { override def defaultSize: Int = 2048 }

/**
 * The GEOMETRY type with spatial reference identifier `srid` (`-1`, rendered as `any`, means a
 * mixed-SRID column). Values are represented as [[Geometry]]. Mirrors
 * `org.apache.spark.sql.types.GeometryType` (Spark 4.1+).
 */
case class GeometryType(srid: Int) extends AtomicType {
  override def defaultSize: Int = 2048
  def isMixedSrid: Boolean = srid == GeometryType.MIXED_SRID
  override def typeName: String = if (isMixedSrid) "geometry(any)" else s"geometry($srid)"
}

object GeometryType {
  val MIXED_SRID: Int = -1
  val GEOMETRY_DEFAULT_SRID: Int = 4326

  def apply(): GeometryType = new GeometryType(GEOMETRY_DEFAULT_SRID)
}

/**
 * The GEOGRAPHY type with spatial reference identifier `srid` (`-1`, rendered as `any`, means a
 * mixed-SRID column). Values are represented as [[Geography]]. Mirrors
 * `org.apache.spark.sql.types.GeographyType` (Spark 4.1+).
 */
case class GeographyType(srid: Int) extends AtomicType {
  override def defaultSize: Int = 2048
  def isMixedSrid: Boolean = srid == GeographyType.MIXED_SRID
  override def typeName: String = if (isMixedSrid) "geography(any)" else s"geography($srid)"
}

object GeographyType {
  val MIXED_SRID: Int = -1
  val GEOGRAPHY_DEFAULT_SRID: Int = 4326

  def apply(): GeographyType = new GeographyType(GEOGRAPHY_DEFAULT_SRID)
}

/** A decimal type with the given precision and scale. */
case class DecimalType(precision: Int, scale: Int) extends NumericType {
  override def defaultSize: Int = if (precision <= 18) 8 else 16
  // typeName (not just simpleString) carries the precision/scale so that treeString /
  // printSchema render `decimal(10,0)` rather than a bare `decimal`, matching Apache Spark.
  override def typeName: String = s"decimal($precision,$scale)"
  override def sql: String = s"DECIMAL($precision,$scale)"
}

object DecimalType {
  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  val DEFAULT: DecimalType = DecimalType(10, 0)
  val USER_DEFAULT: DecimalType = DEFAULT
  val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, 18)
  def apply(): DecimalType = DEFAULT
}

/** A year-month interval type covering a contiguous range of fields. */
case class YearMonthIntervalType(startField: Byte, endField: Byte) extends AtomicType {
  override def defaultSize: Int = 4
  // Render the actual field range (e.g. `interval month`, `interval year to month`) rather than a
  // fixed string, matching Apache Spark for non-default field ranges.
  override def typeName: String = {
    def fieldName(b: Byte): String = if (b == YearMonthIntervalType.YEAR) "year" else "month"
    if (startField == endField) s"interval ${fieldName(startField)}"
    else s"interval ${fieldName(startField)} to ${fieldName(endField)}"
  }
}

object YearMonthIntervalType {
  val YEAR: Byte = 0
  val MONTH: Byte = 1
  def apply(): YearMonthIntervalType = YearMonthIntervalType(YEAR, MONTH)
}

/** A day-time interval type covering a contiguous range of fields. */
case class DayTimeIntervalType(startField: Byte, endField: Byte) extends AtomicType {
  override def defaultSize: Int = 8
  // Render the actual field range (e.g. `interval hour to minute`) rather than a fixed string.
  override def typeName: String = {
    val names = Array("day", "hour", "minute", "second")
    if (startField == endField) s"interval ${names(startField)}"
    else s"interval ${names(startField)} to ${names(endField)}"
  }
}

object DayTimeIntervalType {
  val DAY: Byte = 0
  val HOUR: Byte = 1
  val MINUTE: Byte = 2
  val SECOND: Byte = 3
  def apply(): DayTimeIntervalType = DayTimeIntervalType(DAY, SECOND)
}

/** An array type containing elements of `elementType`. */
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {
  override def defaultSize: Int = 1 * elementType.defaultSize
  override def simpleString: String = s"array<${elementType.simpleString}>"
  override def catalogString: String = s"array<${elementType.catalogString}>"
  override def sql: String = s"ARRAY<${elementType.sql}>"
}

object ArrayType {
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)
}

/** A map type with the given key and value types. */
case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends DataType {
  override def defaultSize: Int = 1 * (keyType.defaultSize + valueType.defaultSize)
  override def simpleString: String = s"map<${keyType.simpleString},${valueType.simpleString}>"
  override def catalogString: String =
    s"map<${keyType.catalogString},${valueType.catalogString}>"
  override def sql: String = s"MAP<${keyType.sql}, ${valueType.sql}>"
}

object MapType {
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType, valueType, valueContainsNull = true)
}

/**
 * A field inside a [[StructType]].
 */
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty
) {

  def simpleString: String = s"$name:${dataType.simpleString}"

  /** Returns a copy with the given comment stored in the field's metadata. */
  def withComment(comment: String): StructField =
    copy(metadata =
      new MetadataBuilder().withMetadata(metadata).putString("comment", comment).build()
    )

  def getComment: Option[String] =
    if (metadata.contains("comment")) Some(metadata.getString("comment")) else None

  private def nullDDL: String = if (nullable) "" else " NOT NULL"

  private def commentDDL: String =
    getComment.map(c => s" COMMENT '${StructField.escapeSingleQuoted(c)}'").getOrElse("")

  /** SQL form used inside `STRUCT<...>`, e.g. `id: INT NOT NULL`. */
  private[sql] def sql: String =
    s"${StructField.quoteIfNeeded(name)}: ${dataType.sql}$nullDDL$commentDDL"

  /**
   * Returns this field in DDL format, e.g. `eventId INT NOT NULL`, matching `StructField.toDDL` in
   * Apache Spark.
   */
  def toDDL: String = s"${StructField.quoteIfNeeded(name)} ${dataType.sql}$nullDDL$commentDDL"
}

object StructField {

  private val validIdent = "[a-zA-Z_][a-zA-Z0-9_]*".r

  /** Quotes an identifier with back-ticks unless it is a plain identifier (as Spark does). */
  private[sql] def quoteIfNeeded(part: String): String =
    if (validIdent.matches(part)) part else "`" + part.replace("`", "``") + "`"

  private[sql] def escapeSingleQuoted(s: String): String =
    s.replace("\\", "\\\\").replace("'", "\\'")
}

/**
 * A struct type, i.e. the schema of a row.
 */
case class StructType(fields: Array[StructField]) extends DataType with Iterable[StructField] {

  /** An empty struct, e.g. to build a schema with `new StructType().add("id", LongType)`. */
  def this() = this(Array.empty[StructField])

  override def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  override def iterator: Iterator[StructField] = fields.iterator

  def fieldNames: Array[String] = fields.map(_.name)

  def length: Int = fields.length

  def apply(name: String): StructField =
    fields
      .find(_.name == name)
      .getOrElse(throw new IllegalArgumentException(s"Field \"$name\" does not exist."))

  def apply(index: Int): StructField = fields(index)

  def fieldIndex(name: String): Int = {
    val idx = fields.indexWhere(_.name == name)
    if (idx < 0) throw new IllegalArgumentException(s"Field \"$name\" does not exist.")
    idx
  }

  /** Returns a new StructType with the given field appended. */
  def add(field: StructField): StructType = StructType(fields :+ field)

  def add(name: String, dataType: DataType): StructType =
    add(StructField(name, dataType, nullable = true))

  def add(name: String, dataType: DataType, nullable: Boolean): StructType =
    add(StructField(name, dataType, nullable))

  override def simpleString: String =
    fields.map(_.simpleString).mkString("struct<", ",", ">")

  override def catalogString: String =
    fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")

  /** SQL form, e.g. `STRUCT<id: BIGINT, name: STRING>`, matching Apache Spark. */
  override def sql: String = fields.map(_.sql).mkString("STRUCT<", ", ", ">")

  /**
   * Returns this schema in DDL format, e.g. `eventId INT,s STRING`, which can be used in a table
   * creation. Matches `StructType.toDDL` in Apache Spark.
   */
  def toDDL: String = fields.map(_.toDDL).mkString(",")

  /** Returns a tree-formatted string of this schema, used by `Dataset.printSchema`. */
  def treeString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    def appendField(field: StructField, prefix: String): Unit = {
      builder.append(
        s"$prefix-- ${field.name}: ${field.dataType.typeName} (nullable = ${field.nullable})\n"
      )
      field.dataType match {
        case s: StructType => s.fields.foreach(appendField(_, prefix + "    |"))
        case ArrayType(s: StructType, _) => s.fields.foreach(appendField(_, prefix + "    |"))
        case _ =>
      }
    }
    fields.foreach(appendField(_, " |"))
    builder.toString
  }

  // Structural (content) equality despite the Array field.
  override def equals(other: Any): Boolean = other match {
    case that: StructType => this.fields.sameElements(that.fields)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(fields.asInstanceOf[Array[AnyRef]])
}

object StructType {
  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)
  def apply(): StructType = StructType(Array.empty[StructField])
}
