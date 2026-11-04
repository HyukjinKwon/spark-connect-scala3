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

import scala.compiletime.constValueTuple
import scala.compiletime.summonAll
import scala.deriving.Mirror
import scala.reflect.ClassTag

import org.apache.spark.sql.types._

/**
 * Converts between JVM values of type `T` and the [[Row]] representation produced and consumed by
 * Spark Connect. Encoders are derived at compile time for primitives, `Option`, collections, maps,
 * tuples, and case classes, so `df.as[Person]` and `spark.createDataset(people)` work with no
 * server-side closures.
 *
 * Encoders intentionally do NOT cover
 * `Dataset.map`/`flatMap`/`groupByKey`/`reduce`/`mapPartitions`: those evaluate a JVM closure per
 * row on the server (the same mechanism as a UDF), which Spark Connect for Scala 3 does not
 * support.
 */
trait Encoder[T] extends Serializable {

  /** The Spark data type of `T`. */
  def dataType: DataType

  /** Whether `T` admits null / `None`. */
  def nullable: Boolean = false

  /** Runtime class tag for `T`, used to materialize typed arrays from `collect()`. */
  def classTag: ClassTag[T]

  /** Deserializes a decoded Catalyst value (primitive, [[Row]], `Seq`, or `Map`) into `T`. */
  private[sql] def fromValue(value: Any): T

  /** Serializes `T` back into a Catalyst value for shipping local data to the server. */
  private[sql] def toValue(value: T): Any

  /** True if `T` is encoded as a struct (a case class, tuple, or [[Row]]). */
  private[sql] def isStruct: Boolean = dataType.isInstanceOf[StructType]

  /** The struct schema of a top-level Dataset of `T`. */
  private[sql] def schema: StructType = dataType match {
    case s: StructType => s
    case other => StructType(Array(StructField("value", other, nullable)))
  }

  /** Deserializes one result row into `T`. */
  private[sql] def fromRow(row: Row): T = dataType match {
    case _: StructType => fromValue(row)
    case _ => fromValue(row.get(0))
  }

  /** Serializes `T` into a [[Row]] matching [[schema]]. */
  private[sql] def toRow(value: T): Row = toValue(value) match {
    case r: Row => r
    case other => Row(other)
  }
}

/**
 * Companion holding the derived [[Encoder]] instances. Because they live here, `df.as[T]` and
 * `spark.createDataset` resolve an encoder for any supported `T` with no explicit import.
 */
object Encoder {

  def apply[T](using e: Encoder[T]): Encoder[T] = e

  /** Builds an atomic encoder from a data type and a pair of conversions. */
  private[sql] def atomic[T](
      dt: DataType,
      from: Any => T,
      to: T => Any = (t: T) => t.asInstanceOf[Any],
      isNullable: Boolean = false
  )(using ct: ClassTag[T]): Encoder[T] =
    new Encoder[T] {
      override def dataType: DataType = dt
      override def nullable: Boolean = isNullable
      override def classTag: ClassTag[T] = ct
      override private[sql] def fromValue(value: Any): T = from(value)
      override private[sql] def toValue(value: T): Any = to(value)
    }

  private def num(value: Any): java.lang.Number = value.asInstanceOf[java.lang.Number]

  // -- Primitive / atomic encoders -------------------------------------------

  given Encoder[Int] = atomic(IntegerType, v => num(v).intValue)
  given Encoder[Long] = atomic(LongType, v => num(v).longValue)
  given Encoder[Short] = atomic(ShortType, v => num(v).shortValue)
  given Encoder[Byte] = atomic(ByteType, v => num(v).byteValue)
  given Encoder[Double] = atomic(DoubleType, v => num(v).doubleValue)
  given Encoder[Float] = atomic(FloatType, v => num(v).floatValue)
  given Encoder[Boolean] = atomic(BooleanType, v => v.asInstanceOf[Boolean])
  // Reference-typed atomics admit null (as in Spark, where only JVM primitives are non-nullable).
  given Encoder[String] =
    atomic(StringType, v => if (v == null) null else v.toString, isNullable = true)
  given Encoder[Array[Byte]] =
    atomic(BinaryType, v => v.asInstanceOf[Array[Byte]], isNullable = true)
  given javaBigDecimalEncoder: Encoder[java.math.BigDecimal] =
    atomic(
      DecimalType.SYSTEM_DEFAULT,
      v => v.asInstanceOf[java.math.BigDecimal],
      isNullable = true
    )
  given scalaBigDecimalEncoder: Encoder[BigDecimal] =
    atomic(
      DecimalType.SYSTEM_DEFAULT,
      v => BigDecimal(v.asInstanceOf[java.math.BigDecimal]),
      (b: BigDecimal) => b.bigDecimal,
      isNullable = true
    )
  given Encoder[java.sql.Date] =
    atomic(
      DateType,
      value =>
        value match
          case date: java.time.LocalDate => SqlTimestampConversion.toJavaDate(date)
          case other => other.asInstanceOf[java.sql.Date],
      isNullable = true
    )
  given Encoder[java.time.LocalDate] =
    atomic(DateType, v => v.asInstanceOf[java.time.LocalDate], isNullable = true)
  given Encoder[java.sql.Timestamp] =
    atomic(
      TimestampType,
      value =>
        value match
          case instant: java.time.Instant => SqlTimestampConversion.toJavaTimestamp(instant)
          case other => other.asInstanceOf[java.sql.Timestamp],
      isNullable = true
    )
  given Encoder[java.time.Instant] =
    atomic(TimestampType, v => v.asInstanceOf[java.time.Instant], isNullable = true)
  given Encoder[java.time.LocalDateTime] =
    atomic(TimestampNTZType, v => v.asInstanceOf[java.time.LocalDateTime], isNullable = true)

  given Encoder[java.time.LocalTime] =
    atomic(TimeType(), v => v.asInstanceOf[java.time.LocalTime], isNullable = true)
  given Encoder[java.time.Duration] =
    atomic(DayTimeIntervalType(), v => v.asInstanceOf[java.time.Duration], isNullable = true)
  given Encoder[java.time.Period] =
    atomic(YearMonthIntervalType(), v => v.asInstanceOf[java.time.Period], isNullable = true)

  // -- Boxed Java primitives (nullable, as in Apache Spark) -------------------

  private def boxed[T](dt: DataType, from: Any => T)(using ClassTag[T]): Encoder[T] =
    atomic(dt, v => if (v == null) null.asInstanceOf[T] else from(v), isNullable = true)

  given boxedLongEncoder: Encoder[java.lang.Long] =
    boxed(LongType, v => java.lang.Long.valueOf(num(v).longValue))
  given boxedIntegerEncoder: Encoder[java.lang.Integer] =
    boxed(IntegerType, v => java.lang.Integer.valueOf(num(v).intValue))
  given boxedShortEncoder: Encoder[java.lang.Short] =
    boxed(ShortType, v => java.lang.Short.valueOf(num(v).shortValue))
  given boxedByteEncoder: Encoder[java.lang.Byte] =
    boxed(ByteType, v => java.lang.Byte.valueOf(num(v).byteValue))
  given boxedDoubleEncoder: Encoder[java.lang.Double] =
    boxed(DoubleType, v => java.lang.Double.valueOf(num(v).doubleValue))
  given boxedFloatEncoder: Encoder[java.lang.Float] =
    boxed(FloatType, v => java.lang.Float.valueOf(num(v).floatValue))
  given boxedBooleanEncoder: Encoder[java.lang.Boolean] =
    boxed(BooleanType, v => java.lang.Boolean.valueOf(v.asInstanceOf[Boolean]))

  /** Identity encoder used for `Dataset[Row]` (DataFrames). */
  private[sql] val rowEncoder: Encoder[Row] = new Encoder[Row] {
    override def dataType: DataType = StructType(Array.empty[StructField])
    override def nullable: Boolean = true
    override def classTag: ClassTag[Row] = ClassTag(classOf[Row])
    override private[sql] def fromValue(value: Any): Row = value.asInstanceOf[Row]
    override private[sql] def toValue(value: Row): Any = value
    override private[sql] def fromRow(row: Row): Row = row
    override private[sql] def toRow(value: Row): Row = value
  }
  given Encoder[Row] = rowEncoder

  // -- Option / collections / map --------------------------------------------

  given optionEncoder[A](using e: Encoder[A]): Encoder[Option[A]] = new Encoder[Option[A]] {
    override def dataType: DataType = e.dataType
    override def nullable: Boolean = true
    override def classTag: ClassTag[Option[A]] = ClassTag(classOf[Option[A]])
    override private[sql] def fromValue(value: Any): Option[A] =
      if (value == null) None else Some(e.fromValue(value))
    override private[sql] def toValue(value: Option[A]): Any = value.map(e.toValue).orNull
  }

  given seqEncoder[A](using e: Encoder[A]): Encoder[Seq[A]] = new Encoder[Seq[A]] {
    override def dataType: DataType = ArrayType(e.dataType, e.nullable)
    override def classTag: ClassTag[Seq[A]] = ClassTag(classOf[Seq[A]])
    override private[sql] def fromValue(value: Any): Seq[A] =
      value.asInstanceOf[scala.collection.Seq[Any]].map(e.fromValue).toSeq
    override private[sql] def toValue(value: Seq[A]): Any = value.map(e.toValue)
  }

  given listEncoder[A](using e: Encoder[A]): Encoder[List[A]] = new Encoder[List[A]] {
    override def dataType: DataType = ArrayType(e.dataType, e.nullable)
    override def classTag: ClassTag[List[A]] = ClassTag(classOf[List[A]])
    override private[sql] def fromValue(value: Any): List[A] =
      value.asInstanceOf[scala.collection.Seq[Any]].map(e.fromValue).toList
    override private[sql] def toValue(value: List[A]): Any = value.map(e.toValue)
  }

  given arrayEncoder[A](using e: Encoder[A], ct: ClassTag[A]): Encoder[Array[A]] =
    new Encoder[Array[A]] {
      override def dataType: DataType = ArrayType(e.dataType, e.nullable)
      override def nullable: Boolean = true
      override def classTag: ClassTag[Array[A]] = ct.wrap
      override private[sql] def fromValue(value: Any): Array[A] =
        if (value == null) null
        else value.asInstanceOf[scala.collection.Seq[Any]].map(e.fromValue).toArray(using ct)
      override private[sql] def toValue(value: Array[A]): Any =
        if (value == null) null else value.toSeq.map(e.toValue)
    }

  given mapEncoder[K, V](using ek: Encoder[K], ev: Encoder[V]): Encoder[Map[K, V]] =
    new Encoder[Map[K, V]] {
      override def dataType: DataType = MapType(ek.dataType, ev.dataType, ev.nullable)
      override def classTag: ClassTag[Map[K, V]] = ClassTag(classOf[Map[K, V]])
      override private[sql] def fromValue(value: Any): Map[K, V] =
        value
          .asInstanceOf[scala.collection.Map[Any, Any]]
          .map { case (k, v) => ek.fromValue(k) -> ev.fromValue(v) }
          .toMap
      override private[sql] def toValue(value: Map[K, V]): Any =
        value.map { case (k, v) => ek.toValue(k) -> ev.toValue(v) }
    }

  // -- Encoders composed at runtime ------------------------------------------

  /**
   * Decodes the first (only) column of each row with `enc`, for `Dataset.select(typedColumn)`:
   * unlike a top-level `as[T]`, a struct-typed value arrives as a single nested struct column.
   */
  private[sql] def columnValue[U](enc: Encoder[U]): Encoder[U] = new Encoder[U] {
    override def dataType: DataType = enc.dataType
    override def nullable: Boolean = enc.nullable
    override def classTag: ClassTag[U] = enc.classTag
    override private[sql] def fromValue(value: Any): U = enc.fromValue(value)
    override private[sql] def toValue(value: U): Any = enc.toValue(value)
    override private[sql] def schema: StructType =
      StructType(Array(StructField("value", enc.dataType, enc.nullable)))
    override private[sql] def fromRow(row: Row): U = enc.fromValue(row.get(0))
    override private[sql] def toRow(value: U): Row = Row(enc.toValue(value))
  }

  /**
   * A tuple encoder whose `i`-th element is decoded from the `i`-th column with `encoders(i)`
   * (fields `_1`, `_2`, ...). Used by the typed `Dataset.select` overloads and `joinWith`, where an
   * element may be `null` (e.g. the non-matching side of an outer join).
   */
  private[sql] def tuple[T](
      encoders: Seq[Encoder[?]],
      elementNullable: Seq[Boolean] = Nil
  ): Encoder[T] = new Encoder[T] {
    private val encs = encoders.map(_.asInstanceOf[Encoder[Any]]).toArray
    private val tupleClass: Class[?] = Class.forName(s"scala.Tuple${encs.length}")

    override def dataType: DataType =
      StructType(encs.zipWithIndex.map { case (e, i) =>
        StructField(
          s"_${i + 1}",
          e.dataType,
          elementNullable.lift(i).getOrElse(false) || e.nullable
        )
      })
    override def classTag: ClassTag[T] = ClassTag(tupleClass)

    override private[sql] def fromValue(value: Any): T = {
      val row = value.asInstanceOf[Row]
      val decoded = Array.tabulate[Any](encs.length) { i =>
        if (row.isNullAt(i)) null else encs(i).fromValue(row.get(i))
      }
      Tuple.fromArray(decoded).asInstanceOf[T]
    }

    override private[sql] def toValue(value: T): Any = {
      val product = value.asInstanceOf[Product]
      Row.fromSeq(
        Array
          .tabulate[Any](encs.length) { i =>
            val v = product.productElement(i)
            if (v == null) null else encs(i).toValue(v)
          }
          .toIndexedSeq
      )
    }
  }

  // -- Product (tuples and case classes) via derivation ----------------------

  inline given derived[T](using m: Mirror.ProductOf[T], ct: ClassTag[T]): Encoder[T] = {
    val labels = constValueTuple[m.MirroredElemLabels].toArray.map(_.toString)
    val elemEncoders =
      summonAll[Tuple.Map[m.MirroredElemTypes, Encoder]].toArray.map(_.asInstanceOf[Encoder[Any]])
    productEncoder[T](labels, elemEncoders, m, ct)
  }

  private[sql] def productEncoder[T](
      labels: Array[String],
      elemEncoders: Array[Encoder[Any]],
      m: Mirror.ProductOf[T],
      ct: ClassTag[T]
  ): Encoder[T] = new Encoder[T] {
    override def dataType: DataType =
      StructType(labels.zip(elemEncoders).map { case (name, enc) =>
        StructField(name, enc.dataType, enc.nullable)
      })
    override def classTag: ClassTag[T] = ct

    override private[sql] def fromValue(value: Any): T = {
      val row = value.asInstanceOf[Row]
      val decoded = Array.tabulate(elemEncoders.length)(i => elemEncoders(i).fromValue(row.get(i)))
      m.fromProduct(Tuple.fromArray(decoded))
    }

    override private[sql] def toValue(value: T): Any = {
      val product = value.asInstanceOf[Product]
      Row.fromSeq(
        Array
          .tabulate(elemEncoders.length)(i => elemEncoders(i).toValue(product.productElement(i)))
          .toIndexedSeq
      )
    }
  }
}

/** Spark-conventional alias; re-exports the derived [[Encoder]] instances and factory. */
object Encoders {
  export Encoder.{apply, rowEncoder}

  // The factory methods below mirror `org.apache.spark.sql.Encoders`. `bean`, `kryo`,
  // `javaSerialization`, and `udt` are not provided: they need server-side (de)serialization of
  // user classes, which Spark Connect for Scala 3 does not ship.

  def BOOLEAN: Encoder[java.lang.Boolean] = Encoder.boxedBooleanEncoder
  def BYTE: Encoder[java.lang.Byte] = Encoder.boxedByteEncoder
  def SHORT: Encoder[java.lang.Short] = Encoder.boxedShortEncoder
  def INT: Encoder[java.lang.Integer] = Encoder.boxedIntegerEncoder
  def LONG: Encoder[java.lang.Long] = Encoder.boxedLongEncoder
  def FLOAT: Encoder[java.lang.Float] = Encoder.boxedFloatEncoder
  def DOUBLE: Encoder[java.lang.Double] = Encoder.boxedDoubleEncoder
  def STRING: Encoder[String] = summon[Encoder[String]]
  def DECIMAL: Encoder[java.math.BigDecimal] = summon[Encoder[java.math.BigDecimal]]
  def DATE: Encoder[java.sql.Date] = summon[Encoder[java.sql.Date]]
  def LOCALDATE: Encoder[java.time.LocalDate] = summon[Encoder[java.time.LocalDate]]
  def LOCALDATETIME: Encoder[java.time.LocalDateTime] = summon[Encoder[java.time.LocalDateTime]]
  def TIMESTAMP: Encoder[java.sql.Timestamp] = summon[Encoder[java.sql.Timestamp]]
  def INSTANT: Encoder[java.time.Instant] = summon[Encoder[java.time.Instant]]
  def LOCALTIME: Encoder[java.time.LocalTime] = summon[Encoder[java.time.LocalTime]]
  def BINARY: Encoder[Array[Byte]] = summon[Encoder[Array[Byte]]]
  def DURATION: Encoder[java.time.Duration] = summon[Encoder[java.time.Duration]]
  def PERIOD: Encoder[java.time.Period] = summon[Encoder[java.time.Period]]

  def scalaInt: Encoder[Int] = summon[Encoder[Int]]
  def scalaLong: Encoder[Long] = summon[Encoder[Long]]
  def scalaDouble: Encoder[Double] = summon[Encoder[Double]]
  def scalaFloat: Encoder[Float] = summon[Encoder[Float]]
  def scalaByte: Encoder[Byte] = summon[Encoder[Byte]]
  def scalaShort: Encoder[Short] = summon[Encoder[Short]]
  def scalaBoolean: Encoder[Boolean] = summon[Encoder[Boolean]]

  /** An encoder for [[Row]]s of the given schema. */
  def row(schema: StructType): Encoder[Row] = rowOf(schema)

  private def rowOf(rowSchema: StructType): Encoder[Row] = new Encoder[Row] {
    override def dataType: DataType = rowSchema
    override def nullable: Boolean = true
    override def classTag: ClassTag[Row] = ClassTag(classOf[Row])
    override private[sql] def fromValue(value: Any): Row = value.asInstanceOf[Row]
    override private[sql] def toValue(value: Row): Any = value
    override private[sql] def fromRow(row: Row): Row = row
    override private[sql] def toRow(value: Row): Row = value
  }

  /** The derived encoder of a case class or tuple `T`. */
  def product[T <: Product](using enc: Encoder[T]): Encoder[T] = enc

  def tuple[T1](e1: Encoder[T1]): Encoder[Tuple1[T1]] = Encoder.tuple(Seq(e1))
  def tuple[T1, T2](e1: Encoder[T1], e2: Encoder[T2]): Encoder[(T1, T2)] =
    Encoder.tuple(Seq(e1, e2))
  def tuple[T1, T2, T3](e1: Encoder[T1], e2: Encoder[T2], e3: Encoder[T3]): Encoder[(T1, T2, T3)] =
    Encoder.tuple(Seq(e1, e2, e3))
  def tuple[T1, T2, T3, T4](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4]
  ): Encoder[(T1, T2, T3, T4)] = Encoder.tuple(Seq(e1, e2, e3, e4))
  def tuple[T1, T2, T3, T4, T5](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4],
      e5: Encoder[T5]
  ): Encoder[(T1, T2, T3, T4, T5)] = Encoder.tuple(Seq(e1, e2, e3, e4, e5))
}
