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
  // Reference-typed atomics admit null (matching Spark, where only JVM primitives are non-nullable).
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
    atomic(DateType, v => v.asInstanceOf[java.sql.Date], isNullable = true)
  given Encoder[java.time.LocalDate] =
    atomic(DateType, v => v.asInstanceOf[java.time.LocalDate], isNullable = true)
  given Encoder[java.sql.Timestamp] =
    atomic(TimestampType, v => v.asInstanceOf[java.sql.Timestamp], isNullable = true)
  given Encoder[java.time.Instant] =
    atomic(TimestampType, v => v.asInstanceOf[java.time.Instant], isNullable = true)
  given Encoder[java.time.LocalDateTime] =
    atomic(TimestampNTZType, v => v.asInstanceOf[java.time.LocalDateTime], isNullable = true)

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
}
