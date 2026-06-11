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

import scala.language.implicitConversions

import org.apache.spark.sql.types._

/**
 * Implicit conversions available via `import spark.implicits.*`: the `$"columnName"` column syntax
 * and a `.toDF` method on local sequences of values, tuples, or case classes.
 *
 * {{{
 *   import spark.implicits.*
 *   val df = Seq((1, "a"), (2, "b")).toDF("id", "name")
 *   df.select($"id", $"name")
 * }}}
 */
class SQLImplicits private[sql] (session: SparkSession) {

  /** Converts a `$"col name"` string interpolation into a [[Column]]. */
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): Column = functions.col(sc.s(args: _*))
  }

  /** Adds `.toDF` to a local sequence of values, tuples, or case classes. */
  implicit def localSeqToDataFrameHolder[A](seq: Seq[A]): DataFrameHolder =
    new DataFrameHolder(session, seq)
}

/**
 * Turns a local sequence into a [[DataFrame]]. `Product` elements (tuples / case classes) become
 * multi-column rows; any other value becomes a single column. Column types are inferred from the
 * first row.
 */
final class DataFrameHolder private[sql] (session: SparkSession, data: Seq[?]) {

  /** Convert to a DataFrame; with no names the columns are `_1`, `_2`, ... */
  def toDF(colNames: String*): DataFrame = {
    val rows = data.map {
      case r: Row => r
      case p: Product => Row.fromSeq(p.productIterator.toIndexedSeq)
      case v => Row(v)
    }
    val arity = rows.headOption.map(_.length).getOrElse(colNames.length)
    val names = if (colNames.nonEmpty) colNames else (1 to arity).map(i => s"_$i")
    val first = rows.headOption
    val fields = names.zipWithIndex.map { case (name, i) =>
      val dt = first.map(r => DataFrameHolder.inferType(r.get(i))).getOrElse(StringType)
      StructField(name, dt, nullable = true)
    }
    session.createDataFrame(rows, StructType(fields.toArray))
  }
}

private object DataFrameHolder {
  def inferType(value: Any): DataType = value match {
    case null => StringType
    case _: Boolean => BooleanType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
    case _: BigDecimal => DecimalType.SYSTEM_DEFAULT
    case _: Array[Byte] => BinaryType
    case _: String => StringType
    case _: java.sql.Date => DateType
    case _: java.time.LocalDate => DateType
    case _: java.sql.Timestamp => TimestampType
    case _: java.time.Instant => TimestampType
    case _ => StringType
  }
}
