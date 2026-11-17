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

package org.apache.spark.ml.linalg

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  ArrayType,
  ByteType,
  DoubleType,
  IntegerType,
  StructField,
  StructType
}

/**
 * A vector of doubles, mirroring `org.apache.spark.ml.linalg.Vector`. Vectors are produced by ML
 * model attributes and transformed columns, and are sent to the server as `VectorUDT` structs.
 */
sealed trait Vector extends Serializable {

  /** Number of elements. */
  def size: Int

  /** Converts the vector to a dense array. */
  def toArray: Array[Double]

  /** Value at index `i`. */
  def apply(i: Int): Double

  /** A deep copy of this vector. */
  def copy: Vector

  /** Number of explicitly stored (active) entries. */
  def numActives: Int

  /** Number of non-zero entries. */
  def numNonzeros: Int

  /** A dense copy of this vector. */
  def toDense: DenseVector = new DenseVector(toArray)

  /** A sparse copy of this vector. */
  def toSparse: SparseVector = {
    val nnz = numNonzeros
    val ii = new Array[Int](nnz)
    val vv = new Array[Double](nnz)
    var k = 0
    foreachActive { (i, v) =>
      if (v != 0.0) {
        ii(k) = i
        vv(k) = v
        k += 1
      }
    }
    new SparseVector(size, ii, vv)
  }

  /** The dense or sparse representation, whichever stores fewer values. */
  def compressed: Vector = {
    val nnz = numNonzeros
    // A sparse entry costs an index and a value, so sparse wins below half density.
    if (1.5 * (nnz + 1.0) < size) toSparse else toDense
  }

  /** Index of the maximum value (the first one on ties). */
  def argmax: Int =
    if (size == 0) -1
    else {
      var maxIdx = 0
      var maxValue = apply(0)
      var i = 1
      while (i < size) {
        val v = apply(i)
        if (v > maxValue) {
          maxIdx = i
          maxValue = v
        }
        i += 1
      }
      maxIdx
    }

  /** Calls `f(index, value)` for every active entry. */
  def foreachActive(f: (Int, Double) => Unit): Unit

  override def equals(other: Any): Boolean = other match {
    case v: Vector => v.size == size && v.toArray.sameElements(toArray)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(toArray)
}

/** A dense vector backed by an array of doubles. */
final class DenseVector(val values: Array[Double]) extends Vector {

  override def size: Int = values.length

  override def toArray: Array[Double] = values

  override def apply(i: Int): Double = values(i)

  override def copy: DenseVector = new DenseVector(values.clone())

  override def numActives: Int = size

  override def numNonzeros: Int = values.count(_ != 0.0)

  override def foreachActive(f: (Int, Double) => Unit): Unit = {
    var i = 0
    while (i < values.length) {
      f(i, values(i))
      i += 1
    }
  }

  override def toString: String = values.mkString("[", ",", "]")
}

/**
 * A sparse vector with sorted `indices` and matching `values`.
 *
 * @param size
 *   the length of the vector
 * @param indices
 *   the indices of the active entries, in increasing order
 * @param values
 *   the values of the active entries
 */
final class SparseVector(override val size: Int, val indices: Array[Int], val values: Array[Double])
    extends Vector {

  require(indices.length == values.length, "indices and values must have the same length")

  override def toArray: Array[Double] = {
    val data = new Array[Double](size)
    var k = 0
    while (k < indices.length) {
      data(indices(k)) = values(k)
      k += 1
    }
    data
  }

  override def apply(i: Int): Double = {
    val pos = java.util.Arrays.binarySearch(indices, i)
    if (pos >= 0) values(pos) else 0.0
  }

  override def copy: SparseVector = new SparseVector(size, indices.clone(), values.clone())

  override def numActives: Int = values.length

  override def numNonzeros: Int = values.count(_ != 0.0)

  override def foreachActive(f: (Int, Double) => Unit): Unit = {
    var k = 0
    while (k < indices.length) {
      f(indices(k), values(k))
      k += 1
    }
  }

  override def toString: String =
    s"($size,${indices.mkString("[", ",", "]")},${values.mkString("[", ",", "]")})"
}

/** Factory methods for [[Vector]], mirroring `org.apache.spark.ml.linalg.Vectors`. */
object Vectors {

  /** A dense vector from its values. */
  def dense(values: Array[Double]): Vector = new DenseVector(values)

  /** A dense vector from its values. */
  def dense(firstValue: Double, otherValues: Double*): Vector =
    new DenseVector((firstValue +: otherValues).toArray)

  /** A sparse vector from its size, sorted indices, and values. */
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector = {
    require(size >= 0, "size must be non-negative")
    new SparseVector(size, indices, values)
  }

  /** A sparse vector from its size and `(index, value)` pairs. */
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {
    val sorted = elements.sortBy(_._1)
    new SparseVector(size, sorted.map(_._1).toArray, sorted.map(_._2).toArray)
  }

  /** A zero vector of the given size (dense). */
  def zeros(size: Int): Vector = new DenseVector(new Array[Double](size))

  /** The p-norm of `v` (`p >= 1`, or `Double.PositiveInfinity` for the max norm). */
  def norm(v: Vector, p: Double): Double = {
    require(p >= 1.0, "p must be at least 1")
    var sum = 0.0
    if (p == 1.0) {
      v.foreachActive((_, x) => sum += math.abs(x))
      sum
    } else if (p == 2.0) {
      v.foreachActive((_, x) => sum += x * x)
      math.sqrt(sum)
    } else if (p == Double.PositiveInfinity) {
      v.foreachActive((_, x) => sum = math.max(sum, math.abs(x)))
      sum
    } else {
      v.foreachActive((_, x) => sum += math.pow(math.abs(x), p))
      math.pow(sum, 1.0 / p)
    }
  }

  /** The squared Euclidean distance between two vectors of the same size. */
  def sqdist(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: ${v1.size} vs ${v2.size}")
    val a = v1.toArray
    val b = v2.toArray
    var sum = 0.0
    var i = 0
    while (i < a.length) {
      val d = a(i) - b(i)
      sum += d * d
      i += 1
    }
    sum
  }
}

/**
 * Codec between [[Vector]] and the `VectorUDT` struct the server uses on the wire and in Arrow
 * results: `(type: byte, size: int, indices: array<int>, values: array<double>)`, where type 0 is
 * sparse and 1 is dense. Use it to decode `Row`s read from a vector column.
 */
object VectorUDT {

  /** The Spark SQL struct that backs `VectorUDT`. */
  val sqlType: StructType = StructType(
    Seq(
      StructField("type", ByteType, nullable = false),
      StructField("size", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)
    )
  )

  /** Encodes `v` as a struct row matching [[sqlType]]. */
  def serialize(v: Vector): Row = v match {
    case s: SparseVector => Row(0.toByte, s.size, s.indices.toSeq, s.values.toSeq)
    case d: DenseVector => Row(1.toByte, null, null, d.values.toSeq)
  }

  /** Decodes a struct row matching [[sqlType]]. */
  def deserialize(row: Row): Vector = row.getByte(0) match {
    case 0 =>
      val size = row.getInt(1)
      val indices = row.getSeq[Any](2).map(_.asInstanceOf[Number].intValue()).toArray
      val values = row.getSeq[Any](3).map(_.asInstanceOf[Number].doubleValue()).toArray
      new SparseVector(size, indices, values)
    case 1 =>
      new DenseVector(row.getSeq[Any](3).map(_.asInstanceOf[Number].doubleValue()).toArray)
    case other => throw new IllegalArgumentException(s"Unknown Vector type $other")
  }
}
