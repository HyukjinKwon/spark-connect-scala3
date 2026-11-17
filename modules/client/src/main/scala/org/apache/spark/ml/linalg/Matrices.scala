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
  BooleanType,
  ByteType,
  DoubleType,
  IntegerType,
  StructField,
  StructType
}

/**
 * A matrix of doubles, mirroring `org.apache.spark.ml.linalg.Matrix`. Values are stored column
 * major unless `isTransposed` is set.
 */
sealed trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Whether the matrix is stored transposed (row major). */
  def isTransposed: Boolean

  /** Value at row `i`, column `j`. */
  def apply(i: Int, j: Int): Double

  /** Column-major array of all values. */
  def toArray: Array[Double] = {
    val data = new Array[Double](numRows * numCols)
    var j = 0
    while (j < numCols) {
      var i = 0
      while (i < numRows) {
        data(i + j * numRows) = apply(i, j)
        i += 1
      }
      j += 1
    }
    data
  }

  /** A dense copy of this matrix. */
  def toDense: DenseMatrix = new DenseMatrix(numRows, numCols, toArray)

  /** A sparse copy of this matrix. */
  def toSparse: SparseMatrix = {
    val colPtrs = new Array[Int](numCols + 1)
    val rowIndices = scala.collection.mutable.ArrayBuilder.make[Int]
    val values = scala.collection.mutable.ArrayBuilder.make[Double]
    var j = 0
    while (j < numCols) {
      var i = 0
      while (i < numRows) {
        val v = apply(i, j)
        if (v != 0.0) {
          rowIndices += i
          values += v
        }
        i += 1
      }
      colPtrs(j + 1) = rowIndices.result().length
      j += 1
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), values.result())
  }

  /** The transpose of this matrix. */
  def transpose: Matrix = {
    val data = new Array[Double](numRows * numCols)
    var i = 0
    while (i < numRows) {
      var j = 0
      while (j < numCols) {
        data(j + i * numCols) = apply(i, j)
        j += 1
      }
      i += 1
    }
    new DenseMatrix(numCols, numRows, data, isTransposed = true)
  }

  /** Number of explicitly stored (active) entries. */
  def numActives: Int

  /** Number of non-zero entries. */
  def numNonzeros: Int = toArray.count(_ != 0.0)

  override def equals(other: Any): Boolean = other match {
    case m: Matrix =>
      m.numRows == numRows && m.numCols == numCols && m.toArray.sameElements(toArray)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(toArray)
}

/**
 * A column-major dense matrix.
 *
 * @param isTransposed
 *   when true, `values` is stored row major
 */
final class DenseMatrix(
    val numRows: Int,
    val numCols: Int,
    val values: Array[Double],
    val isTransposed: Boolean = false
) extends Matrix {

  require(values.length == numRows * numCols, "values length must be numRows * numCols")

  override def apply(i: Int, j: Int): Double =
    if (isTransposed) values(i * numCols + j) else values(i + j * numRows)

  override def numActives: Int = values.length

  override def toString: String =
    s"DenseMatrix($numRows, $numCols, ${values.mkString("[", ",", "]")}, $isTransposed)"
}

/**
 * A compressed-sparse-column matrix. When `isTransposed` is set, `colPtrs` and `rowIndices`
 * describe rows instead.
 */
final class SparseMatrix(
    val numRows: Int,
    val numCols: Int,
    val colPtrs: Array[Int],
    val rowIndices: Array[Int],
    val values: Array[Double],
    val isTransposed: Boolean = false
) extends Matrix {

  override def apply(i: Int, j: Int): Double = {
    val (major, minor, ptrs) =
      if (isTransposed) (i, j, colPtrs) else (j, i, colPtrs)
    val start = ptrs(major)
    val end = ptrs(major + 1)
    val pos = java.util.Arrays.binarySearch(rowIndices, start, end, minor)
    if (pos >= 0) values(pos) else 0.0
  }

  override def numActives: Int = values.length

  override def toString: String =
    s"SparseMatrix($numRows, $numCols, ${colPtrs.mkString("[", ",", "]")}, " +
      s"${rowIndices.mkString("[", ",", "]")}, ${values.mkString("[", ",", "]")}, $isTransposed)"
}

/** Factory methods for [[Matrix]], mirroring `org.apache.spark.ml.linalg.Matrices`. */
object Matrices {

  /** A column-major dense matrix. */
  def dense(numRows: Int, numCols: Int, values: Array[Double]): Matrix =
    new DenseMatrix(numRows, numCols, values)

  /** A compressed-sparse-column matrix. */
  def sparse(
      numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],
      rowIndices: Array[Int],
      values: Array[Double]
  ): Matrix =
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)

  /** A dense matrix of zeros. */
  def zeros(numRows: Int, numCols: Int): Matrix =
    new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))

  /** A dense matrix of ones. */
  def ones(numRows: Int, numCols: Int): Matrix =
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))

  /** A dense identity matrix. */
  def eye(n: Int): Matrix = {
    val values = new Array[Double](n * n)
    var i = 0
    while (i < n) {
      values(i * n + i) = 1.0
      i += 1
    }
    new DenseMatrix(n, n, values)
  }

  /** A square diagonal matrix with the entries of `vector` on the diagonal. */
  def diag(vector: Vector): Matrix = {
    val n = vector.size
    val values = new Array[Double](n * n)
    var i = 0
    while (i < n) {
      values(i * n + i) = vector(i)
      i += 1
    }
    new DenseMatrix(n, n, values)
  }
}

/**
 * Codec between [[Matrix]] and the `MatrixUDT` struct: `(type: byte, numRows: int, numCols: int,
 * colPtrs: array<int>, rowIndices: array<int>, values: array<double>, isTransposed: boolean)`,
 * where type 0 is sparse and 1 is dense.
 */
object MatrixUDT {

  /** The Spark SQL struct that backs `MatrixUDT`. */
  val sqlType: StructType = StructType(
    Seq(
      StructField("type", ByteType, nullable = false),
      StructField("numRows", IntegerType, nullable = false),
      StructField("numCols", IntegerType, nullable = false),
      StructField("colPtrs", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("rowIndices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("isTransposed", BooleanType, nullable = false)
    )
  )

  /** Encodes `m` as a struct row matching [[sqlType]]. */
  def serialize(m: Matrix): Row = m match {
    case s: SparseMatrix =>
      Row(
        0.toByte,
        s.numRows,
        s.numCols,
        s.colPtrs.toSeq,
        s.rowIndices.toSeq,
        s.values.toSeq,
        s.isTransposed
      )
    case d: DenseMatrix =>
      Row(1.toByte, d.numRows, d.numCols, null, null, d.values.toSeq, d.isTransposed)
  }

  /** Decodes a struct row matching [[sqlType]]. */
  def deserialize(row: Row): Matrix = {
    def ints(i: Int): Array[Int] = row.getSeq[Any](i).map(_.asInstanceOf[Number].intValue()).toArray
    def doubles(i: Int): Array[Double] =
      row.getSeq[Any](i).map(_.asInstanceOf[Number].doubleValue()).toArray
    row.getByte(0) match {
      case 0 =>
        new SparseMatrix(
          row.getInt(1),
          row.getInt(2),
          ints(3),
          ints(4),
          doubles(5),
          row.getBoolean(6)
        )
      case 1 =>
        new DenseMatrix(row.getInt(1), row.getInt(2), doubles(5), row.getBoolean(6))
      case other => throw new IllegalArgumentException(s"Unknown Matrix type $other")
    }
  }
}
