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

class VectorsSuite extends munit.FunSuite {

  test("dense and sparse vectors agree on values, size and equality") {
    val dense = Vectors.dense(0.0, 2.0, 0.0, 4.0)
    val sparse = Vectors.sparse(4, Seq((1, 2.0), (3, 4.0)))
    assertEquals(dense.size, 4)
    assertEquals(sparse.size, 4)
    assertEquals(sparse.toArray.toSeq, Seq(0.0, 2.0, 0.0, 4.0))
    assertEquals(sparse(3), 4.0)
    assertEquals(sparse(0), 0.0)
    assertEquals(dense, sparse)
    assertEquals(sparse.numActives, 2)
    assertEquals(dense.numNonzeros, 2)
    assertEquals(dense.argmax, 3)
  }

  test("toSparse, toDense and compressed choose the representation") {
    val dense = Vectors.dense(0.0, 0.0, 0.0, 5.0, 0.0, 0.0, 0.0, 0.0)
    assert(dense.toSparse.isInstanceOf[SparseVector])
    assertEquals(dense.toSparse.indices.toSeq, Seq(3))
    assert(dense.compressed.isInstanceOf[SparseVector])
    assert(Vectors.dense(1.0, 2.0).compressed.isInstanceOf[DenseVector])
  }

  test("sparse vectors reject mismatched indices and values") {
    intercept[IllegalArgumentException] {
      new SparseVector(3, Array(0, 1), Array(1.0))
    }
  }

  test("norm, sqdist and zeros") {
    val v = Vectors.dense(3.0, 4.0)
    assertEqualsDouble(Vectors.norm(v, 2.0), 5.0, 1e-12)
    assertEqualsDouble(Vectors.norm(v, 1.0), 7.0, 1e-12)
    assertEqualsDouble(Vectors.norm(v, Double.PositiveInfinity), 4.0, 1e-12)
    assertEqualsDouble(Vectors.sqdist(v, Vectors.zeros(2)), 25.0, 1e-12)
    assertEquals(Vectors.zeros(3).toArray.toSeq, Seq(0.0, 0.0, 0.0))
  }

  test("toString matches Spark's format") {
    assertEquals(Vectors.dense(1.0, 2.5).toString, "[1.0,2.5]")
    assertEquals(Vectors.sparse(3, Seq((1, 2.0))).toString, "(3,[1],[2.0])")
  }

  test("dense matrices are column major unless transposed") {
    val m = Matrices.dense(2, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    assertEquals(m(0, 0), 1.0)
    assertEquals(m(1, 0), 2.0)
    assertEquals(m(0, 1), 3.0)
    assertEquals(m(1, 2), 6.0)
    val t = m.transpose
    assertEquals(t.numRows, 3)
    assertEquals(t(2, 1), 6.0)
    assertEquals(m.toArray.toSeq, Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
  }

  test("sparse matrices use compressed column storage") {
    val m = Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(7.0, 8.0))
    assertEquals(m(0, 0), 7.0)
    assertEquals(m(1, 1), 8.0)
    assertEquals(m(0, 1), 0.0)
    assertEquals(m.toDense.values.toSeq, Seq(7.0, 0.0, 0.0, 8.0))
    assertEquals(m.toDense.toSparse.numActives, 2)
  }

  test("eye, diag, ones and zeros") {
    assertEquals(Matrices.eye(2).toArray.toSeq, Seq(1.0, 0.0, 0.0, 1.0))
    assertEquals(Matrices.diag(Vectors.dense(2.0, 3.0)).toArray.toSeq, Seq(2.0, 0.0, 0.0, 3.0))
    assertEquals(Matrices.ones(1, 2).toArray.toSeq, Seq(1.0, 1.0))
    assertEquals(Matrices.zeros(1, 2).toArray.toSeq, Seq(0.0, 0.0))
  }

  test("VectorUDT round-trips dense and sparse vectors through struct rows") {
    val dense = Vectors.dense(1.5, -2.0)
    val sparse = Vectors.sparse(5, Seq((0, 1.0), (4, 9.0)))
    val denseRow = VectorUDT.serialize(dense)
    assertEquals(denseRow.getByte(0), 1.toByte)
    assertEquals(VectorUDT.deserialize(denseRow), dense)
    val sparseRow = VectorUDT.serialize(sparse)
    assertEquals(sparseRow.getByte(0), 0.toByte)
    assertEquals(sparseRow.getInt(1), 5)
    assert(VectorUDT.deserialize(sparseRow).isInstanceOf[SparseVector])
    assertEquals(VectorUDT.deserialize(sparseRow).toArray.toSeq, sparse.toArray.toSeq)
    assertEquals(VectorUDT.sqlType.fieldNames.toSeq, Seq("type", "size", "indices", "values"))
  }

  test("MatrixUDT round-trips dense and sparse matrices") {
    val dense = Matrices.dense(2, 2, Array(1.0, 2.0, 3.0, 4.0))
    assertEquals(
      MatrixUDT.deserialize(MatrixUDT.serialize(dense)).toArray.toSeq,
      dense.toArray.toSeq
    )
    val sparse = Matrices.sparse(2, 2, Array(0, 1, 2), Array(1, 0), Array(5.0, 6.0))
    val back = MatrixUDT.deserialize(MatrixUDT.serialize(sparse))
    assert(back.isInstanceOf[SparseMatrix])
    assertEquals(back.toArray.toSeq, sparse.toArray.toSeq)
    assertEquals(MatrixUDT.sqlType.fieldNames.length, 7)
  }

  test("unknown vector type tags are rejected") {
    intercept[IllegalArgumentException] {
      VectorUDT.deserialize(Row(7.toByte, null, null, Seq(1.0)))
    }
  }
}
