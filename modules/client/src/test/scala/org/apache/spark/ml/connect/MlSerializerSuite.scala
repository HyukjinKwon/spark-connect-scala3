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
package org.apache.spark.ml.connect

import org.apache.spark.connect.proto
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{
  DenseMatrix,
  DenseVector,
  Matrices,
  SparseMatrix,
  SparseVector,
  Vectors
}
import org.apache.spark.ml.param.ParamMap

class MlSerializerSuite extends munit.FunSuite {

  private def roundTrip(value: Any): Any =
    MlSerializer.decodeValue(MlSerializer.encodeValue(value))

  test("scalars round-trip with their types") {
    assertEquals(roundTrip(3), 3)
    assertEquals(roundTrip(2L), 2L)
    assertEquals(roundTrip(0.5), 0.5)
    assertEquals(roundTrip(true), true)
    assertEquals(roundTrip("auto"), "auto")
  }

  test("arrays round-trip as ordinary array literals") {
    val doubles = MlSerializer.encodeValue(Array(1.0, 2.5))
    assert(doubles.literalType.isArray, "top-level arrays are array literals")
    assertEquals(roundTrip(Array(1.0, 2.5)).asInstanceOf[Array[Double]].toSeq, Seq(1.0, 2.5))
    assertEquals(roundTrip(Array(1, 2)).asInstanceOf[Array[Int]].toSeq, Seq(1, 2))
    assertEquals(roundTrip(Array("a", "b")).asInstanceOf[Array[String]].toSeq, Seq("a", "b"))
  }

  test("dense and sparse vectors encode as VectorUDT structs") {
    val dense = MlSerializer.encodeValue(Vectors.dense(1.0, 2.0))
    assert(dense.literalType.isStruct)
    assertEquals(
      dense.getStruct.getStructType.getUdt.getJvmClass,
      "org.apache.spark.ml.linalg.VectorUDT"
    )
    assertEquals(dense.getStruct.elements(0).getByte, 1)
    val back = roundTrip(Vectors.dense(1.0, 2.0))
    assertEquals(back, Vectors.dense(1.0, 2.0))
    val sparse = Vectors.sparse(4, Seq((1, 3.0)))
    val sparseBack = roundTrip(sparse).asInstanceOf[SparseVector]
    assertEquals(sparseBack.size, 4)
    assertEquals(sparseBack.indices.toSeq, Seq(1))
    assertEquals(sparseBack.values.toSeq, Seq(3.0))
  }

  test("dense and sparse matrices encode as MatrixUDT structs") {
    val dense = Matrices.dense(2, 2, Array(1.0, 2.0, 3.0, 4.0))
    val encoded = MlSerializer.encodeValue(dense)
    assertEquals(
      encoded.getStruct.getStructType.getUdt.getJvmClass,
      "org.apache.spark.ml.linalg.MatrixUDT"
    )
    assertEquals(roundTrip(dense).asInstanceOf[DenseMatrix].toArray.toSeq, dense.toArray.toSeq)
    val sparse = Matrices.sparse(2, 2, Array(0, 1, 2), Array(0, 1), Array(5.0, 6.0))
    assertEquals(roundTrip(sparse).asInstanceOf[SparseMatrix].toArray.toSeq, sparse.toArray.toSeq)
  }

  test("unsupported values are rejected instead of silently encoded") {
    intercept[IllegalArgumentException] {
      MlSerializer.encodeValue(new Object())
    }
  }

  test("method arguments use array literals with an element type") {
    val arg = MlSerializer.encodeArgument(Array(0.5, 1.5))
    assert(arg.literalType.isArray)
    assertEquals(arg.getArray.elements.length, 2)
    assert(arg.getArray.elementType.isDefined)
  }

  test("encodeParams sends only explicitly set parameters") {
    val lr = new LogisticRegression().setRegParam(0.25)
    val params = MlSerializer.encodeParams(lr)
    assertEquals(params.params.keySet, Set("regParam"))
    assertEquals(MlSerializer.decodeParams(params), Seq(("regParam", 0.25)))
  }

  test("encodeParamMap encodes the entries of a map") {
    val lr = new LogisticRegression()
    val params = MlSerializer.encodeParamMap(ParamMap(lr.maxIter -> 9))
    assertEquals(MlSerializer.decodeParams(params), Seq(("maxIter", 9)))
  }

  test("applyParams restores set values without re-validating them") {
    val lr = new LogisticRegression()
    val params = proto.MlParams(params =
      Map("maxIter" -> MlSerializer.encodeValue(12), "unknownParam" -> MlSerializer.encodeValue(1))
    )
    MlSerializer.applyParams(lr, params)
    assertEquals(lr.getMaxIter, 12)
    assert(lr.isSet(lr.maxIter))
  }
}
