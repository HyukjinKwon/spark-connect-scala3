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
import org.apache.spark.ml.linalg.{
  DenseMatrix,
  DenseVector,
  Matrix,
  SparseMatrix,
  SparseVector,
  Vector
}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter

/**
 * Encodes parameter values as `Expression.Literal` protos and decodes them back, following the
 * server's `Serializer` and `MLUtils` so both ends agree on the wire format.
 *
 * Vectors and matrices travel as structs tagged with their `VectorUDT`/`MatrixUDT` class name.
 * Numeric arrays travel as specialized arrays.
 */
private[spark] object MlSerializer {

  private val VectorUDTClass = "org.apache.spark.ml.linalg.VectorUDT"
  private val MatrixUDTClass = "org.apache.spark.ml.linalg.MatrixUDT"

  private def udt(jvmClass: String): proto.DataType =
    proto.DataType(kind =
      proto.DataType.Kind.Udt(proto.DataType.UDT(`type` = "udt", jvmClass = Some(jvmClass)))
    )

  private def literal(t: proto.Expression.Literal.LiteralType): proto.Expression.Literal =
    proto.Expression.Literal(literalType = t)

  private def byteLit(v: Int) = literal(proto.Expression.Literal.LiteralType.Byte(v))

  private def intLit(v: Int) = literal(proto.Expression.Literal.LiteralType.Integer(v))

  private def boolLit(v: Boolean) = literal(proto.Expression.Literal.LiteralType.Boolean(v))

  private def nullLit: proto.Expression.Literal =
    literal(
      proto.Expression.Literal.LiteralType
        .Null(proto.DataType(kind = proto.DataType.Kind.Null(proto.DataType.NULL())))
    )

  private def struct(
      jvmClass: String,
      elements: Seq[proto.Expression.Literal]
  ): proto.Expression.Literal =
    literal(
      proto.Expression.Literal.LiteralType.Struct(
        proto.Expression.Literal.Struct(structType = Some(udt(jvmClass)), elements = elements)
      )
    )

  private def ints(values: Array[Int]): proto.Expression.Literal =
    literal(
      proto.Expression.Literal.LiteralType.SpecializedArray(
        proto.Expression.Literal.SpecializedArray(valueType =
          proto.Expression.Literal.SpecializedArray.ValueTypeOneof.Ints(proto.Ints(values.toSeq))
        )
      )
    )

  private def doubles(values: Array[Double]): proto.Expression.Literal =
    literal(
      proto.Expression.Literal.LiteralType.SpecializedArray(
        proto.Expression.Literal.SpecializedArray(valueType =
          proto.Expression.Literal.SpecializedArray.ValueTypeOneof
            .Doubles(proto.Doubles(values.toSeq))
        )
      )
    )

  /** Encodes a parameter value (or method argument) as a literal. */
  def encodeValue(value: Any): proto.Expression.Literal = value match {
    case null => nullLit
    case v: DenseVector =>
      struct(VectorUDTClass, Seq(byteLit(1), nullLit, nullLit, doubles(v.values)))
    case v: SparseVector =>
      struct(VectorUDTClass, Seq(byteLit(0), intLit(v.size), ints(v.indices), doubles(v.values)))
    case v: Vector => encodeValue(v.toDense: DenseVector)
    case m: DenseMatrix =>
      struct(
        MatrixUDTClass,
        Seq(
          byteLit(1),
          intLit(m.numRows),
          intLit(m.numCols),
          nullLit,
          nullLit,
          doubles(m.values),
          boolLit(m.isTransposed)
        )
      )
    case m: SparseMatrix =>
      struct(
        MatrixUDTClass,
        Seq(
          byteLit(0),
          intLit(m.numRows),
          intLit(m.numCols),
          ints(m.colPtrs),
          ints(m.rowIndices),
          doubles(m.values),
          boolLit(m.isTransposed)
        )
      )
    case m: Matrix => encodeValue(m.toDense)
    case v: Int => intLit(v)
    case v: Long => literal(proto.Expression.Literal.LiteralType.Long(v))
    case v: Double => literal(proto.Expression.Literal.LiteralType.Double(v))
    case v: Float => literal(proto.Expression.Literal.LiteralType.Float(v))
    case v: Boolean => boolLit(v)
    case v: String => literal(proto.Expression.Literal.LiteralType.String(v))
    // Top-level arrays use ordinary array literals, as PySpark sends them. The server rejects
    // specialized arrays outside vector and matrix structs.
    case _: Array[Int] | _: Array[Double] | _: Array[String] => encodeArgument(value)
    case other =>
      // Fall back to the generic literal converter for remaining scalar types.
      throw new IllegalArgumentException(
        s"Unsupported ML parameter value of type ${other.getClass.getName}: $other"
      )
  }

  /**
   * Encodes a method argument. Arrays are sent as ordinary (non-specialized) array literals with an
   * explicit element type, since the server only decodes those for method arguments.
   */
  def encodeArgument(value: Any): proto.Expression.Literal = {
    def array(elementType: proto.DataType, elements: Seq[proto.Expression.Literal]) =
      literal(
        proto.Expression.Literal.LiteralType.Array(
          proto.Expression.Literal.Array(elementType = Some(elementType), elements = elements)
        )
      )
    value match {
      case a: Array[Double] =>
        array(
          proto.DataType(kind = proto.DataType.Kind.Double(proto.DataType.Double())),
          a.toSeq.map(v => literal(proto.Expression.Literal.LiteralType.Double(v)))
        )
      case a: Array[Int] =>
        array(
          proto.DataType(kind = proto.DataType.Kind.Integer(proto.DataType.Integer())),
          a.toSeq.map(v => intLit(v))
        )
      case a: Array[String] =>
        array(
          proto.DataType(kind = proto.DataType.Kind.String(proto.DataType.String())),
          a.toSeq.map(v => literal(proto.Expression.Literal.LiteralType.String(v)))
        )
      case other => encodeValue(other)
    }
  }

  /** Decodes a literal into a Scala value: vectors, matrices, or the generic literal value. */
  def decodeValue(literal: proto.Expression.Literal): Any =
    if (literal.literalType.isStruct) {
      val s = literal.getStruct
      s.getStructType.getUdt.getJvmClass match {
        case VectorUDTClass => decodeVector(s)
        case MatrixUDTClass => decodeMatrix(s)
        case other => throw new IllegalArgumentException(s"Unsupported struct type $other")
      }
    } else if (literal.literalType.isSpecializedArray) {
      import proto.Expression.Literal.SpecializedArray.ValueTypeOneof
      literal.getSpecializedArray.valueType match {
        case ValueTypeOneof.Doubles(d) => d.values.toArray
        case ValueTypeOneof.Ints(i) => i.values.toArray
        case ValueTypeOneof.Longs(l) => l.values.toArray
        case ValueTypeOneof.Floats(f) => f.values.map(_.toDouble).toArray
        case ValueTypeOneof.Strings(v) => v.values.toArray
        case ValueTypeOneof.Bools(b) => b.values.toArray
        case _ => Array.empty[Double]
      }
    } else if (literal.literalType.isArray) {
      // Element values keep their literal types, so the array type follows the first element.
      val elements = literal.getArray.elements.map(decodeValue)
      elements.headOption match {
        case None => Array.empty[Double]
        case Some(_: Long) => elements.map(_.asInstanceOf[Long]).toArray
        case Some(_: Int) => elements.map(_.asInstanceOf[Int]).toArray
        case Some(_: Double) => elements.map(_.asInstanceOf[Double]).toArray
        case Some(_: Float) => elements.map(_.asInstanceOf[Float].toDouble).toArray
        case Some(_: String) => elements.map(_.asInstanceOf[String]).toArray
        case Some(_: Boolean) => elements.map(_.asInstanceOf[Boolean]).toArray
        case Some(_) => elements.toArray
      }
    } else {
      LiteralValueProtoConverter.toScalaValue(literal)
    }

  private def numbers(literal: proto.Expression.Literal): Array[Double] =
    LiteralValueProtoConverter
      .toScalaValue(literal)
      .asInstanceOf[Seq[Any]]
      .map(_.asInstanceOf[Number].doubleValue())
      .toArray

  private def intValues(literal: proto.Expression.Literal): Array[Int] =
    LiteralValueProtoConverter
      .toScalaValue(literal)
      .asInstanceOf[Seq[Any]]
      .map(_.asInstanceOf[Number].intValue())
      .toArray

  /** Decodes a `VectorUDT` struct. */
  def decodeVector(s: proto.Expression.Literal.Struct): Vector =
    s.elements(0).getByte match {
      case 0 =>
        val size = s.elements(1).getInteger
        new SparseVector(size, intValues(s.elements(2)), numbers(s.elements(3)))
      case 1 => new DenseVector(numbers(s.elements(3)))
      case other => throw new IllegalArgumentException(s"Unknown Vector type $other")
    }

  /** Decodes a `MatrixUDT` struct. */
  def decodeMatrix(s: proto.Expression.Literal.Struct): Matrix =
    s.elements(0).getByte match {
      case 0 =>
        new SparseMatrix(
          s.elements(1).getInteger,
          s.elements(2).getInteger,
          intValues(s.elements(3)),
          intValues(s.elements(4)),
          numbers(s.elements(5)),
          s.elements(6).getBoolean
        )
      case 1 =>
        new DenseMatrix(
          s.elements(1).getInteger,
          s.elements(2).getInteger,
          numbers(s.elements(5)),
          s.elements(6).getBoolean
        )
      case other => throw new IllegalArgumentException(s"Unknown Matrix type $other")
    }

  /** The `MlParams` for the explicitly set parameters of `instance`. */
  def encodeParams(instance: Params): proto.MlParams =
    encodeParamMap(instance.paramMap)

  /** The `MlParams` for the entries of `map`. */
  def encodeParamMap(map: ParamMap): proto.MlParams =
    proto.MlParams(params = map.toSeq.map { pair =>
      pair.param.name -> encodeValue(pair.value)
    }.toMap)

  /** Decodes an `MlParams` into `(name, value)` pairs. */
  def decodeParams(params: proto.MlParams): Seq[(String, Any)] =
    params.params.toSeq.map { case (name, lit) => name -> decodeValue(lit) }

  /**
   * Restores the explicitly set parameters of `instance` from a server response, without
   * validation, since the server has already validated them.
   */
  def applyParams(instance: Params, params: proto.MlParams): Unit =
    decodeParams(params).foreach { case (name, value) =>
      if (instance.hasParam(name)) {
        instance.setInternalParam(name, value)
      }
    }
}
