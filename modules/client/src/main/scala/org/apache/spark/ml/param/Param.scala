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

package org.apache.spark.ml.param

import org.apache.spark.ml.util.Identifiable

/**
 * A named, documented parameter of an ML operator, mirroring `org.apache.spark.ml.param.Param`.
 *
 * @param parent
 *   the uid of the owning operator
 * @param name
 *   the parameter name, as the server knows it
 * @param doc
 *   a human-readable description
 * @param isValid
 *   the validator applied by [[Params.set]]
 */
class Param[T](val parent: String, val name: String, val doc: String, val isValid: T => Boolean)
    extends Serializable {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[T])

  /** Throws `IllegalArgumentException` when `value` fails the validator. */
  def validate(value: T): Unit =
    if (!isValid(value)) {
      throw new IllegalArgumentException(s"$this given invalid value $value, ${doc} is violated.")
    }

  /** A `(param, value)` pair, for building a [[ParamMap]]. */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override def toString: String = s"${parent}__$name"

  override def hashCode: Int = toString.##

  override def equals(obj: Any): Boolean = obj match {
    case p: Param[_] => parent == p.parent && name == p.name
    case _ => false
  }
}

/** A parameter holding an `Int`. */
class IntParam(parent: String, name: String, doc: String, isValid: Int => Boolean)
    extends Param[Int](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Int => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Int])
}

/** A parameter holding a `Long`. */
class LongParam(parent: String, name: String, doc: String, isValid: Long => Boolean)
    extends Param[Long](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Long => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Long])
}

/** A parameter holding a `Float`. */
class FloatParam(parent: String, name: String, doc: String, isValid: Float => Boolean)
    extends Param[Float](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Float => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Float])
}

/** A parameter holding a `Double`. */
class DoubleParam(parent: String, name: String, doc: String, isValid: Double => Boolean)
    extends Param[Double](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Double => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Double])
}

/** A parameter holding a `Boolean`. */
class BooleanParam(parent: String, name: String, doc: String, isValid: Boolean => Boolean)
    extends Param[Boolean](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Boolean => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Boolean])
}

/** A parameter holding a `String`. */
class StringParam(parent: String, name: String, doc: String, isValid: String => Boolean)
    extends Param[String](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: String => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[String])
}

/** A parameter holding an `Array[Int]`. */
class IntArrayParam(parent: String, name: String, doc: String, isValid: Array[Int] => Boolean)
    extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Array[Int] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Array[Int]])
}

/** A parameter holding an `Array[Double]`. */
class DoubleArrayParam(parent: String, name: String, doc: String, isValid: Array[Double] => Boolean)
    extends Param[Array[Double]](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Array[Double] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Array[Double]])
}

/** A parameter holding an `Array[String]`. */
class StringArrayParam(parent: String, name: String, doc: String, isValid: Array[String] => Boolean)
    extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Identifiable, name: String, doc: String, isValid: Array[String] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc, ParamValidators.alwaysTrue[Array[String]])
}

/**
 * A `(param, value)` pair. The value is validated when the pair is built.
 */
final case class ParamPair[T](param: Param[T], value: T) {
  param.validate(value)
}

/** Validators for [[Param]] values, mirroring `org.apache.spark.ml.param.ParamValidators`. */
object ParamValidators {

  /** Accepts every value. */
  def alwaysTrue[T]: T => Boolean = (_: T) => true

  /** Accepts values greater than `lowerBound`. */
  def gt[T](lowerBound: T)(implicit ev: Numeric[T]): T => Boolean = (x: T) => ev.gt(x, lowerBound)

  /** Accepts values greater than or equal to `lowerBound`. */
  def gtEq[T](lowerBound: T)(implicit ev: Numeric[T]): T => Boolean =
    (x: T) => ev.gteq(x, lowerBound)

  /** Accepts values less than `upperBound`. */
  def lt[T](upperBound: T)(implicit ev: Numeric[T]): T => Boolean = (x: T) => ev.lt(x, upperBound)

  /** Accepts values less than or equal to `upperBound`. */
  def ltEq[T](upperBound: T)(implicit ev: Numeric[T]): T => Boolean =
    (x: T) => ev.lteq(x, upperBound)

  /** Accepts values in `[lower, upper]` (or `(lower, upper)` when `lowerInclusive` is false). */
  def inRange[T](
      lowerBound: T,
      upperBound: T,
      lowerInclusive: Boolean = true,
      upperInclusive: Boolean = true
  )(implicit ev: Numeric[T]): T => Boolean = { (x: T) =>
    val lowerOk = if (lowerInclusive) ev.gteq(x, lowerBound) else ev.gt(x, lowerBound)
    val upperOk = if (upperInclusive) ev.lteq(x, upperBound) else ev.lt(x, upperBound)
    lowerOk && upperOk
  }

  /** Accepts the values in `allowed`. */
  def inArray[T](allowed: Array[T]): T => Boolean = (x: T) => allowed.contains(x)

  /** Accepts the values in `allowed`. */
  def inArray[T](allowed: java.util.List[T]): T => Boolean = (x: T) => allowed.contains(x)

  /** Accepts arrays longer than `lowerBound`. */
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean =
    (x: Array[T]) => x.length > lowerBound
}
