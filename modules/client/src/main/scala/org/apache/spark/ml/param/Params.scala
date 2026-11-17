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

import java.lang.reflect.Method

import org.apache.spark.ml.util.Identifiable

/**
 * A component with named, typed parameters, mirroring `org.apache.spark.ml.param.Params`.
 *
 * Only parameters that are explicitly set are sent to the server. Defaults are kept client side and
 * are not sent, so the server applies its own defaults for anything left unset.
 */
trait Params extends Identifiable with Serializable {

  private[ml] val defaultParamMap: ParamMap = new ParamMap()

  private[ml] val paramMap: ParamMap = new ParamMap()

  /**
   * All parameters of this instance, found by reflection on its `Param`-typed members. Not cached:
   * traits call `setDefault` while their later siblings are still uninitialized, so a cache taken
   * early could miss parameters.
   */
  def params: Array[Param[_]] =
    this.getClass.getMethods
      .filter((m: Method) =>
        classOf[Param[_]].isAssignableFrom(m.getReturnType) && m.getParameterCount == 0
      )
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
      .filter(_ != null)
      .sortBy(_.name)

  /** Whether this instance has a parameter called `paramName`. */
  def hasParam(paramName: String): Boolean = params.exists(_.name == paramName)

  /** The parameter called `paramName`. */
  def getParam(paramName: String): Param[Any] =
    params
      .find(_.name == paramName)
      .map(_.asInstanceOf[Param[Any]])
      .getOrElse(throw new NoSuchElementException(s"Param $paramName does not exist."))

  /** Requires that `param` belongs to this instance. */
  protected final def shouldOwn(param: Param[_]): Unit =
    require(param.parent == uid, s"Param $param does not belong to $uid.")

  /** Whether `param` has been explicitly set. */
  final def isSet(param: Param[_]): Boolean = {
    shouldOwn(param)
    paramMap.contains(param)
  }

  /** Whether `param` has a default value. */
  final def hasDefault(param: Param[_]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param)
  }

  /** Whether `param` is either set or has a default. */
  final def isDefined(param: Param[_]): Boolean = hasDefault(param) || isSet(param)

  /** Sets `param` to `value`. */
  final def set[T](param: Param[T], value: T): this.type = {
    shouldOwn(param)
    param.validate(value)
    paramMap.put(param, value)
    this
  }

  /** Sets a parameter by name. */
  final def set(paramName: String, value: Any): this.type = {
    val p = getParam(paramName)
    set(p, value)
  }

  /** Sets the parameter of a [[ParamPair]]. */
  final def set(pair: ParamPair[_]): this.type =
    set(pair.param.asInstanceOf[Param[Any]], pair.value)

  /** Sets `param` to `value` as its default. */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    shouldOwn(param)
    param.validate(value)
    defaultParamMap.put(param, value)
    this
  }

  /** Sets default values from the given pairs. */
  protected final def setDefault(pairs: ParamPair[_]*): this.type = {
    pairs.foreach { p =>
      setDefault(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /** The explicitly set value of `param`, if any. */
  final def get[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    paramMap.get(param)
  }

  /** The default value of `param`, if any. */
  final def getDefault[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    defaultParamMap.get(param)
  }

  /** The set value of `param`, falling back to its default. Throws if neither exists. */
  final def getOrDefault[T](param: Param[T]): T = {
    shouldOwn(param)
    get(param).orElse(getDefault(param)).getOrElse {
      throw new NoSuchElementException(
        s"Failed to find a default value for ${param.name}. Set it first or call setDefault."
      )
    }
  }

  /** Shorthand for [[getOrDefault]]. */
  final def $[T](param: Param[T]): T = getOrDefault(param)

  /** Removes the explicitly set value of `param`, falling back to its default. */
  final def clear(param: Param[_]): this.type = {
    shouldOwn(param)
    paramMap.remove(param.asInstanceOf[Param[Any]])
    this
  }

  /** A one-line explanation of `param`, its default and its current value. */
  def explainParam(param: Param[_]): String = {
    shouldOwn(param)
    val valueStr = if (isDefined(param)) {
      val defaultValueStr = getDefault(param).map(v => s"default: $v")
      val currentValueStr = get(param).map(v => s"current: $v")
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }
    s"${param.name}: ${param.doc} $valueStr"
  }

  /** Explanations of every parameter, one per line. */
  def explainParams(): String = params.map(p => explainParam(p)).mkString("\n")

  /** The full parameter map: defaults, overridden by set values, overridden by `extra`. */
  final def extractParamMap(extra: ParamMap): ParamMap =
    defaultParamMap ++ paramMap ++ extra

  /** The full parameter map of this instance. */
  final def extractParamMap(): ParamMap = extractParamMap(ParamMap.empty)

  /**
   * Copies the values of this instance to `to`, with `extra` applied on top. Explicitly set values
   * stay set and defaults stay defaults, for every parameter `to` also has. Used by `copy`
   * implementations and by fitting, so a model keeps its estimator's settings.
   */
  private[ml] final def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = paramMap ++ extra
    params.foreach { param =>
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(to.getParam(param.name), map(param.asInstanceOf[Param[Any]]))
      }
      if (defaultParamMap.contains(param) && to.hasParam(param.name)) {
        to.setDefault(to.getParam(param.name), defaultParamMap(param.asInstanceOf[Param[Any]]))
      }
    }
    to
  }

  /** Sets default values in a way that makes the target usable by [[copyValues]]. */
  private[ml] def setDefaultInternal[T](param: Param[T], value: T): Unit =
    defaultParamMap.put(param, value)

  /** Records a set value without validation. Used when restoring state from the server. */
  private[ml] def setInternal(param: Param[Any], value: Any): Unit =
    paramMap.put(param, value)

  /** Records a set value by parameter name without validation. */
  private[ml] def setInternalParam(paramName: String, value: Any): Unit =
    setInternal(getParam(paramName), value)
}
