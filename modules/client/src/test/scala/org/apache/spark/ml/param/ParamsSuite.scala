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

import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Tokenizer, VectorSizeHint}

class ParamsSuite extends munit.FunSuite {

  test("defaults match Spark, and set values override them") {
    val lr = new LogisticRegression()
    assertEquals(lr.getMaxIter, 100)
    assertEquals(lr.getRegParam, 0.0)
    assertEquals(lr.getThreshold, 0.5)
    assertEquals(lr.getFamily, "auto")
    assertEquals(lr.getFeaturesCol, "features")
    assert(lr.hasDefault(lr.maxIter))
    assert(!lr.isSet(lr.maxIter))
    lr.setMaxIter(3)
    assert(lr.isSet(lr.maxIter))
    assertEquals(lr.getOrDefault(lr.maxIter), 3)
    lr.clear(lr.maxIter)
    assertEquals(lr.getMaxIter, 100)
  }

  test("set validates values") {
    val lr = new LogisticRegression()
    intercept[IllegalArgumentException](lr.setRegParam(-1.0))
    intercept[IllegalArgumentException](lr.setElasticNetParam(2.0))
    intercept[IllegalArgumentException](lr.setFamily("poisson"))
    intercept[IllegalArgumentException](new VectorSizeHint().setSize(-1))
  }

  test("params are discovered by reflection and explained") {
    val lr = new LogisticRegression().setRegParam(0.3)
    assert(lr.hasParam("regParam"))
    assert(!lr.hasParam("noSuchParam"))
    val names = lr.params.map(_.name).toSet
    assert(names.contains("maxIter") && names.contains("family") && names.contains("featuresCol"))
    val explained = lr.explainParam(lr.regParam)
    assert(explained.startsWith("regParam:"))
    assert(explained.contains("current: 0.3"))
    assert(lr.explainParams().contains("maxIter"))
  }

  test("extractParamMap merges defaults, set values and extras") {
    val lr = new LogisticRegression().setMaxIter(7)
    val map = lr.extractParamMap(ParamMap(lr.regParam -> 0.9))
    assertEquals(map(lr.maxIter), 7)
    assertEquals(map(lr.regParam), 0.9)
    assertEquals(map(lr.fitIntercept), true)
    assert(!lr.isSet(lr.regParam))
  }

  test("copy keeps uid and values, and applies extra params") {
    val lr = new LogisticRegression().setMaxIter(7)
    val copied = lr.copy(ParamMap(lr.maxIter -> 11))
    assertEquals(copied.uid, lr.uid)
    assertEquals(copied.getMaxIter, 11)
    assertEquals(lr.getMaxIter, 7)
    val plain = lr.copy(ParamMap.empty)
    assertEquals(plain.getMaxIter, 7)
    assert(plain.isSet(plain.maxIter))
  }

  test("a set value copies as set, and a default copies as default") {
    val lr = new LogisticRegression().setRegParam(0.2)
    val copied = lr.copy(ParamMap.empty)
    assert(copied.isSet(copied.regParam))
    assert(!copied.isSet(copied.maxIter))
    assert(copied.hasDefault(copied.maxIter))
  }

  test("ParamMap supports put, get, filter, remove and merge") {
    val lr = new LogisticRegression()
    val map = ParamMap(lr.maxIter -> 5, lr.regParam -> 0.1)
    assertEquals(map.size, 2)
    assertEquals(map.get(lr.maxIter), Some(5))
    assert(map.contains(lr.regParam))
    assertEquals(map.filter(lr).size, 2)
    val merged = map ++ ParamMap(lr.maxIter -> 9)
    assertEquals(merged(lr.maxIter), 9)
    assertEquals(map(lr.maxIter), 5)
    assertEquals(merged.remove(lr.regParam), Some(0.1))
    assertEquals(merged.getOrElse(lr.regParam, 1.0), 1.0)
  }

  test("ParamPair and Param -> value build pairs that validate") {
    val lr = new LogisticRegression()
    val pair = lr.regParam -> 0.5
    assertEquals(pair.value, 0.5)
    intercept[IllegalArgumentException](lr.maxIter -> -1)
  }

  test("ParamGridBuilder builds the cartesian product with the first grid varying fastest") {
    val lr = new LogisticRegression()
    val grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.0, 0.1))
      .addGrid(lr.maxIter, Array(5, 10))
      .build()
    assertEquals(grid.length, 4)
    assertEquals(
      grid.map(m => (m(lr.regParam), m(lr.maxIter))).toSeq,
      Seq((0.0, 5), (0.1, 5), (0.0, 10), (0.1, 10))
    )
  }

  test("ParamGridBuilder adds boolean grids and base values") {
    val lr = new LogisticRegression()
    val grid = new ParamGridBuilder().addGrid(lr.fitIntercept).baseOn(lr.maxIter -> 4).build()
    assertEquals(grid.length, 2)
    assertEquals(grid.map(_(lr.maxIter)).toSet, Set(4))
  }

  test("validators accept and reject as Spark does") {
    import ParamValidators._
    val nonNegative: Double => Boolean = gtEq(0.0)
    val positiveInt: Int => Boolean = gt(0)
    val unit: Double => Boolean = inRange(0.0, 1.0)
    val openUnit: Double => Boolean = inRange(0.0, 1.0, lowerInclusive = false)
    assert(nonNegative(0.0))
    assert(!positiveInt(0))
    assert(unit(0.5))
    assert(!openUnit(0.0))
    assert(inArray(Array("a", "b"))("a"))
    assert(!inArray(Array("a", "b"))("c"))
  }

  test("an unknown param name is rejected") {
    intercept[NoSuchElementException] {
      new LogisticRegression().getParam("bogus")
    }
  }

  test("shared traits carry their Spark defaults") {
    val tok = new Tokenizer()
    assertEquals(tok.getOutputCol, tok.uid + "__output")
    assertEquals(new LogisticRegression().getLabelCol, "label")
    assertEquals(new LogisticRegression().getPredictionCol, "prediction")
    assertEquals(new LogisticRegression().getProbabilityCol, "probability")
  }
}
