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

import org.apache.spark.connect.proto
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Hermetic coverage for [[Window]] / [[org.apache.spark.sql.expressions.WindowSpec]] plan building:
 * partition/order specs and, importantly, the frame-boundary sentinel mapping (unbounded /
 * current-row / explicit value). No server.
 */
class WindowSpecSuite extends munit.FunSuite {

  test("partitionBy and orderBy populate the spec") {
    val w = Window.partitionBy("a", "b").orderBy(col("c").desc)
    assertEquals(w.partitionSpec.size, 2)
    assertEquals(w.orderSpec.size, 1)
    // a plain column order defaults to ascending / nulls-first
    val asc = Window.orderBy(col("c"))
    assertEquals(
      asc.orderSpec.head.direction,
      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
    )
  }

  test("rowsBetween maps unbounded-preceding and current-row to the right sentinels") {
    val w = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val f = w.frame.get
    assertEquals(f.frameType, proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW)
    assert(f.lower.get.boundary.isUnbounded)
    assert(f.upper.get.boundary.isCurrentRow)
  }

  test("rangeBetween maps current-row and unbounded-following to the right sentinels") {
    val w = Window.rangeBetween(Window.currentRow, Window.unboundedFollowing)
    val f = w.frame.get
    assertEquals(f.frameType, proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE)
    assert(f.lower.get.boundary.isCurrentRow)
    assert(f.upper.get.boundary.isUnbounded)
  }

  test("explicit offsets become value boundaries") {
    val w = Window.rowsBetween(-1, 1)
    val f = w.frame.get
    assert(f.lower.get.boundary.isValue)
    assert(f.upper.get.boundary.isValue)
  }

  test("the unbounded sentinels are the Long extremes and current-row is zero") {
    assertEquals(Window.unboundedPreceding, Long.MinValue)
    assertEquals(Window.unboundedFollowing, Long.MaxValue)
    assertEquals(Window.currentRow, 0L)
  }

  test("a function over a window builds a Window expression") {
    val w = Window.partitionBy("a").orderBy("b")
    val c = sum(col("x")).over(w)
    assert(c.expr.exprType.isWindow)
  }
}
