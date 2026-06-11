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

import org.apache.spark.connect.proto.MergeAction.ActionType
import org.apache.spark.sql.functions.*

/**
 * Verifies the `MERGE INTO` protobuf command that [[MergeIntoWriter]] builds. These assertions are
 * pure plan construction (no server contact); the lazily-built session never issues an RPC.
 */
class MergeIntoSuite extends munit.FunSuite:

  // No `.config(...)` => `create()` performs no RPC; the channel is built lazily.
  private lazy val spark: SparkSession =
    SparkSession.builder.remote("sc://localhost:15099").create()

  private def source: Dataset[?] = spark.range(0)

  test("mergeInto builds a MergeIntoTableCommand covering every clause type") {
    val cmd = source
      .mergeInto("target", col("source.id") === col("target.id"))
      .whenMatched()
      .updateAll()
      .whenMatched(col("source.deleted"))
      .delete()
      .whenNotMatched(col("source.x") > 0)
      .insert(Map("id" -> col("source.id")))
      .whenNotMatched()
      .insertAll()
      .whenNotMatchedBySource()
      .delete()
      .withSchemaEvolution()
      .buildCommand()

    assertEquals(cmd.targetTableName, "target")
    assert(cmd.sourceTablePlan.isDefined)
    assert(cmd.mergeCondition.isDefined)
    assert(cmd.withSchemaEvolution)

    assertEquals(cmd.matchActions.size, 2)
    assertEquals(
      cmd.matchActions.head.getMergeAction.actionType,
      ActionType.ACTION_TYPE_UPDATE_STAR
    )
    assertEquals(cmd.matchActions(1).getMergeAction.actionType, ActionType.ACTION_TYPE_DELETE)
    assert(cmd.matchActions(1).getMergeAction.condition.isDefined)

    assertEquals(cmd.notMatchedActions.size, 2)
    val condInsert = cmd.notMatchedActions.head.getMergeAction
    assertEquals(condInsert.actionType, ActionType.ACTION_TYPE_INSERT)
    assert(condInsert.condition.isDefined)
    assertEquals(condInsert.assignments.size, 1)
    assertEquals(
      cmd.notMatchedActions(1).getMergeAction.actionType,
      ActionType.ACTION_TYPE_INSERT_STAR
    )

    assertEquals(cmd.notMatchedBySourceActions.size, 1)
    assertEquals(
      cmd.notMatchedBySourceActions.head.getMergeAction.actionType,
      ActionType.ACTION_TYPE_DELETE
    )
  }

  test("merge with no WHEN clause is rejected") {
    intercept[IllegalStateException] {
      source.mergeInto("t", col("a") === col("b")).buildCommand()
    }
  }
