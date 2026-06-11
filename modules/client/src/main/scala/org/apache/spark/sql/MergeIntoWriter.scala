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

import scala.collection.mutable

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.MergeAction.ActionType

/**
 * Builder for a `MERGE INTO` statement, obtained from [[Dataset.mergeInto]]. Specify one or more
 * `whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` clauses, then call [[merge]] to
 * execute the statement against the target table.
 *
 * {{{
 *   spark.table("source")
 *     .mergeInto("target", col("source.id") === col("target.id"))
 *     .whenMatched().updateAll()
 *     .whenNotMatched().insertAll()
 *     .whenNotMatchedBySource().delete()
 *     .merge()
 * }}}
 */
class MergeIntoWriter[T] private[sql] (table: String, ds: Dataset[T], on: Column) {

  private var schemaEvolutionEnabled = false
  private val matchedActions = mutable.ArrayBuffer.empty[proto.Expression]
  private val notMatchedActions = mutable.ArrayBuffer.empty[proto.Expression]
  private val notMatchedBySourceActions = mutable.ArrayBuffer.empty[proto.Expression]

  /** Initiates a `WHEN MATCHED` clause without an extra condition. */
  def whenMatched(): WhenMatched[T] = new WhenMatched[T](this, None)

  /** Initiates a `WHEN MATCHED AND <condition>` clause. */
  def whenMatched(condition: Column): WhenMatched[T] = new WhenMatched[T](this, Some(condition))

  /** Initiates a `WHEN NOT MATCHED` clause without an extra condition. */
  def whenNotMatched(): WhenNotMatched[T] = new WhenNotMatched[T](this, None)

  /** Initiates a `WHEN NOT MATCHED AND <condition>` clause. */
  def whenNotMatched(condition: Column): WhenNotMatched[T] =
    new WhenNotMatched[T](this, Some(condition))

  /** Initiates a `WHEN NOT MATCHED BY SOURCE` clause without an extra condition. */
  def whenNotMatchedBySource(): WhenNotMatchedBySource[T] =
    new WhenNotMatchedBySource[T](this, None)

  /** Initiates a `WHEN NOT MATCHED BY SOURCE AND <condition>` clause. */
  def whenNotMatchedBySource(condition: Column): WhenNotMatchedBySource[T] =
    new WhenNotMatchedBySource[T](this, Some(condition))

  /** Enables automatic schema evolution for this merge. */
  def withSchemaEvolution(): MergeIntoWriter[T] = {
    schemaEvolutionEnabled = true
    this
  }

  /** Executes the merge against the target table. */
  def merge(): Unit =
    ds.sparkSession.executeCommand(
      proto.Command(commandType = proto.Command.CommandType.MergeIntoTableCommand(buildCommand()))
    )

  private[sql] def buildCommand(): proto.MergeIntoTableCommand = {
    if (matchedActions.isEmpty && notMatchedActions.isEmpty && notMatchedBySourceActions.isEmpty) {
      throw new IllegalStateException(
        "At least one WHEN clause is required for a MERGE INTO statement."
      )
    }
    proto.MergeIntoTableCommand(
      targetTableName = table,
      sourceTablePlan = Some(ds.relation),
      mergeCondition = Some(on.expr),
      matchActions = matchedActions.toSeq,
      notMatchedActions = notMatchedActions.toSeq,
      notMatchedBySourceActions = notMatchedBySourceActions.toSeq,
      withSchemaEvolution = schemaEvolutionEnabled
    )
  }

  private[sql] def appendMatched(action: proto.Expression): MergeIntoWriter[T] = {
    matchedActions += action
    this
  }

  private[sql] def appendNotMatched(action: proto.Expression): MergeIntoWriter[T] = {
    notMatchedActions += action
    this
  }

  private[sql] def appendNotMatchedBySource(action: proto.Expression): MergeIntoWriter[T] = {
    notMatchedBySourceActions += action
    this
  }
}

private[sql] object MergeIntoWriter {

  /** Builds a [[proto.MergeAction]] wrapped in an [[proto.Expression]]. */
  def action(
      actionType: ActionType,
      condition: Option[Column],
      assignments: Map[String, Column]
  ): proto.Expression = {
    val protoAssignments = assignments.toSeq.map { case (key, value) =>
      proto.MergeAction
        .Assignment(key = Some(Column.fromName(key).expr), value = Some(value.expr))
    }
    proto.Expression(exprType =
      proto.Expression.ExprType.MergeAction(
        proto.MergeAction(
          actionType = actionType,
          condition = condition.map(_.expr),
          assignments = protoAssignments
        )
      )
    )
  }
}

/** A `WHEN MATCHED [AND condition]` clause of a [[MergeIntoWriter]]. */
class WhenMatched[T] private[sql] (writer: MergeIntoWriter[T], condition: Option[Column]) {

  /** Updates all target columns from the matching source row. */
  def updateAll(): MergeIntoWriter[T] =
    writer.appendMatched(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_UPDATE_STAR, condition, Map.empty)
    )

  /** Updates the given target columns with the given expressions. */
  def update(set: Map[String, Column]): MergeIntoWriter[T] =
    writer.appendMatched(MergeIntoWriter.action(ActionType.ACTION_TYPE_UPDATE, condition, set))

  /** Deletes the matching target row. */
  def delete(): MergeIntoWriter[T] =
    writer.appendMatched(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_DELETE, condition, Map.empty)
    )
}

/** A `WHEN NOT MATCHED [AND condition]` clause of a [[MergeIntoWriter]]. */
class WhenNotMatched[T] private[sql] (writer: MergeIntoWriter[T], condition: Option[Column]) {

  /** Inserts a new target row with all columns from the source row. */
  def insertAll(): MergeIntoWriter[T] =
    writer.appendNotMatched(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_INSERT_STAR, condition, Map.empty)
    )

  /** Inserts a new target row, setting the given columns to the given expressions. */
  def insert(set: Map[String, Column]): MergeIntoWriter[T] =
    writer.appendNotMatched(MergeIntoWriter.action(ActionType.ACTION_TYPE_INSERT, condition, set))
}

/** A `WHEN NOT MATCHED BY SOURCE [AND condition]` clause of a [[MergeIntoWriter]]. */
class WhenNotMatchedBySource[T] private[sql] (
    writer: MergeIntoWriter[T],
    condition: Option[Column]
) {

  /** Updates all target columns (using only target-side expressions). */
  def updateAll(): MergeIntoWriter[T] =
    writer.appendNotMatchedBySource(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_UPDATE_STAR, condition, Map.empty)
    )

  /** Updates the given target columns with the given expressions. */
  def update(set: Map[String, Column]): MergeIntoWriter[T] =
    writer.appendNotMatchedBySource(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_UPDATE, condition, set)
    )

  /** Deletes the target row that has no match in the source. */
  def delete(): MergeIntoWriter[T] =
    writer.appendNotMatchedBySource(
      MergeIntoWriter.action(ActionType.ACTION_TYPE_DELETE, condition, Map.empty)
    )
}
