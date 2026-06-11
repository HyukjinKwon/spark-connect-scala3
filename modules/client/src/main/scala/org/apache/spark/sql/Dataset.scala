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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Relation.{RelType}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.StructType

/**
 * A distributed collection of rows. A [[Dataset]] is lazy: transformations build up a protobuf
 * logical plan, and nothing executes until an action (e.g. [[collect]], [[show]], [[count]]) is
 * called.
 *
 * Mirrors the public surface of `org.apache.spark.sql.Dataset` over the Spark Connect protocol.
 */
class Dataset private[sql] (
    val sparkSession: SparkSession,
    private[sql] val relation: proto.Relation) {

  private[sql] def plan: proto.Plan = proto.Plan(proto.Plan.OpType.Root(relation))

  /** Builds a new DataFrame whose relation has `this` as input. */
  private def withInput(relType: RelType): DataFrame = sparkSession.newDataFrame(relType)

  // -- Schema ----------------------------------------------------------------

  /** The schema of this Dataset. */
  def schema: StructType =
    DataTypeProtoConverter.toCatalystType(sparkSession.client.analyzeSchema(plan)) match {
      case s: StructType => s
      case other => StructType(Array(org.apache.spark.sql.types.StructField("value", other)))
    }

  def columns: Array[String] = schema.fieldNames
  def dtypes: Array[(String, String)] = schema.fields.map(f => (f.name, f.dataType.simpleString))
  def printSchema(): Unit = print(schema.treeString)
  def printSchema(level: Int): Unit = print(sparkSession.client.treeString(plan, Some(level)))
  def isLocal: Boolean = sparkSession.client.isLocal(plan)
  def isStreaming: Boolean = sparkSession.client.isStreaming(plan)
  def inputFiles: Array[String] = sparkSession.client.inputFiles(plan).toArray

  // -- Column references -----------------------------------------------------

  /**
   * Selects a column by name, qualified by this Dataset's plan id so that it resolves
   * unambiguously even in self-joins.
   */
  def col(colName: String): Column = {
    val planId = relation.common.flatMap(_.planId)
    if (colName == "*") {
      Column(
        proto.Expression(exprType = proto.Expression.ExprType.UnresolvedStar(
          proto.Expression.UnresolvedStar(planId = planId))))
    } else {
      Column(
        proto.Expression(exprType = proto.Expression.ExprType.UnresolvedAttribute(
          proto.Expression.UnresolvedAttribute(unparsedIdentifier = colName, planId = planId))))
    }
  }

  /** Selects a column by name. Alias for [[col]]. */
  def apply(colName: String): Column = col(colName)

  /** Selects columns based on a column name regular expression. */
  def colRegex(colName: String): Column = {
    val planId = relation.common.flatMap(_.planId)
    Column(
      proto.Expression(exprType = proto.Expression.ExprType.UnresolvedRegex(
        proto.Expression.UnresolvedRegex(colName = colName, planId = planId))))
  }

  // -- Projection / filtering ------------------------------------------------

  def select(cols: Column*): DataFrame =
    withInput(RelType.Project(proto.Project(input = Some(relation), expressions = cols.map(_.expr))))

  def select(col: String, cols: String*): DataFrame =
    select((col +: cols).map(Column.fromName)*)

  def selectExpr(exprs: String*): DataFrame =
    select(exprs.map(Dataset.exprColumn)*)

  def filter(condition: Column): DataFrame =
    withInput(
      RelType.Filter(proto.Filter(input = Some(relation), condition = Some(condition.expr))))

  def filter(conditionExpr: String): DataFrame = filter(Dataset.exprColumn(conditionExpr))
  def where(condition: Column): DataFrame = filter(condition)
  def where(conditionExpr: String): DataFrame = filter(conditionExpr)

  def withColumn(colName: String, col: Column): DataFrame =
    withInput(
      RelType.WithColumns(
        proto.WithColumns(
          input = Some(relation),
          aliases =
            Seq(proto.Expression.Alias(expr = Some(col.expr), name = Seq(colName))))))

  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    withInput(
      RelType.WithColumnsRenamed(
        proto.WithColumnsRenamed(
          input = Some(relation),
          renames = Seq(
            proto.WithColumnsRenamed.Rename(
              colName = existingName,
              newColName = newName)))))

  def withColumnsRenamed(renames: Map[String, String]): DataFrame =
    withInput(
      RelType.WithColumnsRenamed(
        proto.WithColumnsRenamed(
          input = Some(relation),
          renames = renames.toSeq.map { case (k, v) =>
            proto.WithColumnsRenamed.Rename(colName = k, newColName = v)
          })))

  def drop(colNames: String*): DataFrame =
    withInput(RelType.Drop(proto.Drop(input = Some(relation), columnNames = colNames)))

  def drop(col: Column): DataFrame =
    withInput(RelType.Drop(proto.Drop(input = Some(relation), columns = Seq(col.expr))))

  def toDF(): DataFrame = this
  def toDF(colNames: String*): DataFrame =
    withInput(RelType.ToDf(proto.ToDF(input = Some(relation), columnNames = colNames)))

  def as(alias: String): DataFrame =
    withInput(
      RelType.SubqueryAlias(proto.SubqueryAlias(input = Some(relation), alias = alias)))
  def alias(alias: String): DataFrame = as(alias)

  def hint(name: String, parameters: Any*): DataFrame =
    withInput(
      RelType.Hint(
        proto.Hint(
          input = Some(relation),
          name = name,
          parameters = parameters.map(p => Column.lit(p).expr))))

  // -- Ordering & limiting ---------------------------------------------------

  def limit(n: Int): DataFrame =
    withInput(RelType.Limit(proto.Limit(input = Some(relation), limit = n)))

  def offset(n: Int): DataFrame =
    withInput(RelType.Offset(proto.Offset(input = Some(relation), offset = n)))

  def sort(sortCol: String, sortCols: String*): DataFrame =
    sort((sortCol +: sortCols).map(Column.fromName)*)

  def sort(sortExprs: Column*): DataFrame = buildSort(sortExprs, global = true)
  def orderBy(sortCol: String, sortCols: String*): DataFrame = sort(sortCol, sortCols*)
  def orderBy(sortExprs: Column*): DataFrame = sort(sortExprs*)
  def sortWithinPartitions(sortExprs: Column*): DataFrame = buildSort(sortExprs, global = false)
  def sortWithinPartitions(sortCol: String, sortCols: String*): DataFrame =
    buildSort((sortCol +: sortCols).map(Column.fromName), global = false)

  private def buildSort(sortExprs: Seq[Column], global: Boolean): DataFrame =
    withInput(
      RelType.Sort(
        proto.Sort(
          input = Some(relation),
          order = sortExprs.map(Dataset.toSortOrder),
          isGlobal = Some(global))))

  // -- Distinct --------------------------------------------------------------

  def distinct(): DataFrame =
    withInput(
      RelType.Deduplicate(
        proto.Deduplicate(input = Some(relation), allColumnsAsKeys = Some(true))))

  def dropDuplicates(): DataFrame = distinct()
  def dropDuplicates(colNames: Seq[String]): DataFrame =
    withInput(
      RelType.Deduplicate(proto.Deduplicate(input = Some(relation), columnNames = colNames)))
  def dropDuplicates(col1: String, cols: String*): DataFrame = dropDuplicates(col1 +: cols)

  // -- Set operations --------------------------------------------------------

  def union(other: Dataset): DataFrame = setOp(other, union = true, isAll = true)
  def unionAll(other: Dataset): DataFrame = union(other)
  def unionByName(other: Dataset): DataFrame = unionByName(other, allowMissingColumns = false)
  def unionByName(other: Dataset, allowMissingColumns: Boolean): DataFrame =
    withInput(
      RelType.SetOp(
        proto.SetOperation(
          leftInput = Some(relation),
          rightInput = Some(other.relation),
          setOpType = proto.SetOperation.SetOpType.SET_OP_TYPE_UNION,
          isAll = Some(true),
          byName = Some(true),
          allowMissingColumns = Some(allowMissingColumns))))

  def intersect(other: Dataset): DataFrame =
    setOpTyped(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = false)
  def intersectAll(other: Dataset): DataFrame =
    setOpTyped(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = true)
  def except(other: Dataset): DataFrame =
    setOpTyped(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = false)
  def exceptAll(other: Dataset): DataFrame =
    setOpTyped(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = true)

  private def setOp(other: Dataset, union: Boolean, isAll: Boolean): DataFrame =
    setOpTyped(other, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION, isAll)

  private def setOpTyped(
      other: Dataset,
      opType: proto.SetOperation.SetOpType,
      isAll: Boolean): DataFrame =
    withInput(
      RelType.SetOp(
        proto.SetOperation(
          leftInput = Some(relation),
          rightInput = Some(other.relation),
          setOpType = opType,
          isAll = Some(isAll))))

  // -- Joins -----------------------------------------------------------------

  def join(right: Dataset): DataFrame = buildJoin(right, None, Nil, "inner")
  def join(right: Dataset, usingColumn: String): DataFrame =
    buildJoin(right, None, Seq(usingColumn), "inner")
  def join(right: Dataset, usingColumns: Seq[String]): DataFrame =
    buildJoin(right, None, usingColumns, "inner")
  def join(right: Dataset, usingColumns: Seq[String], joinType: String): DataFrame =
    buildJoin(right, None, usingColumns, joinType)
  def join(right: Dataset, joinExprs: Column): DataFrame =
    buildJoin(right, Some(joinExprs), Nil, "inner")
  def join(right: Dataset, joinExprs: Column, joinType: String): DataFrame =
    buildJoin(right, Some(joinExprs), Nil, joinType)
  def crossJoin(right: Dataset): DataFrame = buildJoin(right, None, Nil, "cross")

  private def buildJoin(
      right: Dataset,
      joinExprs: Option[Column],
      usingColumns: Seq[String],
      joinType: String): DataFrame =
    withInput(
      RelType.Join(
        proto.Join(
          left = Some(relation),
          right = Some(right.relation),
          joinCondition = joinExprs.map(_.expr),
          joinType = Dataset.toJoinType(joinType),
          usingColumns = usingColumns)))

  // -- Repartitioning --------------------------------------------------------

  def repartition(numPartitions: Int): DataFrame =
    withInput(
      RelType.Repartition(
        proto.Repartition(input = Some(relation), numPartitions = numPartitions, shuffle = Some(true))))

  def repartition(numPartitions: Int, partitionExprs: Column*): DataFrame =
    withInput(
      RelType.RepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(_.expr),
          numPartitions = Some(numPartitions))))

  def repartition(partitionExprs: Column*): DataFrame =
    withInput(
      RelType.RepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(_.expr))))

  def coalesce(numPartitions: Int): DataFrame =
    withInput(
      RelType.Repartition(
        proto.Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(false))))

  // -- Sampling --------------------------------------------------------------

  def sample(fraction: Double): DataFrame = sample(withReplacement = false, fraction)
  def sample(fraction: Double, seed: Long): DataFrame =
    sample(withReplacement = false, fraction, seed)
  def sample(withReplacement: Boolean, fraction: Double): DataFrame =
    sample(withReplacement, fraction, scala.util.Random.nextLong())
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame =
    withInput(
      RelType.Sample(
        proto.Sample(
          input = Some(relation),
          lowerBound = 0.0d,
          upperBound = fraction,
          withReplacement = Some(withReplacement),
          seed = Some(seed))))

  // -- Grouping & aggregation ------------------------------------------------

  def groupBy(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset =
    groupBy((col1 +: cols).map(Column.fromName)*)

  def rollup(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
  def cube(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_CUBE)

  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs*)

  // -- Actions ---------------------------------------------------------------

  def collect(): Array[Row] = sparkSession.execute(plan).toArray
  def collectAsList(): java.util.List[Row] = collect().toList.asJava
  def toLocalIterator(): java.util.Iterator[Row] = sparkSession.execute(plan).iterator.asJava

  def count(): Long = {
    val agg = proto.Aggregate(
      input = Some(relation),
      groupType = proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY,
      aggregateExpressions = Seq(Column.fn("count", Column.lit(1)).expr))
    val rel = sparkSession.newRelation(RelType.Aggregate(agg))
    val rows = sparkSession.execute(proto.Plan(proto.Plan.OpType.Root(rel))).toArray
    if (rows.isEmpty) 0L else rows.head.getLong(0)
  }

  def head(n: Int): Array[Row] = limit(n).collect()
  def head(): Row = head(1).head
  def first(): Row = head()
  def take(n: Int): Array[Row] = head(n)
  def takeAsList(n: Int): java.util.List[Row] = take(n).toList.asJava
  def isEmpty: Boolean = limit(1).collect().isEmpty

  def show(): Unit = show(20)
  def show(numRows: Int): Unit = show(numRows, truncate = true)
  def show(truncate: Boolean): Unit = show(20, truncate)
  def show(numRows: Int, truncate: Boolean): Unit =
    show(numRows, if (truncate) 20 else 0, vertical = false)
  def show(numRows: Int, truncate: Int): Unit = show(numRows, truncate, vertical = false)
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = {
    val rel = sparkSession.newRelation(
      RelType.ShowString(
        proto.ShowString(
          input = Some(relation),
          numRows = numRows,
          truncate = truncate,
          vertical = vertical)))
    val rows = sparkSession.execute(proto.Plan(proto.Plan.OpType.Root(rel))).toArray
    if (rows.nonEmpty) print(rows.head.getString(0))
  }

  // -- Explain ---------------------------------------------------------------

  def explain(): Unit = explain("simple")
  def explain(extended: Boolean): Unit = explain(if (extended) "extended" else "simple")
  def explain(mode: String): Unit = {
    val explainMode = mode.toLowerCase match {
      case "simple" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
      case "extended" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      case "codegen" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
      case "cost" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
      case "formatted" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
      case other => throw new IllegalArgumentException(s"Unsupported explain mode: $other")
    }
    print(sparkSession.client.explainString(plan, explainMode))
  }

  // -- Temp views ------------------------------------------------------------

  def createTempView(viewName: String): Unit = createView(viewName, replace = false, global = false)
  def createOrReplaceTempView(viewName: String): Unit =
    createView(viewName, replace = true, global = false)
  def createGlobalTempView(viewName: String): Unit =
    createView(viewName, replace = false, global = true)
  def createOrReplaceGlobalTempView(viewName: String): Unit =
    createView(viewName, replace = true, global = true)

  private def createView(viewName: String, replace: Boolean, global: Boolean): Unit =
    sparkSession.executeCommand(
      proto.Command(
        proto.Command.CommandType.CreateDataframeView(
          proto.CreateDataFrameViewCommand(
            input = Some(relation),
            name = viewName,
            isGlobal = global,
            replace = replace))))

  // -- Statistics / summary --------------------------------------------------

  /** Computes basic statistics (count, mean, stddev, min, max) for numeric and string columns. */
  def describe(cols: String*): DataFrame =
    withInput(RelType.Describe(proto.StatDescribe(input = Some(relation), cols = cols)))

  /** Computes the requested summary statistics; defaults match Spark's `summary()`. */
  def summary(statistics: String*): DataFrame =
    withInput(RelType.Summary(proto.StatSummary(input = Some(relation), statistics = statistics)))

  // -- Reading / writing / na / stat / observe -------------------------------

  /** Interface for saving the content of this Dataset to external storage. */
  def write: DataFrameWriter = new DataFrameWriter(this)

  /** Interface for saving the content of a streaming Dataset to external storage. */
  def writeStream: streaming.DataStreamWriter = new streaming.DataStreamWriter(this)

  /** Returns a [[DataFrameNaFunctions]] for working with missing data. */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(this)

  /** Returns a [[DataFrameStatFunctions]] for statistic functions. */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(this)

  /** Defines named observed metrics computed while this Dataset is processed. */
  def observe(observation: Observation, expr: Column, exprs: Column*): DataFrame =
    withInput(RelType.CollectMetrics(observation.markObserved(this, expr +: exprs)))

  /** Concisely applies a transformation to this Dataset. */
  def transform(t: Dataset => Dataset): Dataset = t(this)

  // -- Misc ------------------------------------------------------------------

  def sameSemantics(other: Dataset): Boolean =
    sparkSession.client.sameSemantics(plan, other.plan)
  def semanticHash(): Int = sparkSession.client.semanticHash(plan)
}

private[sql] object Dataset {

  def exprColumn(e: String): Column =
    new Column(
      proto.Expression(exprType = proto.Expression.ExprType.ExpressionString(
          proto.Expression.ExpressionString(expression = e))))

  def toSortOrder(col: Column): proto.Expression.SortOrder =
    col.expr.exprType match {
      case proto.Expression.ExprType.SortOrder(order) => order
      case _ =>
        proto.Expression.SortOrder(
          child = Some(col.expr),
          direction = proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
    }

  def toJoinType(joinType: String): proto.Join.JoinType =
    joinType.toLowerCase.replaceAll("[_ ]", "") match {
      case "inner" => proto.Join.JoinType.JOIN_TYPE_INNER
      case "cross" => proto.Join.JoinType.JOIN_TYPE_CROSS
      case "outer" | "full" | "fullouter" => proto.Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "left" | "leftouter" => proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter" => proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "leftsemi" | "semi" => proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "leftanti" | "anti" => proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case other => throw new IllegalArgumentException(s"Unsupported join type: $other")
    }
}
