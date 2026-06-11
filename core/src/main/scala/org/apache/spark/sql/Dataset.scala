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

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{DataTypeProtoConverter, SparkResult}
import org.apache.spark.sql.types.StructType

/**
 * A Dataset is a strongly typed collection of domain-specific objects that can be transformed in
 * parallel using functional or relational operations. Each Dataset also has an untyped view called
 * a [[DataFrame]], which is a Dataset of [[Row]].
 *
 * This is the Spark Connect (Scala 3) client implementation. A `Dataset` is lazy: it wraps a
 * `proto.Relation` (the logical plan) and a [[SparkSession]]. Transformations build a new relation
 * referencing the current one; actions submit the plan to the server for execution.
 *
 * The untyped `DataFrame` (= `Dataset[Row]`) surface is fully supported. Typed encoders (`as[U:
 * Encoder]`, typed `map`) are a planned enhancement; until then `collect()` yields [[Row]]s and
 * `as[U]` reinterprets the wrapper.
 *
 * @groupname basic     Basic Dataset functions
 * @groupname action     Actions
 * @groupname untypedrel Untyped transformations
 * @since 0.1.0
 */
class Dataset[T] private[sql] (
    @transient val sparkSession: SparkSession,
    private[sql] val relation: proto.Relation
) extends Serializable {

  // ---------------------------------------------------------------------------
  // Internal plan-building helpers (the heart of the dataset lane).
  // ---------------------------------------------------------------------------

  /** The plan id assigned to this relation, used to disambiguate `df("col")` references. */
  private[sql] def planId: Option[Long] = relation.common.flatMap(_.planId)

  /** A fresh relation carrying a new, unique plan id. Add the operator via `.withXxx(...)`. */
  private def base: proto.Relation =
    proto.Relation(common = Some(proto.RelationCommon(planId = Some(sparkSession.newPlanId()))))

  private def df(rel: proto.Relation): DataFrame = new Dataset[Row](sparkSession, rel)
  private def typed(rel: proto.Relation): Dataset[T] = new Dataset[T](sparkSession, rel)

  /** The full `proto.Plan` (root relation) for execution / analysis. */
  private[sql] def plan: proto.Plan = proto.Plan(opType = proto.Plan.OpType.Root(relation))

  private def aliasExpr(e: proto.Expression, name: String): proto.Expression =
    proto.Expression().withAlias(proto.Expression.Alias(expr = Some(e), name = Seq(name)))

  /** Build an unresolved attribute carrying this dataset's plan id (for `df("col")`). */
  private def attr(colName: String): proto.Expression =
    proto
      .Expression()
      .withUnresolvedAttribute(
        proto.Expression.UnresolvedAttribute(unparsedIdentifier = colName, planId = planId)
      )

  private def withResult[A](f: SparkResult => A): A = {
    val result =
      new SparkResult(
        sparkSession.execute(plan).asScala,
        sparkSession.allocator,
        sparkSession.timeZoneId
      )
    try {
      val out = f(result)
      // Deliver any observed metrics (Dataset.observe) to registered Observations.
      if (!sparkSession.observations.isEmpty) {
        sparkSession.completeObservations(result.observedMetrics)
      }
      out
    } finally result.close()
  }

  private def checkSameSession(other: Dataset[?]): Unit =
    require(other.sparkSession eq sparkSession, "Cannot combine Datasets from different sessions.")

  // ---------------------------------------------------------------------------
  // Basic functions
  // ---------------------------------------------------------------------------

  /** Returns the schema of this Dataset (resolved by the server). @group basic */
  def schema: StructType = {
    val resp = sparkSession.analyze(
      proto.AnalyzePlanRequest.Analyze.Schema(proto.AnalyzePlanRequest.Schema(plan = Some(plan)))
    )
    DataTypeProtoConverter.toCatalystType(resp.getSchema.getSchema).asInstanceOf[StructType]
  }

  /** Column names. @group basic */
  def columns: Array[String] = schema.fieldNames

  /** `(name, dataType.catalogString)` pairs. @group basic */
  def dtypes: Array[(String, String)] = schema.fields.map(f => (f.name, f.dataType.catalogString))

  /** Prints the schema to the console in a tree format. @group basic */
  def printSchema(): Unit = println(treeString(schema))

  private def treeString(s: StructType): String = {
    val sb = new StringBuilder("root\n")
    def rec(st: StructType, prefix: String): Unit =
      st.fields.foreach { f =>
        sb.append(s"$prefix|-- ${f.name}: ${f.dataType.typeName} (nullable = ${f.nullable})\n")
        f.dataType match {
          case child: StructType => rec(child, prefix + "|    ")
          case _ =>
        }
      }
    rec(s, " ")
    sb.toString
  }

  /** Prints the plan to the console. @group basic */
  def explain(extended: Boolean): Unit = explain(if (extended) "extended" else "simple")

  /** Prints the plan to the console. @group basic */
  def explain(): Unit = explain("simple")

  def explain(mode: String): Unit = {
    val explainMode = mode.trim.toLowerCase match {
      case "simple" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
      case "extended" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      case "codegen" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
      case "cost" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
      case "formatted" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
      case other => throw new IllegalArgumentException(s"Unsupported explain mode: $other")
    }
    val resp = sparkSession.analyze(
      proto.AnalyzePlanRequest.Analyze.Explain(
        proto.AnalyzePlanRequest.Explain(plan = Some(plan), explainMode = explainMode)
      )
    )
    println(resp.getExplain.explainString)
  }

  def isLocal: Boolean =
    sparkSession
      .analyze(
        proto.AnalyzePlanRequest.Analyze
          .IsLocal(proto.AnalyzePlanRequest.IsLocal(plan = Some(plan)))
      )
      .getIsLocal
      .isLocal

  def isStreaming: Boolean =
    sparkSession
      .analyze(
        proto.AnalyzePlanRequest.Analyze
          .IsStreaming(proto.AnalyzePlanRequest.IsStreaming(plan = Some(plan)))
      )
      .getIsStreaming
      .isStreaming

  def inputFiles: Array[String] =
    sparkSession
      .analyze(
        proto.AnalyzePlanRequest.Analyze.InputFiles(
          proto.AnalyzePlanRequest.InputFiles(plan = Some(plan))
        )
      )
      .getInputFiles
      .files
      .toArray

  def isEmpty: Boolean = limit(1).withResult(_.length == 0)

  /** Converts this strongly typed Dataset to a generic [[DataFrame]]. @group basic */
  def toDF(): DataFrame = new Dataset[Row](sparkSession, relation)

  /** Returns a new DataFrame with renamed columns by position. @group basic */
  def toDF(colNames: String*): DataFrame =
    df(base.withToDf(proto.ToDF(input = Some(relation), columnNames = colNames)))

  /** Projects the Dataset onto the given schema. @group basic */
  def to(schema: StructType): DataFrame =
    df(
      base.withToSchema(
        proto.ToSchema(
          input = Some(relation),
          schema = Some(DataTypeProtoConverter.toConnectProtoType(schema))
        )
      )
    )

  /** Reinterprets this Dataset as `Dataset[U]`. Typed decode is a planned enhancement. */
  def as[U]: Dataset[U] = new Dataset[U](sparkSession, relation)

  /** Returns a new Dataset with an alias set. @group basic */
  def as(alias: String): Dataset[T] =
    typed(base.withSubqueryAlias(proto.SubqueryAlias(input = Some(relation), alias = alias)))

  def alias(alias: String): Dataset[T] = as(alias)

  // ---------------------------------------------------------------------------
  // Column selection
  // ---------------------------------------------------------------------------

  /** Selects a column by name, bound to this Dataset's plan id. @group untypedrel */
  def col(colName: String): Column =
    if (colName == "*")
      Column(proto.Expression().withUnresolvedStar(proto.Expression.UnresolvedStar()))
    else Column(attr(colName))

  /** Selects a column by name. */
  def apply(colName: String): Column = col(colName)

  def colRegex(colName: String): Column =
    Column(
      proto
        .Expression()
        .withUnresolvedRegex(proto.Expression.UnresolvedRegex(colName = colName, planId = planId))
    )

  // ---------------------------------------------------------------------------
  // Untyped transformations
  // ---------------------------------------------------------------------------

  /** @group untypedrel */
  def select(cols: Column*): DataFrame =
    df(base.withProject(proto.Project(input = Some(relation), expressions = cols.map(_.expr))))

  /** @group untypedrel */
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(functions.col)*)

  /** @group untypedrel */
  def selectExpr(exprs: String*): DataFrame = select(exprs.map(functions.expr)*)

  /** @group untypedrel */
  def filter(condition: Column): Dataset[T] =
    typed(base.withFilter(proto.Filter(input = Some(relation), condition = Some(condition.expr))))

  /** @group untypedrel */
  def filter(conditionExpr: String): Dataset[T] = filter(functions.expr(conditionExpr))

  /** @group untypedrel */
  def where(condition: Column): Dataset[T] = filter(condition)

  /** @group untypedrel */
  def where(conditionExpr: String): Dataset[T] = filter(conditionExpr)

  /** @group untypedrel */
  def withColumn(colName: String, col: Column): DataFrame = withColumns(Seq(colName), Seq(col))

  /** @group untypedrel */
  def withColumns(colsMap: Map[String, Column]): DataFrame = {
    val (names, cols) = colsMap.toSeq.unzip
    withColumns(names, cols)
  }

  private def withColumns(names: Seq[String], cols: Seq[Column]): DataFrame = {
    require(names.size == cols.size, "The size of column names must equal the size of columns.")
    val aliases = names.zip(cols).map { case (n, c) =>
      proto.Expression.Alias(expr = Some(c.expr), name = Seq(n))
    }
    df(base.withWithColumns(proto.WithColumns(input = Some(relation), aliases = aliases)))
  }

  /** @group untypedrel */
  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    withColumnsRenamed(Map(existingName -> newName))

  /** @group untypedrel */
  def withColumnsRenamed(colsMap: Map[String, String]): DataFrame = {
    val renames = colsMap.toSeq.map { case (from, to) =>
      proto.WithColumnsRenamed.Rename(colName = from, newColName = to)
    }
    df(
      base.withWithColumnsRenamed(
        proto.WithColumnsRenamed(input = Some(relation), renames = renames)
      )
    )
  }

  /** @group untypedrel */
  def drop(colNames: String*): DataFrame =
    df(base.withDrop(proto.Drop(input = Some(relation), columnNames = colNames)))

  /** @group untypedrel */
  def drop(col: Column, cols: Column*): DataFrame =
    df(base.withDrop(proto.Drop(input = Some(relation), columns = (col +: cols).map(_.expr))))

  /** @group untypedrel */
  def withMetadataColumn: DataFrame = toDF()

  // ---------------------------------------------------------------------------
  // Sorting / limiting
  // ---------------------------------------------------------------------------

  private def sortInternal(global: Boolean, sortCols: Seq[Column]): Dataset[T] =
    typed(
      base.withSort(
        proto.Sort(
          input = Some(relation),
          order = sortCols.map(_.sortOrder),
          isGlobal = Some(global)
        )
      )
    )

  /** @group untypedrel */
  def sort(sortExprs: Column*): Dataset[T] = sortInternal(global = true, sortExprs)

  /** @group untypedrel */
  def sort(sortCol: String, sortCols: String*): Dataset[T] =
    sort((sortCol +: sortCols).map(functions.col)*)

  /** @group untypedrel */
  def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs*)

  /** @group untypedrel */
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = sort(sortCol, sortCols*)

  /** @group untypedrel */
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = sortInternal(global = false, sortExprs)

  /** @group untypedrel */
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] =
    sortInternal(global = false, (sortCol +: sortCols).map(functions.col))

  /** @group untypedrel */
  def limit(n: Int): Dataset[T] =
    typed(base.withLimit(proto.Limit(input = Some(relation), limit = n)))

  /** @group untypedrel */
  def offset(n: Int): Dataset[T] =
    typed(base.withOffset(proto.Offset(input = Some(relation), offset = n)))

  // ---------------------------------------------------------------------------
  // Distinct / dedup
  // ---------------------------------------------------------------------------

  /** @group untypedrel */
  def distinct(): Dataset[T] = dropDuplicates()

  /** @group untypedrel */
  def dropDuplicates(): Dataset[T] =
    typed(
      base.withDeduplicate(proto.Deduplicate(input = Some(relation), allColumnsAsKeys = Some(true)))
    )

  /** @group untypedrel */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] =
    typed(base.withDeduplicate(proto.Deduplicate(input = Some(relation), columnNames = colNames)))

  /** @group untypedrel */
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = dropDuplicates(col1 +: cols)

  // ---------------------------------------------------------------------------
  // Set operations
  // ---------------------------------------------------------------------------

  private def setOp(
      other: Dataset[T],
      op: proto.SetOperation.SetOpType,
      isAll: Boolean,
      byName: Boolean = false,
      allowMissing: Boolean = false
  ): Dataset[T] = {
    checkSameSession(other)
    typed(
      base.withSetOp(
        proto.SetOperation(
          leftInput = Some(relation),
          rightInput = Some(other.relation),
          setOpType = op,
          isAll = Some(isAll),
          byName = Some(byName),
          allowMissingColumns = Some(allowMissing)
        )
      )
    )
  }

  /** @group untypedrel */
  def union(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION, isAll = true)

  /** @group untypedrel */
  def unionAll(other: Dataset[T]): Dataset[T] = union(other)

  /** @group untypedrel */
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean = false): Dataset[T] =
    setOp(
      other,
      proto.SetOperation.SetOpType.SET_OP_TYPE_UNION,
      isAll = true,
      byName = true,
      allowMissing = allowMissingColumns
    )

  /** @group untypedrel */
  def intersect(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = false)

  /** @group untypedrel */
  def intersectAll(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = true)

  /** @group untypedrel */
  def except(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = false)

  /** @group untypedrel */
  def exceptAll(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = true)

  // ---------------------------------------------------------------------------
  // Joins
  // ---------------------------------------------------------------------------

  private def toJoinType(name: String): proto.Join.JoinType = name.trim.toLowerCase match {
    case "inner" => proto.Join.JoinType.JOIN_TYPE_INNER
    case "cross" => proto.Join.JoinType.JOIN_TYPE_CROSS
    case "outer" | "full" | "fullouter" | "full_outer" => proto.Join.JoinType.JOIN_TYPE_FULL_OUTER
    case "left" | "leftouter" | "left_outer" => proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER
    case "right" | "rightouter" | "right_outer" => proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
    case "semi" | "leftsemi" | "left_semi" => proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI
    case "anti" | "leftanti" | "left_anti" => proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI
    case other => throw new IllegalArgumentException(s"Unsupported join type '$other'.")
  }

  private def buildJoin(right: Dataset[?])(f: proto.Join => proto.Join): DataFrame = {
    checkSameSession(right)
    df(base.withJoin(f(proto.Join(left = Some(relation), right = Some(right.relation)))))
  }

  /** Inner equi-join on a single column name. @group untypedrel */
  def join(right: Dataset[?]): DataFrame =
    buildJoin(right)(_.copy(joinType = proto.Join.JoinType.JOIN_TYPE_INNER))

  /** @group untypedrel */
  def join(right: Dataset[?], usingColumn: String): DataFrame =
    join(right, Seq(usingColumn), "inner")

  /** @group untypedrel */
  def join(right: Dataset[?], usingColumns: Seq[String]): DataFrame =
    join(right, usingColumns, "inner")

  /** @group untypedrel */
  def join(right: Dataset[?], usingColumns: Seq[String], joinType: String): DataFrame =
    buildJoin(right)(_.copy(joinType = toJoinType(joinType), usingColumns = usingColumns))

  /** @group untypedrel */
  def join(right: Dataset[?], joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

  /** @group untypedrel */
  def join(right: Dataset[?], joinExprs: Column, joinType: String): DataFrame =
    buildJoin(right)(_.copy(joinType = toJoinType(joinType), joinCondition = Some(joinExprs.expr)))

  /** @group untypedrel */
  def crossJoin(right: Dataset[?]): DataFrame =
    buildJoin(right)(_.copy(joinType = proto.Join.JoinType.JOIN_TYPE_CROSS))

  // ---------------------------------------------------------------------------
  // Grouping / aggregation
  // ---------------------------------------------------------------------------

  /** @group untypedrel */
  def groupBy(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)

  /** @group untypedrel */
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset =
    groupBy((col1 +: cols).map(functions.col)*)

  /** @group untypedrel */
  def rollup(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)

  /** @group untypedrel */
  def rollup(col1: String, cols: String*): RelationalGroupedDataset =
    rollup((col1 +: cols).map(functions.col)*)

  /** @group untypedrel */
  def cube(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(toDF(), cols, proto.Aggregate.GroupType.GROUP_TYPE_CUBE)

  /** @group untypedrel */
  def cube(col1: String, cols: String*): RelationalGroupedDataset =
    cube((col1 +: cols).map(functions.col)*)

  /** @group untypedrel */
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs*)

  /** @group untypedrel */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /** @group untypedrel */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    groupBy().agg(aggExpr, aggExprs*)

  // ---------------------------------------------------------------------------
  // Sampling / partitioning
  // ---------------------------------------------------------------------------

  /** @group untypedrel */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] =
    typed(
      base.withSample(
        proto.Sample(
          input = Some(relation),
          lowerBound = 0.0d,
          upperBound = fraction,
          withReplacement = Some(withReplacement),
          seed = Some(seed)
        )
      )
    )

  /** @group untypedrel */
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    sample(withReplacement, fraction, scala.util.Random.nextLong())

  /** @group untypedrel */
  def sample(fraction: Double, seed: Long): Dataset[T] =
    sample(withReplacement = false, fraction, seed)

  /** @group untypedrel */
  def sample(fraction: Double): Dataset[T] = sample(withReplacement = false, fraction)

  /** @group untypedrel */
  def repartition(numPartitions: Int): Dataset[T] =
    typed(
      base.withRepartition(
        proto.Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(true)
        )
      )
    )

  /** @group untypedrel */
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    typed(
      base.withRepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(_.expr),
          numPartitions = Some(numPartitions)
        )
      )
    )

  /** @group untypedrel */
  def repartition(partitionExprs: Column*): Dataset[T] =
    typed(
      base.withRepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(_.expr)
        )
      )
    )

  /** @group untypedrel */
  def coalesce(numPartitions: Int): Dataset[T] =
    typed(
      base.withRepartition(
        proto.Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(false)
        )
      )
    )

  // ---------------------------------------------------------------------------
  // Describe / summary / hint / observe
  // ---------------------------------------------------------------------------

  /** @group action */
  def describe(cols: String*): DataFrame =
    df(base.withDescribe(proto.StatDescribe(input = Some(relation), cols = cols)))

  /** @group action */
  def summary(statistics: String*): DataFrame =
    df(base.withSummary(proto.StatSummary(input = Some(relation), statistics = statistics)))

  /** @group basic */
  def hint(name: String, parameters: Any*): Dataset[T] =
    typed(
      base.withHint(
        proto.Hint(
          input = Some(relation),
          name = name,
          parameters = parameters.map(p => functions.lit(p).expr)
        )
      )
    )

  /** Observe (named) metrics on this Dataset. @group basic */
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] =
    typed(
      base.withCollectMetrics(
        proto.CollectMetrics(
          input = Some(relation),
          name = name,
          metrics = (expr +: exprs).map(_.expr)
        )
      )
    )

  /**
   * Observe metrics through an [[Observation]] instance. The metrics are available on the
   * observation after the first action runs on the returned Dataset. @group basic
   */
  def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T] = {
    sparkSession.registerObservation(observation)
    observe(observation.name, expr, exprs*)
  }

  // ---------------------------------------------------------------------------
  // NA / stat namespaces
  // ---------------------------------------------------------------------------

  /** Returns a [[DataFrameNaFunctions]] for working with missing data. @group untypedrel */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(toDF())

  /** Returns a [[DataFrameStatFunctions]] for statistic functions. @group untypedrel */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(toDF())

  // ---------------------------------------------------------------------------
  // Temp views (commands)
  // ---------------------------------------------------------------------------

  /** @group basic */
  def createTempView(viewName: String): Unit = createView(viewName, replace = false, global = false)

  /** @group basic */
  def createOrReplaceTempView(viewName: String): Unit =
    createView(viewName, replace = true, global = false)

  /** @group basic */
  def createGlobalTempView(viewName: String): Unit =
    createView(viewName, replace = false, global = true)

  /** @group basic */
  def createOrReplaceGlobalTempView(viewName: String): Unit =
    createView(viewName, replace = true, global = true)

  private def createView(name: String, replace: Boolean, global: Boolean): Unit = {
    val command = proto
      .Command()
      .withCreateDataframeView(
        proto.CreateDataFrameViewCommand(
          input = Some(relation),
          name = name,
          isGlobal = global,
          replace = replace
        )
      )
    sparkSession.execute(command)
  }

  // ---------------------------------------------------------------------------
  // Write
  // ---------------------------------------------------------------------------

  /** Interface for saving the content of the (non-streaming) Dataset out. @group basic */
  def write: DataFrameWriter[T] = new DataFrameWriter[T](this)

  /** Create a write configuration builder for v2 sources. @group basic */
  def writeTo(table: String): DataFrameWriterV2[T] = new DataFrameWriterV2[T](table, this)

  /** Interface for saving the content of the streaming Dataset out to a streaming sink. */
  def writeStream: org.apache.spark.sql.streaming.DataStreamWriter[T] =
    new org.apache.spark.sql.streaming.DataStreamWriter[T](this)

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  /** Returns all rows as an array. @group action */
  def collect(): Array[T] = withResult(_.toArray).asInstanceOf[Array[T]]

  /** Returns all rows as a Java list. @group action */
  def collectAsList(): java.util.List[T] = collect().toList.asJava

  /** Returns an iterator that streams rows from the server. @group action */
  def toLocalIterator(): java.util.Iterator[T] =
    withResult(r => r.toArray.iterator).asInstanceOf[Iterator[T]].asJava

  /** Number of rows. @group action */
  def count(): Long = {
    val countExpr = aliasExpr(Column.fn("count", functions.lit(1)).expr, "count")
    df(
      base.withAggregate(
        proto.Aggregate(
          input = Some(relation),
          groupType = proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY,
          aggregateExpressions = Seq(countExpr)
        )
      )
    ).collect().head.getLong(0)
  }

  /** First `n` rows. @group action */
  def head(n: Int): Array[T] = limit(n).collect()

  /** First row. @group action */
  def head(): T = head(1).head

  /** First row. @group action */
  def first(): T = head()

  /** First `n` rows. @group action */
  def take(n: Int): Array[T] = head(n)

  /** First `n` rows as a Java list. @group action */
  def takeAsList(n: Int): java.util.List[T] = head(n).toList.asJava

  /** Last `n` rows. @group action */
  def tail(n: Int): Array[T] =
    df(base.withTail(proto.Tail(input = Some(relation), limit = n)))
      .collect()
      .asInstanceOf[Array[T]]

  /** Displays the top rows in a tabular form. @group action */
  def show(): Unit = show(20)

  /** @group action */
  def show(numRows: Int): Unit = show(numRows, truncate = true)

  /** @group action */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /** @group action */
  def show(numRows: Int, truncate: Boolean): Unit =
    show(numRows, if (truncate) 20 else 0, vertical = false)

  /** @group action */
  def show(numRows: Int, truncate: Int): Unit = show(numRows, truncate, vertical = false)

  /** @group action */
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = {
    // The server formats the table and returns it as a single-cell DataFrame.
    val rel = base.withShowString(
      proto.ShowString(
        input = Some(relation),
        numRows = numRows,
        truncate = truncate,
        vertical = vertical
      )
    )
    print(new Dataset[Row](sparkSession, rel).collect().head.getString(0))
  }

  /** Applies a function to each row. @group action */
  def foreach(f: Row => Unit): Unit = collect().asInstanceOf[Array[Row]].foreach(f)

  // ---------------------------------------------------------------------------
  // Closure-based typed transformations - require shipping user classes as
  // artifacts to the server, which is out of scope for the v0.1 client.
  // ---------------------------------------------------------------------------

  private def unsupportedClosure(op: String): Nothing =
    throw new UnsupportedOperationException(
      s"$op requires shipping user-defined closures to the server (artifact upload), " +
        "which is not yet supported by the Scala 3 Spark Connect client."
    )

  def map[U](f: T => U): Dataset[U] = unsupportedClosure("Dataset.map")
  def flatMap[U](f: T => IterableOnce[U]): Dataset[U] = unsupportedClosure("Dataset.flatMap")
  def filter(f: T => Boolean): Dataset[T] = unsupportedClosure("Dataset.filter(func)")
  def mapPartitions[U](f: Iterator[T] => Iterator[U]): Dataset[U] =
    unsupportedClosure("Dataset.mapPartitions")
  def reduce(f: (T, T) => T): T = unsupportedClosure("Dataset.reduce")

  // ---------------------------------------------------------------------------
  // Additional relational transformations and lifecycle operations
  // ---------------------------------------------------------------------------

  /** Concise syntax for chaining custom transformations. @group untypedrel */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  /** Returns a new Dataset range-partitioned by the given expressions into `numPartitions`. */
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    typed(
      base.withRepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(c => proto.Expression().withSortOrder(c.sortOrder)),
          numPartitions = Some(numPartitions)
        )
      )
    )

  /** Returns a new Dataset range-partitioned by the given expressions. */
  def repartitionByRange(partitionExprs: Column*): Dataset[T] =
    typed(
      base.withRepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs.map(c => proto.Expression().withSortOrder(c.sortOrder))
        )
      )
    )

  /** Unpivots a DataFrame from wide to long format. @group untypedrel */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    df(
      base.withUnpivot(
        proto.Unpivot(
          input = Some(relation),
          ids = ids.toSeq.map(_.expr),
          values = Some(proto.Unpivot.Values(values = values.toSeq.map(_.expr))),
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      )
    )

  /**
   * Unpivots a DataFrame, inferring the value columns from those not in `ids`. @group untypedrel
   */
  def unpivot(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame =
    df(
      base.withUnpivot(
        proto.Unpivot(
          input = Some(relation),
          ids = ids.toSeq.map(_.expr),
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      )
    )

  /** Alias for [[unpivot]]. @group untypedrel */
  def melt(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    unpivot(ids, values, variableColumnName, valueColumnName)

  /** Alias for [[unpivot]]. @group untypedrel */
  def melt(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame =
    unpivot(ids, variableColumnName, valueColumnName)

  /** Transposes the DataFrame, using the first column as the new column names. @group untypedrel */
  def transpose(): DataFrame =
    df(base.withTranspose(proto.Transpose(input = Some(relation))))

  /** Transposes the DataFrame using `indexColumn` as the new column names. @group untypedrel */
  def transpose(indexColumn: Column): DataFrame =
    df(
      base.withTranspose(
        proto.Transpose(input = Some(relation), indexColumns = Seq(indexColumn.expr))
      )
    )

  /** Defines an event-time watermark for this streaming Dataset. @group streaming */
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] =
    typed(
      base.withWithWatermark(
        proto.WithWatermark(
          input = Some(relation),
          eventTime = eventTime,
          delayThreshold = delayThreshold
        )
      )
    )

  /** Randomly splits this Dataset with the provided weights and a fixed seed. @group action */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    require(weights.forall(_ >= 0), "Weights must be nonnegative")
    val sum = weights.sum
    require(sum > 0, "Sum of weights must be positive")
    val bounds = weights.scanLeft(0.0)((acc, w) => acc + w / sum)
    weights.indices.map { i =>
      typed(
        base.withSample(
          proto.Sample(
            input = Some(relation),
            lowerBound = bounds(i),
            upperBound = bounds(i + 1),
            withReplacement = Some(false),
            seed = Some(seed),
            deterministicOrder = true
          )
        )
      )
    }.toArray
  }

  /** Randomly splits this Dataset with the provided weights. @group action */
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    randomSplit(weights, scala.util.Random.nextLong())

  /** Returns the content as a DataFrame of JSON strings (column `value`). @group action */
  def toJSON: DataFrame = {
    val jsonExpr = aliasExpr(functions.expr("to_json(struct(*))").expr, "value")
    df(base.withProject(proto.Project(input = Some(relation), expressions = Seq(jsonExpr))))
  }

  /** Returns true if this Dataset and `other` produce the same logical plan. @group basic */
  def sameSemantics(other: Dataset[T]): Boolean = {
    checkSameSession(other)
    sparkSession
      .analyze(
        proto.AnalyzePlanRequest.Analyze.SameSemantics(
          proto.AnalyzePlanRequest
            .SameSemantics(targetPlan = Some(plan), otherPlan = Some(other.plan))
        )
      )
      .getSameSemantics
      .result
  }

  /** Returns a hash of the logical query plan. @group basic */
  def semanticHash(): Int =
    sparkSession
      .analyze(
        proto.AnalyzePlanRequest.Analyze
          .SemanticHash(proto.AnalyzePlanRequest.SemanticHash(plan = Some(plan)))
      )
      .getSemanticHash
      .result

  /** Eagerly checkpoints this Dataset to reliable storage and returns the checkpointed copy. */
  def checkpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = true)

  /** Checkpoints this Dataset to reliable storage. @group basic */
  def checkpoint(eager: Boolean): Dataset[T] = checkpoint(eager, reliableCheckpoint = true)

  /** Locally checkpoints this Dataset (eager). @group basic */
  def localCheckpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = false)

  /** Locally checkpoints this Dataset. @group basic */
  def localCheckpoint(eager: Boolean): Dataset[T] = checkpoint(eager, reliableCheckpoint = false)

  private def checkpoint(eager: Boolean, reliableCheckpoint: Boolean): Dataset[T] = {
    val command = proto
      .Command()
      .withCheckpointCommand(
        proto
          .CheckpointCommand(relation = Some(relation), local = !reliableCheckpoint, eager = eager)
      )
    val cached = sparkSession
      .executeCommandResponses(command)
      .find(_.responseType.isCheckpointCommandResult)
      .getOrElse(throw new RuntimeException("CheckpointCommandResult missing from server response"))
      .getCheckpointCommandResult
      .getRelation
    typed(base.withCachedRemoteRelation(cached))
  }

  /** Persists this Dataset with the default storage level (`MEMORY_AND_DISK`). @group basic */
  def persist(): this.type = persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

  /** Persists this Dataset with the given storage level. @group basic */
  def persist(newLevel: org.apache.spark.storage.StorageLevel): this.type = {
    sparkSession.analyze(
      proto.AnalyzePlanRequest.Analyze.Persist(
        proto.AnalyzePlanRequest
          .Persist(relation = Some(relation), storageLevel = Some(newLevel.toProto))
      )
    )
    this
  }

  /** Persists this Dataset with the default storage level. @group basic */
  def cache(): this.type = persist()

  /** Marks this Dataset as non-persistent (blocking until blocks are freed). @group basic */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.analyze(
      proto.AnalyzePlanRequest.Analyze.Unpersist(
        proto.AnalyzePlanRequest.Unpersist(relation = Some(relation), blocking = Some(blocking))
      )
    )
    this
  }

  /** Marks this Dataset as non-persistent. @group basic */
  def unpersist(): this.type = unpersist(blocking = false)

  /** Returns the current storage level of this Dataset. @group basic */
  def storageLevel: org.apache.spark.storage.StorageLevel =
    org.apache.spark.storage.StorageLevel.fromProto(
      sparkSession
        .analyze(
          proto.AnalyzePlanRequest.Analyze
            .GetStorageLevel(proto.AnalyzePlanRequest.GetStorageLevel(relation = Some(relation)))
        )
        .getGetStorageLevel
        .getStorageLevel
    )

  override def toString: String =
    try {
      val fields = schema.fields.take(2).map(f => s"${f.name}: ${f.dataType.typeName}")
      val more =
        if (schema.fields.length > 2) s" ... ${schema.fields.length - 2} more fields" else ""
      s"[${fields.mkString(", ")}$more]"
    } catch {
      case scala.util.control.NonFatal(e) => s"Invalid Dataset; ${e.getMessage}"
    }
}

object Dataset {

  /** Wraps a relation as an untyped [[DataFrame]]. */
  private[sql] def ofRows(session: SparkSession, relation: proto.Relation): DataFrame =
    new Dataset[Row](session, relation)
}
