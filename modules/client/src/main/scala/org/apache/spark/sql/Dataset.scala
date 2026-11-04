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

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Relation.RelType
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.{Metadata, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * A distributed collection of rows. A [[Dataset]] is lazy: transformations build up a protobuf
 * logical plan, and nothing executes until an action (e.g. [[collect]], [[show]], [[count]]) is
 * called.
 *
 * Mirrors the public surface of `org.apache.spark.sql.Dataset` over the Spark Connect protocol:
 * operations that keep the row type (filtering, sorting, set operations, repartitioning, ...)
 * return a `Dataset[T]`, while operations that change the schema return a [[DataFrame]].
 */
class Dataset[T] private[sql] (
    val sparkSession: SparkSession,
    /**
     * The protobuf logical plan (a [[proto.Relation]]) backing this Dataset.
     *
     * Exposed for Spark Connect plugin authors who need to embed an existing Dataset inside a
     * custom relation -- for example, referencing `df.relation` as an input to a plugin message
     * before packing it via
     * [[SparkSession.newDataFrame(extension:com\.google\.protobuf\.any\.Any)*]].
     */
    val relation: proto.Relation,
    private[sql] val encoder: Encoder[T]
) {

  /**
   * The full protobuf [[proto.Plan]] (a root plan wrapping [[relation]]) that would be sent to the
   * server to execute this Dataset. Exposed for plugin authors and tooling; ordinary code never
   * needs it.
   */
  def plan: proto.Plan = proto.Plan(proto.Plan.OpType.Root(relation))

  /** The id the server uses to refer to this Dataset's plan. */
  private[sql] def planId: Option[Long] = relation.common.flatMap(_.planId)

  /** Builds a new DataFrame whose relation has `this` as input. */
  private def withInput(relType: RelType, cols: Seq[Column] = Nil): DataFrame =
    sparkSession.newDataFrame(relType, cols)

  /** Builds a new Dataset of the same element type `T` from a relation built on this input. */
  private def withSameType(relType: RelType, cols: Seq[Column] = Nil): Dataset[T] =
    sparkSession.newDataset(relType, encoder, cols)

  private def checkSameSparkSession(other: Dataset[?]): Unit =
    if (sparkSession.sessionId != other.sparkSession.sessionId) {
      throw new SparkException(
        "[CONNECT.SESSION_NOT_SAME] Generic Spark Connect error. Both Datasets must belong to " +
          "the same SparkSession. SQLSTATE: 56K00",
        null,
        Some("CONNECT.SESSION_NOT_SAME"),
        Map.empty,
        Some("56K00")
      )
    }

  /**
   * Returns a new Dataset where each record is mapped to type `U` via its [[Encoder]]. This is a
   * purely client-side reinterpretation (no server-side closure), so it works over Spark Connect.
   */
  def as[U](using enc: Encoder[U]): Dataset[U] = new Dataset(sparkSession, relation, enc)

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
   * Selects a column by name, qualified by this Dataset's plan id so that it resolves unambiguously
   * even in self-joins.
   */
  def col(colName: String): Column =
    if (colName == "*") {
      Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedStar(proto.Expression.UnresolvedStar(planId = planId))
        )
      )
    } else if (colName.endsWith(".*")) {
      Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedStar(
            proto.Expression.UnresolvedStar(unparsedTarget = Some(colName), planId = planId)
          )
        )
      )
    } else {
      Column(
        proto.Expression(exprType =
          proto.Expression.ExprType.UnresolvedAttribute(
            proto.Expression.UnresolvedAttribute(unparsedIdentifier = colName, planId = planId)
          )
        )
      )
    }

  /** Selects a column by name. Alias for [[col]]. */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects a metadata column (such as `_metadata` of file sources) by name, qualified by this
   * Dataset's plan id.
   */
  def metadataColumn(colName: String): Column =
    Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedAttribute(
          proto.Expression.UnresolvedAttribute(
            unparsedIdentifier = colName,
            planId = planId,
            isMetadataColumn = Some(true)
          )
        )
      )
    )

  /** Selects columns based on a column name regular expression. */
  def colRegex(colName: String): Column =
    Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.UnresolvedRegex(
          proto.Expression.UnresolvedRegex(colName = colName, planId = planId)
        )
      )
    )

  // -- Subqueries ------------------------------------------------------------

  /**
   * Returns a [[Column]] for this single-column, single-row Dataset as a scalar subquery, e.g.
   * `df.where(col("v") > spark.table("t").select(max("v")).scalar())`.
   */
  def scalar(): Column = subquery(proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR)

  /** Returns a [[Column]] that is true if this Dataset has any rows (an EXISTS subquery). */
  def exists(): Column = subquery(proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS)

  private def subquery(subqueryType: proto.SubqueryExpression.SubqueryType): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.SubqueryExpression(
          proto.SubqueryExpression(planId = Column.planIdOf(relation), subqueryType = subqueryType)
        )
      ),
      Seq(relation)
    )

  // -- Projection / filtering ------------------------------------------------

  def select(cols: Column*): DataFrame =
    withInput(
      RelType.Project(proto.Project(input = Some(relation), expressions = cols.map(_.expr))),
      cols
    )

  def select(col: String, cols: String*): DataFrame =
    select((col +: cols).map(Column.fromName)*)

  /** Selects one typed column, returning a `Dataset` of its values. */
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] =
    selectTyped(Seq(c1), Encoder.columnValue(c1.encoder))

  /** Selects two typed columns, returning a `Dataset` of tuples. */
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    selectTyped(Seq(c1, c2), Encoder.tuple[(U1, U2)](Seq(c1.encoder, c2.encoder)))

  /** Selects three typed columns, returning a `Dataset` of tuples. */
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]
  ): Dataset[(U1, U2, U3)] =
    selectTyped(
      Seq(c1, c2, c3),
      Encoder.tuple[(U1, U2, U3)](Seq(c1.encoder, c2.encoder, c3.encoder))
    )

  /** Selects four typed columns, returning a `Dataset` of tuples. */
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]
  ): Dataset[(U1, U2, U3, U4)] =
    selectTyped(
      Seq(c1, c2, c3, c4),
      Encoder.tuple[(U1, U2, U3, U4)](Seq(c1.encoder, c2.encoder, c3.encoder, c4.encoder))
    )

  /** Selects five typed columns, returning a `Dataset` of tuples. */
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]
  ): Dataset[(U1, U2, U3, U4, U5)] =
    selectTyped(
      Seq(c1, c2, c3, c4, c5),
      Encoder.tuple[(U1, U2, U3, U4, U5)](
        Seq(c1.encoder, c2.encoder, c3.encoder, c4.encoder, c5.encoder)
      )
    )

  private def selectTyped[U](cols: Seq[Column], enc: Encoder[U]): Dataset[U] =
    sparkSession.newDataset(
      RelType.Project(proto.Project(input = Some(relation), expressions = cols.map(_.expr))),
      enc,
      cols
    )

  def selectExpr(exprs: String*): DataFrame =
    select(exprs.map(Dataset.exprColumn)*)

  def filter(condition: Column): Dataset[T] =
    withSameType(
      RelType.Filter(proto.Filter(input = Some(relation), condition = Some(condition.expr))),
      Seq(condition)
    )

  def filter(conditionExpr: String): Dataset[T] = filter(Dataset.exprColumn(conditionExpr))
  def where(condition: Column): Dataset[T] = filter(condition)
  def where(conditionExpr: String): Dataset[T] = filter(conditionExpr)

  def withColumn(colName: String, col: Column): DataFrame = withColumns(Seq(colName), Seq(col))

  /** Adds or replaces several columns at once. */
  def withColumns(colsMap: Map[String, Column]): DataFrame = {
    val (names, cols) = colsMap.toSeq.unzip
    withColumns(names, cols)
  }

  /** (Java-specific) Adds or replaces several columns at once. */
  def withColumns(colsMap: java.util.Map[String, Column]): DataFrame =
    withColumns(colsMap.asScala.toMap)

  private def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = {
    require(
      colNames.size == cols.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of columns: ${cols.size}"
    )
    withInput(
      RelType.WithColumns(
        proto.WithColumns(
          input = Some(relation),
          aliases = colNames.zip(cols).map { case (name, c) =>
            proto.Expression.Alias(expr = Some(c.expr), name = Seq(name))
          }
        )
      ),
      cols
    )
  }

  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    withColumnsRenamed(Seq(existingName), Seq(newName))

  def withColumnsRenamed(colsMap: Map[String, String]): DataFrame = {
    val (names, newNames) = colsMap.toSeq.unzip
    withColumnsRenamed(names, newNames)
  }

  /** (Java-specific) Renames several columns at once. */
  def withColumnsRenamed(colsMap: java.util.Map[String, String]): DataFrame =
    withColumnsRenamed(colsMap.asScala.toMap)

  private def withColumnsRenamed(colNames: Seq[String], newColNames: Seq[String]): DataFrame =
    withInput(
      RelType.WithColumnsRenamed(
        proto.WithColumnsRenamed(
          input = Some(relation),
          renames = colNames.zip(newColNames).map { case (k, v) =>
            proto.WithColumnsRenamed.Rename(colName = k, newColName = v)
          }
        )
      )
    )

  /** Returns a new Dataset where the metadata of column `columnName` is replaced by `metadata`. */
  def withMetadata(columnName: String, metadata: Metadata): DataFrame =
    withInput(
      RelType.WithColumns(
        proto.WithColumns(
          input = Some(relation),
          aliases = Seq(
            proto.Expression.Alias(
              expr = Some(col(columnName).expr),
              name = Seq(columnName),
              metadata = Some(metadata.json)
            )
          )
        )
      )
    )

  def drop(colName: String): DataFrame = drop(Seq(colName)*)

  def drop(colNames: String*): DataFrame =
    withInput(RelType.Drop(proto.Drop(input = Some(relation), columnNames = colNames)))

  def drop(col: Column): DataFrame = drop(col, Seq.empty*)

  def drop(col: Column, cols: Column*): DataFrame =
    withInput(
      RelType.Drop(proto.Drop(input = Some(relation), columns = (col +: cols).map(_.expr))),
      col +: cols
    )

  def toDF(): DataFrame = new Dataset(sparkSession, relation, Encoder.rowEncoder)
  def toDF(colNames: String*): DataFrame =
    withInput(RelType.ToDf(proto.ToDF(input = Some(relation), columnNames = colNames)))

  def as(alias: String): Dataset[T] =
    withSameType(
      RelType.SubqueryAlias(proto.SubqueryAlias(input = Some(relation), alias = alias))
    )
  def as(alias: Symbol): Dataset[T] = as(alias.name)
  def alias(alias: String): Dataset[T] = as(alias)
  def alias(alias: Symbol): Dataset[T] = as(alias)

  def hint(name: String, parameters: Any*): Dataset[T] = {
    val params = parameters.map(Column.lit)
    withSameType(
      RelType.Hint(
        proto.Hint(input = Some(relation), name = name, parameters = params.map(_.expr))
      ),
      params
    )
  }

  /**
   * Appends a column of consecutive 0-based `Long` indices named `index`, similar to
   * `RDD.zipWithIndex()`. The index column is the last column of the result.
   */
  def zipWithIndex(): DataFrame = zipWithIndex("index")

  /**
   * Appends a column of consecutive 0-based `Long` indices named `indexColName`, similar to
   * `RDD.zipWithIndex()`. If a column with that name already exists, the result has duplicate
   * column names.
   */
  def zipWithIndex(indexColName: String): DataFrame =
    select(col("*"), Column.internalFn("distributed_sequence_id").alias(indexColName))

  // -- Ordering & limiting ---------------------------------------------------

  def limit(n: Int): Dataset[T] =
    withSameType(RelType.Limit(proto.Limit(input = Some(relation), limit = n)))

  def offset(n: Int): Dataset[T] =
    withSameType(RelType.Offset(proto.Offset(input = Some(relation), offset = n)))

  def sort(sortCol: String, sortCols: String*): Dataset[T] =
    sort((sortCol +: sortCols).map(Column.fromName)*)

  def sort(sortExprs: Column*): Dataset[T] = buildSort(sortExprs, global = true)
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = sort(sortCol, sortCols*)
  def orderBy(sortExprs: Column*): Dataset[T] = sort(sortExprs*)
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = buildSort(sortExprs, global = false)
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] =
    buildSort((sortCol +: sortCols).map(Column.fromName), global = false)

  private def buildSort(sortExprs: Seq[Column], global: Boolean): Dataset[T] =
    withSameType(
      RelType.Sort(
        proto.Sort(
          input = Some(relation),
          order = sortExprs.map(Dataset.toSortOrder),
          isGlobal = Some(global)
        )
      ),
      sortExprs
    )

  // -- Distinct --------------------------------------------------------------

  def distinct(): Dataset[T] = dropDuplicates()

  def dropDuplicates(): Dataset[T] =
    withSameType(
      RelType.Deduplicate(proto.Deduplicate(input = Some(relation), allColumnsAsKeys = Some(true)))
    )
  def dropDuplicates(colNames: Seq[String]): Dataset[T] =
    withSameType(
      RelType.Deduplicate(proto.Deduplicate(input = Some(relation), columnNames = colNames))
    )
  def dropDuplicates(colNames: Array[String]): Dataset[T] = dropDuplicates(colNames.toSeq)
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = dropDuplicates(col1 +: cols)

  /** Drops duplicates within the event-time watermark, keeping state bounded for streaming. */
  def dropDuplicatesWithinWatermark(): Dataset[T] =
    withSameType(
      RelType.Deduplicate(
        proto.Deduplicate(
          input = Some(relation),
          allColumnsAsKeys = Some(true),
          withinWatermark = Some(true)
        )
      )
    )

  def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[T] =
    withSameType(
      RelType.Deduplicate(
        proto.Deduplicate(
          input = Some(relation),
          columnNames = colNames,
          withinWatermark = Some(true)
        )
      )
    )

  def dropDuplicatesWithinWatermark(colNames: Array[String]): Dataset[T] =
    dropDuplicatesWithinWatermark(colNames.toSeq)

  def dropDuplicatesWithinWatermark(col1: String, cols: String*): Dataset[T] =
    dropDuplicatesWithinWatermark(col1 +: cols)

  // -- Set operations --------------------------------------------------------

  def union(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_UNION, isAll = true)
  def unionAll(other: Dataset[T]): Dataset[T] = union(other)
  def unionByName(other: Dataset[T]): Dataset[T] = unionByName(other, allowMissingColumns = false)
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] =
    setOp(
      other,
      proto.SetOperation.SetOpType.SET_OP_TYPE_UNION,
      isAll = true,
      byName = true,
      allowMissingColumns = Some(allowMissingColumns)
    )

  def intersect(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = false)
  def intersectAll(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, isAll = true)
  def except(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = false)
  def exceptAll(other: Dataset[T]): Dataset[T] =
    setOp(other, proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, isAll = true)

  private def setOp(
      other: Dataset[T],
      opType: proto.SetOperation.SetOpType,
      isAll: Boolean,
      byName: Boolean = false,
      allowMissingColumns: Option[Boolean] = None
  ): Dataset[T] = {
    checkSameSparkSession(other)
    withSameType(
      RelType.SetOp(
        proto.SetOperation(
          leftInput = Some(relation),
          rightInput = Some(other.relation),
          setOpType = opType,
          isAll = Some(isAll),
          byName = if (byName) Some(true) else None,
          allowMissingColumns = allowMissingColumns
        )
      )
    )
  }

  // -- Joins -----------------------------------------------------------------

  def join(right: Dataset[?]): DataFrame = buildJoin(right, None, Nil, "inner")
  def join(right: Dataset[?], usingColumn: String): DataFrame = join(right, Seq(usingColumn))
  def join(right: Dataset[?], usingColumns: Array[String]): DataFrame =
    join(right, usingColumns.toSeq)
  def join(right: Dataset[?], usingColumns: Seq[String]): DataFrame =
    join(right, usingColumns, "inner")
  def join(right: Dataset[?], usingColumn: String, joinType: String): DataFrame =
    join(right, Seq(usingColumn), joinType)
  def join(right: Dataset[?], usingColumns: Array[String], joinType: String): DataFrame =
    join(right, usingColumns.toSeq, joinType)
  def join(right: Dataset[?], usingColumns: Seq[String], joinType: String): DataFrame =
    buildJoin(right, None, usingColumns, joinType)
  def join(right: Dataset[?], joinExprs: Column): DataFrame = join(right, joinExprs, "inner")
  def join(right: Dataset[?], joinExprs: Column, joinType: String): DataFrame =
    buildJoin(right, Some(joinExprs), Nil, joinType)
  def crossJoin(right: Dataset[?]): DataFrame = buildJoin(right, None, Nil, "cross")

  private def buildJoin(
      right: Dataset[?],
      joinExprs: Option[Column],
      usingColumns: Seq[String],
      joinType: String
  ): DataFrame = {
    checkSameSparkSession(right)
    withInput(
      RelType.Join(
        proto.Join(
          left = Some(relation),
          right = Some(right.relation),
          joinCondition = joinExprs.map(_.expr),
          joinType = Dataset.toJoinType(joinType),
          usingColumns = usingColumns
        )
      ),
      joinExprs.toSeq
    )
  }

  /**
   * Joins with `other` (an inner join), returning each matching pair of records as a tuple. Like
   * Apache Spark, a struct-typed (case class) side becomes a nested struct column.
   */
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] =
    joinWith(other, condition, "inner")

  /**
   * Joins with `other` using the given join type (`inner`, `cross`, `outer`/`full`, `left` or
   * `right` variants), returning each matching pair of records as a tuple.
   */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
    checkSameSparkSession(other)
    val joinTypeValue = Dataset.toJoinType(joinType, skipSemiAnti = true)
    val (leftNullable, rightNullable) = joinTypeValue match {
      case proto.Join.JoinType.JOIN_TYPE_INNER | proto.Join.JoinType.JOIN_TYPE_CROSS =>
        (false, false)
      case proto.Join.JoinType.JOIN_TYPE_FULL_OUTER => (true, true)
      case proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER => (false, true)
      case proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER => (true, false)
      case e => throw new IllegalArgumentException(s"Unsupported join type '$e'.")
    }
    val tupleEncoder =
      Encoder.tuple[(T, U)](Seq(encoder, other.encoder), Seq(leftNullable, rightNullable))
    sparkSession.newDataset(
      RelType.Join(
        proto.Join(
          left = Some(relation),
          right = Some(other.relation),
          joinCondition = Some(condition.expr),
          joinType = joinTypeValue,
          joinDataType = Some(
            proto.Join.JoinDataType(
              isLeftStruct = encoder.isStruct,
              isRightStruct = other.encoder.isStruct
            )
          )
        )
      ),
      tupleEncoder,
      Seq(condition)
    )
  }

  /** Lateral join with a correlated `right` relation. */
  def lateralJoin(right: Dataset[?]): DataFrame = buildLateralJoin(right, None, "inner")
  def lateralJoin(right: Dataset[?], joinExprs: Column): DataFrame =
    buildLateralJoin(right, Some(joinExprs), "inner")
  def lateralJoin(right: Dataset[?], joinType: String): DataFrame =
    buildLateralJoin(right, None, joinType)
  def lateralJoin(right: Dataset[?], joinExprs: Column, joinType: String): DataFrame =
    buildLateralJoin(right, Some(joinExprs), joinType)

  private def buildLateralJoin(
      right: Dataset[?],
      joinExprs: Option[Column],
      joinType: String
  ): DataFrame = {
    checkSameSparkSession(right)
    withInput(
      RelType.LateralJoin(
        proto.LateralJoin(
          left = Some(relation),
          right = Some(right.relation),
          joinCondition = joinExprs.map(_.expr),
          joinType = Dataset.toJoinType(joinType)
        )
      ),
      joinExprs.toSeq
    )
  }

  /**
   * Combines the columns of this DataFrame with `other` side by side, preserving row alignment.
   * Both must derive from the same source through projection-only operations (`select`,
   * `withColumn`, ...); otherwise the server raises an `AnalysisException`. Requires Spark 4.3+.
   */
  def zip(other: Dataset[?]): DataFrame = {
    checkSameSparkSession(other)
    withInput(RelType.Zip(proto.Zip(left = Some(relation), right = Some(other.relation))))
  }

  /**
   * Nearest-by top-K ranking join with the default `inner` join type: for each row of this Dataset,
   * returns up to `numResults` rows of `right` ranked by `rankingExpression`. `mode` is `approx` or
   * `exact`; `direction` is `distance` (smallest first) or `similarity` (largest first). Requires
   * Spark 4.2+.
   */
  def nearestByJoin(
      right: Dataset[?],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String
  ): DataFrame =
    nearestByJoin(right, rankingExpression, numResults, mode, direction, "inner")

  /**
   * Nearest-by top-K ranking join; `joinType` is `inner` or `leftouter`. The arguments are
   * validated client-side with the same `NEAREST_BY_JOIN.*` errors as the server. Requires Spark
   * 4.2+.
   */
  def nearestByJoin(
      right: Dataset[?],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String,
      joinType: String
  ): DataFrame = {
    checkSameSparkSession(right)
    Dataset.validateNearestByJoinArgs(numResults, joinType, mode, direction)
    withInput(
      RelType.NearestByJoin(
        proto.NearestByJoin(
          left = Some(relation),
          right = Some(right.relation),
          rankingExpression = Some(rankingExpression.expr),
          numResults = numResults,
          joinType = joinType,
          mode = mode,
          direction = direction
        )
      ),
      Seq(rankingExpression)
    )
  }

  // -- Repartitioning --------------------------------------------------------

  def repartition(numPartitions: Int): Dataset[T] =
    withSameType(
      RelType.Repartition(
        proto.Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(true)
        )
      )
    )

  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    repartitionByExpression(Some(numPartitions), partitionExprs.map(_.expr), partitionExprs)

  def repartition(partitionExprs: Column*): Dataset[T] =
    repartitionByExpression(None, partitionExprs.map(_.expr), partitionExprs)

  /** Range-partitions by the given expressions into `numPartitions`. */
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] =
    repartitionByExpression(Some(numPartitions), rangeExprs(partitionExprs), partitionExprs)

  /** Range-partitions by the given expressions. */
  def repartitionByRange(partitionExprs: Column*): Dataset[T] =
    repartitionByExpression(None, rangeExprs(partitionExprs), partitionExprs)

  private def rangeExprs(cols: Seq[Column]): Seq[proto.Expression] =
    cols.map(c =>
      proto.Expression(exprType = proto.Expression.ExprType.SortOrder(Dataset.toSortOrder(c)))
    )

  /**
   * Repartitions into `numPartitions` partitions, placing each row in the partition whose id is
   * given by `partitionIdExpr` (evaluated per row), without hashing.
   */
  def repartitionById(numPartitions: Int, partitionIdExpr: Column): Dataset[T] = {
    val direct = proto.Expression(exprType =
      proto.Expression.ExprType.DirectShufflePartitionId(
        proto.Expression.DirectShufflePartitionID(child = Some(partitionIdExpr.expr))
      )
    )
    repartitionByExpression(Some(numPartitions), Seq(direct), Seq(partitionIdExpr))
  }

  private def repartitionByExpression(
      numPartitions: Option[Int],
      partitionExprs: Seq[proto.Expression],
      cols: Seq[Column]
  ): Dataset[T] =
    withSameType(
      RelType.RepartitionByExpression(
        proto.RepartitionByExpression(
          input = Some(relation),
          partitionExprs = partitionExprs,
          numPartitions = numPartitions
        )
      ),
      cols
    )

  def coalesce(numPartitions: Int): Dataset[T] =
    withSameType(
      RelType.Repartition(
        proto.Repartition(
          input = Some(relation),
          numPartitions = numPartitions,
          shuffle = Some(false)
        )
      )
    )

  // -- Sampling --------------------------------------------------------------

  def sample(fraction: Double): Dataset[T] = sample(withReplacement = false, fraction)
  def sample(fraction: Double, seed: Long): Dataset[T] =
    sample(withReplacement = false, fraction, seed)
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    sample(withReplacement, fraction, scala.util.Random.nextLong())
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] =
    withSameType(
      RelType.Sample(
        proto.Sample(
          input = Some(relation),
          lowerBound = 0.0d,
          upperBound = fraction,
          withReplacement = Some(withReplacement),
          seed = Some(seed)
        )
      )
    )

  /** Randomly splits this Dataset with the given weights and a fixed seed. */
  def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    require(
      weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}"
    )
    val sum = weights.sum
    require(sum > 0, s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")
    val bounds = weights.scanLeft(0.0)((acc, w) => acc + w / sum)
    weights.indices.map { i =>
      withSameType(
        RelType.Sample(
          proto.Sample(
            input = Some(relation),
            lowerBound = bounds(i),
            upperBound = bounds(i + 1),
            withReplacement = Some(false),
            seed = Some(seed),
            // Sort each partition server-side so the splits are disjoint across executions.
            deterministicOrder = true
          )
        )
      )
    }.toArray
  }

  /** Randomly splits this Dataset with the given weights. */
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    randomSplit(weights, scala.util.Random.nextLong())

  /** (Java-specific) Randomly splits this Dataset with the given weights and seed. */
  def randomSplitAsList(weights: Array[Double], seed: Long): java.util.List[Dataset[T]] =
    java.util.Arrays.asList(randomSplit(weights, seed)*)

  // -- Grouping & aggregation ------------------------------------------------

  def groupBy(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset =
    groupBy((col1 +: cols).map(Column.fromName)*)

  def rollup(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP)
  def rollup(col1: String, cols: String*): RelationalGroupedDataset =
    rollup((col1 +: cols).map(Column.fromName)*)
  def cube(cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(this, cols, proto.Aggregate.GroupType.GROUP_TYPE_CUBE)
  def cube(col1: String, cols: String*): RelationalGroupedDataset =
    cube((col1 +: cols).map(Column.fromName)*)

  /**
   * Creates a multi-dimensional aggregation over the given grouping sets, e.g.
   * `ds.groupingSets(Seq(Seq($"dept", $"gender"), Seq()), $"dept", $"gender").agg(...)`.
   */
  def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      this,
      cols,
      proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS,
      groupingSets = groupingSets
    )

  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs*)
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    groupBy().agg(aggExpr, aggExprs*)
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  // -- Actions ---------------------------------------------------------------

  def collect(): Array[T] =
    sparkSession.execute(plan).toArray.map(encoder.fromRow)(using encoder.classTag)
  def collectAsList(): java.util.List[T] = java.util.Arrays.asList(collect()*)
  def toLocalIterator(): java.util.Iterator[T] =
    sparkSession.execute(plan).iterator.map(encoder.fromRow).asJava

  def count(): Long = {
    val agg = proto.Aggregate(
      input = Some(relation),
      groupType = proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY,
      aggregateExpressions = Seq(Column.fn("count", Column.lit(1)).expr)
    )
    val rel = sparkSession.newRelation(RelType.Aggregate(agg))
    val rows = sparkSession.execute(proto.Plan(proto.Plan.OpType.Root(rel))).toArray
    if (rows.isEmpty) 0L else rows.head.getLong(0)
  }

  def head(n: Int): Array[T] = limit(n).collect()
  def head(): T = head(1).head
  def first(): T = head()
  def take(n: Int): Array[T] = head(n)
  def takeAsList(n: Int): java.util.List[T] = java.util.Arrays.asList(take(n)*)

  /** Returns the last `n` rows. Like `collect()`, this moves the data into the client. */
  def tail(n: Int): Array[T] =
    withSameType(RelType.Tail(proto.Tail(input = Some(relation), limit = n))).collect()

  def isEmpty: Boolean =
    sparkSession
      .execute(
        proto.Plan(
          proto.Plan.OpType.Root(
            sparkSession.newRelation(RelType.Limit(proto.Limit(input = Some(relation), limit = 1)))
          )
        )
      )
      .toArray
      .isEmpty

  def show(): Unit = show(20)
  def show(numRows: Int): Unit = show(numRows, truncate = true)
  def show(truncate: Boolean): Unit = show(20, truncate)
  def show(numRows: Int, truncate: Boolean): Unit =
    show(numRows, if (truncate) 20 else 0, vertical = false)
  def show(numRows: Int, truncate: Int): Unit = show(numRows, truncate, vertical = false)
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = {
    // Render client-side from collected rows so that show() is deterministic and exactly
    // consistent with collect(). The server-side ShowString relation can return a
    // partially-rendered table for some plans (e.g. aggregations), so we do not rely on it.
    val n = math.max(numRows, 0)
    val result = sparkSession.execute(limit(n + 1).plan)
    print(
      org.apache.spark.sql.connect.client.ShowString(
        result.toArray.toSeq,
        result.schema,
        n,
        truncate,
        vertical
      )
    )
  }

  // -- Explain ---------------------------------------------------------------

  def explain(): Unit = explain("simple")
  def explain(extended: Boolean): Unit = explain(if (extended) "extended" else "simple")
  def explain(mode: String): Unit = {
    val explainMode = mode.trim.toLowerCase(java.util.Locale.ROOT) match {
      case "simple" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
      case "extended" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      case "codegen" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
      case "cost" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
      case "formatted" => proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
      case _ => throw new IllegalArgumentException("Unsupported explain mode: " + mode)
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

  /** Registers this Dataset as a temporary view. Use [[createOrReplaceTempView]] instead. */
  @deprecated("Use createOrReplaceTempView(viewName) instead.", "2.0.0")
  def registerTempTable(tableName: String): Unit = createOrReplaceTempView(tableName)

  private def createView(viewName: String, replace: Boolean, global: Boolean): Unit =
    sparkSession.executeCommand(
      proto.Command(
        proto.Command.CommandType.CreateDataframeView(
          proto.CreateDataFrameViewCommand(
            input = Some(relation),
            name = viewName,
            isGlobal = global,
            replace = replace
          )
        )
      )
    )

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

  /**
   * Defines named observed metrics computed while this Dataset is processed. The metrics are
   * reported with the query result; use the [[Observation]] overload to read them back.
   */
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] =
    withSameType(
      RelType.CollectMetrics(
        proto.CollectMetrics(
          input = Some(relation),
          name = name,
          metrics = (expr +: exprs).map(_.expr)
        )
      ),
      expr +: exprs
    )

  /** Defines observed metrics whose values can be read back through `observation.get`. */
  def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T] = {
    observation.markRegistered()
    val ds = observe(observation.name, expr, exprs*)
    sparkSession.registerObservation(ds.planId, observation)
    ds
  }

  /** Concisely applies a transformation to this Dataset. */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  // -- Misc ------------------------------------------------------------------

  def sameSemantics(other: Dataset[T]): Boolean =
    sparkSession.client.sameSemantics(plan, other.plan)
  def semanticHash(): Int = sparkSession.client.semanticHash(plan)

  // -- Additional relational transformations ---------------------------------

  /** Unpivots (melts) a DataFrame from wide to long format. */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    withInput(
      RelType.Unpivot(
        proto.Unpivot(
          input = Some(relation),
          ids = ids.toSeq.map(_.expr),
          values = Some(proto.Unpivot.Values(values = values.toSeq.map(_.expr))),
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      ),
      ids.toSeq ++ values.toSeq
    )

  /** Unpivots, inferring the value columns from those not in `ids`. */
  def unpivot(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame =
    withInput(
      RelType.Unpivot(
        proto.Unpivot(
          input = Some(relation),
          ids = ids.toSeq.map(_.expr),
          variableColumnName = variableColumnName,
          valueColumnName = valueColumnName
        )
      ),
      ids.toSeq
    )

  /** Alias for [[unpivot]]. */
  def melt(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    unpivot(ids, values, variableColumnName, valueColumnName)

  /** Alias for [[unpivot]]. */
  def melt(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame =
    unpivot(ids, variableColumnName, valueColumnName)

  /** Transposes the DataFrame, turning the first column into the new column names. */
  def transpose(): DataFrame =
    withInput(RelType.Transpose(proto.Transpose(input = Some(relation))))

  /** Transposes the DataFrame using `indexColumn` for the new column names. */
  def transpose(indexColumn: Column): DataFrame =
    withInput(
      RelType.Transpose(
        proto.Transpose(input = Some(relation), indexColumns = Seq(indexColumn.expr))
      ),
      Seq(indexColumn)
    )

  /** Defines an event-time watermark for this streaming Dataset. */
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] =
    withSameType(
      RelType.WithWatermark(
        proto.WithWatermark(
          input = Some(relation),
          eventTime = eventTime,
          delayThreshold = delayThreshold
        )
      )
    )

  /** Returns the content as a `Dataset[String]` of JSON documents in a `value` column. */
  def toJSON: Dataset[String] =
    select(functions.to_json(functions.struct(col("*"))).as("value")).as[String]

  /** Eagerly checkpoints this Dataset to reliable storage and returns the checkpointed copy. */
  def checkpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = true, None)

  /** Checkpoints this Dataset to reliable storage. */
  def checkpoint(eager: Boolean): Dataset[T] =
    checkpoint(eager, reliableCheckpoint = true, None)

  /** Eagerly locally checkpoints this Dataset. */
  def localCheckpoint(): Dataset[T] = checkpoint(eager = true, reliableCheckpoint = false, None)

  /** Locally checkpoints this Dataset. */
  def localCheckpoint(eager: Boolean): Dataset[T] =
    checkpoint(eager, reliableCheckpoint = false, None)

  /** Locally checkpoints this Dataset, caching it at the given storage level. */
  def localCheckpoint(eager: Boolean, storageLevel: StorageLevel): Dataset[T] =
    checkpoint(eager, reliableCheckpoint = false, Some(storageLevel))

  private def checkpoint(
      eager: Boolean,
      reliableCheckpoint: Boolean,
      storageLevel: Option[StorageLevel]
  ): Dataset[T] = {
    val command = proto.Command(commandType =
      proto.Command.CommandType.CheckpointCommand(
        proto.CheckpointCommand(
          relation = Some(relation),
          local = !reliableCheckpoint,
          eager = eager,
          storageLevel = storageLevel.map(_.toProto)
        )
      )
    )
    val responses =
      sparkSession.client.execute(proto.Plan(proto.Plan.OpType.Command(command))).toList
    val cached = responses
      .find(_.responseType.isCheckpointCommandResult)
      .map(_.getCheckpointCommandResult.getRelation)
      .getOrElse(throw new RuntimeException("CheckpointCommandResult missing from server response"))
    withSameType(RelType.CachedRemoteRelation(cached))
  }

  // -- Persistence -----------------------------------------------------------

  /** Persists this Dataset with the default storage level (`MEMORY_AND_DISK`). */
  def persist(): this.type = persist(StorageLevel.MEMORY_AND_DISK)

  /** Persists this Dataset with the given storage level. */
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.client.analyze(
      proto.AnalyzePlanRequest.Analyze.Persist(
        proto.AnalyzePlanRequest
          .Persist(relation = Some(relation), storageLevel = Some(newLevel.toProto))
      )
    )
    this
  }

  /** Persists this Dataset with the default storage level. */
  def cache(): this.type = persist()

  /** Marks this Dataset as non-persistent. */
  def unpersist(blocking: Boolean): this.type = {
    sparkSession.client.analyze(
      proto.AnalyzePlanRequest.Analyze.Unpersist(
        proto.AnalyzePlanRequest.Unpersist(relation = Some(relation), blocking = Some(blocking))
      )
    )
    this
  }

  /** Marks this Dataset as non-persistent. */
  def unpersist(): this.type = unpersist(blocking = false)

  /** Returns the current storage level of this Dataset. */
  def storageLevel: StorageLevel =
    StorageLevel.fromProto(
      sparkSession.client
        .analyze(
          proto.AnalyzePlanRequest.Analyze
            .GetStorageLevel(proto.AnalyzePlanRequest.GetStorageLevel(relation = Some(relation)))
        )
        .getGetStorageLevel
        .getStorageLevel
    )

  // -- v2 write --------------------------------------------------------------

  /** Creates a v2 (catalog) write configuration builder. */
  def writeTo(table: String): DataFrameWriterV2 = new DataFrameWriterV2(table, this)

  /**
   * Merges this Dataset (the source) into the `table` (the target) using `condition` to match rows.
   * Returns a [[MergeIntoWriter]] to configure the WHEN clauses; call `merge()` to run it.
   */
  def mergeInto(table: String, condition: Column): MergeIntoWriter[T] =
    new MergeIntoWriter[T](table, this, condition)

  // -- Schema reconciliation -------------------------------------------------

  /**
   * Returns a new Dataset where each row is reconciled to match the specified schema (by column
   * name, reordering and casting as needed).
   */
  def to(schema: StructType): DataFrame =
    withInput(
      RelType.ToSchema(
        proto.ToSchema(
          input = Some(relation),
          schema = Some(DataTypeProtoConverter.toConnectProtoType(schema))
        )
      )
    )
}

private[sql] object Dataset {

  def exprColumn(e: String): Column =
    new Column(
      proto.Expression(exprType =
        proto.Expression.ExprType.ExpressionString(
          proto.Expression.ExpressionString(expression = e)
        )
      )
    )

  def toSortOrder(col: Column): proto.Expression.SortOrder =
    col.expr.exprType match {
      case proto.Expression.ExprType.SortOrder(order) => order
      case _ =>
        proto.Expression.SortOrder(
          child = Some(col.expr),
          direction = proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        )
    }

  def toJoinType(joinType: String, skipSemiAnti: Boolean = false): proto.Join.JoinType =
    joinType.toLowerCase(java.util.Locale.ROOT).replaceAll("[_ ]", "") match {
      case "inner" => proto.Join.JoinType.JOIN_TYPE_INNER
      case "cross" => proto.Join.JoinType.JOIN_TYPE_CROSS
      case "outer" | "full" | "fullouter" => proto.Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "left" | "leftouter" => proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter" => proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "leftsemi" | "semi" if !skipSemiAnti => proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "leftanti" | "anti" if !skipSemiAnti => proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case _ => throw new IllegalArgumentException(s"Unsupported join type '$joinType'.")
    }

  // The acceptance lists mirror `NearestByJoinValidation` in Apache Spark.
  private val NearestByMaxNumResults = 100000
  private val NearestBySupportedJoinTypes = Seq("inner", "leftouter", "left")
  private val NearestBySupportedModes = Seq("approx", "exact")
  private val NearestBySupportedDirections = Seq("distance", "similarity")

  /**
   * Validates `nearestByJoin` arguments client-side, raising the same `NEAREST_BY_JOIN.*` errors as
   * the server so users see consistent failures without a round trip.
   */
  private[sql] def validateNearestByJoinArgs(
      numResults: Int,
      joinType: String,
      mode: String,
      direction: String
  ): Unit = {
    if (numResults < 1 || numResults > NearestByMaxNumResults) {
      throw AnalysisException.withErrorClass(
        "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
        Map(
          "numResults" -> numResults.toString,
          "min" -> "1",
          "max" -> NearestByMaxNumResults.toString
        )
      )
    }
    if (
      !NearestBySupportedJoinTypes.contains(
        joinType.toLowerCase(java.util.Locale.ROOT).replace("_", "")
      )
    ) {
      throw AnalysisException.withErrorClass(
        "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE",
        Map("joinType" -> joinType, "supported" -> "'INNER', 'LEFT OUTER'")
      )
    }
    if (!NearestBySupportedModes.contains(mode.toLowerCase(java.util.Locale.ROOT))) {
      throw AnalysisException.withErrorClass(
        "NEAREST_BY_JOIN.UNSUPPORTED_MODE",
        Map("mode" -> mode, "supported" -> NearestBySupportedModes.mkString("'", "', '", "'"))
      )
    }
    if (!NearestBySupportedDirections.contains(direction.toLowerCase(java.util.Locale.ROOT))) {
      throw AnalysisException.withErrorClass(
        "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION",
        Map(
          "direction" -> direction,
          "supported" -> NearestBySupportedDirections.mkString("'", "', '", "'")
        )
      )
    }
  }
}
