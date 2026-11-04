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

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

/** A case class for typed-Dataset plan tests. */
case class ParityPoint(x: Long, y: String)

/**
 * Offline (server-free) plan tests for the Spark 4.2 / 4.3 parity APIs: each test builds a plan and
 * asserts on the protobuf the Apache Spark Connect client would send, or on client-side validation.
 */
class ParityPlanSuite extends munit.FunSuite {

  private def withSession(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    spark.releaseSessionOnClose = false
    try body(spark)
    finally spark.stop()
  }

  private def fn(c: Column): proto.Expression.UnresolvedFunction = c.expr.getUnresolvedFunction

  // -- Dataset ---------------------------------------------------------------

  test("zip builds a Zip relation over both inputs") {
    withSession { spark =>
      val base = spark.range(3)
      val df = base.select(col("id")).zip(base.select((col("id") * 2).as("d")))
      assert(df.relation.relType.isZip)
      assert(df.relation.getZip.left.get.relType.isProject)
      assert(df.relation.getZip.right.get.relType.isProject)
    }
  }

  test("zipWithIndex appends the internal distributed_sequence_id column") {
    withSession { spark =>
      val df = spark.range(3).zipWithIndex("idx")
      val exprs = df.relation.getProject.expressions
      assertEquals(exprs.size, 2)
      assert(exprs.head.exprType.isUnresolvedStar)
      val alias = exprs(1).getAlias
      assertEquals(alias.name, Seq("idx"))
      val f = alias.getExpr.getUnresolvedFunction
      assertEquals(f.functionName, "distributed_sequence_id")
      assertEquals(f.isInternal, Some(true))
      assertEquals(
        spark.range(1).zipWithIndex().relation.getProject.expressions(1).getAlias.name,
        Seq("index")
      )
    }
  }

  test("nearestByJoin builds a NearestByJoin relation") {
    withSession { spark =>
      val l = spark.range(3)
      val r = spark.range(5)
      val df = l.nearestByJoin(r, abs(l("id") - r("id")), 2, "exact", "distance", "leftouter")
      val j = df.relation.getNearestByJoin
      assertEquals(j.numResults, 2)
      assertEquals(j.mode, "exact")
      assertEquals(j.direction, "distance")
      assertEquals(j.joinType, "leftouter")
      assert(j.rankingExpression.isDefined)
      assertEquals(
        l.nearestByJoin(r, l("id"), 1, "approx", "similarity").relation.getNearestByJoin.joinType,
        "inner"
      )
    }
  }

  test("nearestByJoin validates its arguments client-side") {
    withSession { spark =>
      val l = spark.range(3)
      val r = spark.range(5)
      def errorClass(f: => Any): String = intercept[AnalysisException](f).getErrorClass
      assertEquals(
        errorClass(l.nearestByJoin(r, l("id"), 0, "exact", "distance")),
        "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE"
      )
      assertEquals(
        errorClass(l.nearestByJoin(r, l("id"), 100001, "exact", "distance")),
        "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE"
      )
      assertEquals(
        errorClass(l.nearestByJoin(r, l("id"), 1, "exact", "distance", "full")),
        "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE"
      )
      assertEquals(
        errorClass(l.nearestByJoin(r, l("id"), 1, "fuzzy", "distance")),
        "NEAREST_BY_JOIN.UNSUPPORTED_MODE"
      )
      val e = intercept[AnalysisException](l.nearestByJoin(r, l("id"), 1, "exact", "up"))
      assertEquals(e.getErrorClass, "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION")
      assertEquals(e.getSqlState, "42604")
      assertEquals(e.getMessageParameters.get("direction"), "up")
      assert(e.getMessage.startsWith("[NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION]"), e.getMessage)
    }
  }

  test("scalar / exists / isin(Dataset) subqueries are shipped in WithRelations") {
    withSession { spark =>
      val t = spark.range(10)
      val sub = spark.range(3).select(max(col("id")))
      val df = t.where(col("id") > sub.scalar())
      assert(df.relation.relType.isWithRelations, df.relation.relType.getClass.getName)
      val wr = df.relation.getWithRelations
      assert(wr.root.get.relType.isFilter)
      assertEquals(wr.references.size, 1)
      val subPlanId = sub.relation.common.get.planId.get
      assertEquals(wr.references.head.common.get.planId, Some(subPlanId))
      val cond = wr.root.get.getFilter.condition.get.getUnresolvedFunction.arguments(1)
      assertEquals(cond.getSubqueryExpression.planId, subPlanId)
      assertEquals(
        cond.getSubqueryExpression.subqueryType,
        proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR
      )

      val ex = t.select(sub.exists().as("e"))
      assert(ex.relation.relType.isWithRelations)

      val in = t.where(col("id").isin(spark.range(2)))
      val inExpr = in.relation.getWithRelations.root.get.getFilter.condition.get
      assertEquals(
        inExpr.getSubqueryExpression.subqueryType,
        proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN
      )
      assertEquals(inExpr.getSubqueryExpression.inSubqueryValues.size, 1)
      // Columns without subqueries keep the plain relation.
      assert(t.where(col("id") > 1).relation.relType.isFilter)
    }
  }

  test("typed transformations keep the element type") {
    withSession { spark =>
      import spark.implicits.*
      val ds: Dataset[ParityPoint] = spark.createDataset(Seq(ParityPoint(1L, "a")))
      val filtered: Dataset[ParityPoint] = ds.filter(col("x") > 0).limit(1).orderBy("x")
      val union: Dataset[ParityPoint] = filtered.union(ds).distinct().as("p")
      assert(union.relation.relType.isSubqueryAlias)
      val range: Dataset[java.lang.Long] = spark.range(5).where(col("id") > 1)
      assert(range.relation.relType.isFilter)
      val splits: Array[Dataset[java.lang.Long]] = spark.range(10).randomSplit(Array(0.5, 0.5), 7L)
      assertEquals(splits.length, 2)
      assertEquals(spark.range(10).randomSplitAsList(Array(1.0, 2.0), 7L).size(), 2)
    }
  }

  test("typed select and joinWith carry encoders and join data types") {
    withSession { spark =>
      val ds = spark.range(3)
      val one: Dataset[Long] = ds.select(col("id").as[Long])
      assert(one.relation.relType.isProject)
      val two: Dataset[(Long, String)] = ds.select(col("id").as[Long], lit("a").as[String])
      assertEquals(two.encoder.dataType.asInstanceOf[StructType].fieldNames.toSeq, Seq("_1", "_2"))
      val points = spark.createDataset(Seq(ParityPoint(1L, "a")))
      val joined: Dataset[(ParityPoint, java.lang.Long)] =
        points.joinWith(ds, points("x") === ds("id"), "left")
      val jdt = joined.relation.getJoin.joinDataType.get
      assert(jdt.isLeftStruct)
      assert(!jdt.isRightStruct)
      assertEquals(joined.relation.getJoin.joinType, proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER)
      intercept[IllegalArgumentException](points.joinWith(ds, lit(true), "left_semi"))
    }
  }

  test("groupingSets, metadataColumn, repartitionById, withMetadata, observe(name)") {
    withSession { spark =>
      val df = spark.range(3).toDF()
      val gs = df
        .groupingSets(Seq(Seq(col("id")), Seq()), col("id"))
        .agg(count(lit(1)))
        .relation
        .getAggregate
      assertEquals(gs.groupType, proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS)
      assertEquals(gs.groupingSets.map(_.groupingSet.size), Seq(1, 0))

      val meta = df.metadataColumn("_metadata").expr.getUnresolvedAttribute
      assertEquals(meta.isMetadataColumn, Some(true))
      assert(meta.planId.isDefined)

      val byId = df.repartitionById(4, col("id") % 4).relation.getRepartitionByExpression
      assertEquals(byId.numPartitions, Some(4))
      assert(byId.partitionExprs.head.exprType.isDirectShufflePartitionId)

      val md = new MetadataBuilder().putString("comment", "the id").build()
      val alias = df.withMetadata("id", md).relation.getWithColumns.aliases.head
      assertEquals(alias.name, Seq("id"))
      assert(alias.metadata.exists(_.contains("the id")))

      val cm = df.observe("m", count(lit(1)).as("n")).relation.getCollectMetrics
      assertEquals(cm.name, "m")
      assertEquals(cm.metrics.size, 1)

      val dropped = df.drop(col("id"), col("id")).relation.getDrop
      assertEquals(dropped.columns.size, 2)
    }
  }

  test("toJSON returns a Dataset[String] over a value column") {
    withSession { spark =>
      val json: Dataset[String] = spark.range(2).toJSON
      assertEquals(json.relation.getProject.expressions.head.getAlias.name, Seq("value"))
    }
  }

  test("checkSameSparkSession rejects Datasets from another session") {
    withSession { a =>
      withSession { b =>
        val e = intercept[SparkException](a.range(1).zip(b.range(1)))
        assertEquals(e.getErrorClass, "CONNECT.SESSION_NOT_SAME")
      }
    }
  }

  // -- Column ----------------------------------------------------------------

  test("withField / dropFields build UpdateFields chains") {
    val c = col("s").withField("a", lit(1)).dropFields("b", "c")
    val outer = c.expr.getUpdateFields
    assertEquals(outer.fieldName, "c")
    assert(outer.valueExpression.isEmpty)
    val middle = outer.getStructExpression.getUpdateFields
    assertEquals(middle.fieldName, "b")
    val inner = middle.getStructExpression.getUpdateFields
    assertEquals(inner.fieldName, "a")
    assert(inner.valueExpression.isDefined)
  }

  test("try_cast uses the TRY eval mode; cast does not") {
    val t = col("a").try_cast(IntegerType).expr.getCast
    assertEquals(t.evalMode, proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
    val s = col("a").try_cast("int").expr.getCast
    assertEquals(s.getTypeStr, "int")
    assertEquals(s.evalMode, proto.Expression.Cast.EvalMode.EVAL_MODE_TRY)
    assertEquals(
      col("a").cast(IntegerType).expr.getCast.evalMode,
      proto.Expression.Cast.EvalMode.EVAL_MODE_UNSPECIFIED
    )
  }

  test("Column helpers mirror Apache Spark") {
    assertEquals(fn(col("a").isInCollection(Seq(1, 2))).functionName, "in")
    assertEquals(fn(col("a").isInCollection(Seq(1, 2))).arguments.size, 3)
    assertEquals(fn(col("a").substr(lit(1), lit(2))).arguments.size, 3)
    assertEquals(fn(col("a").startsWith("x")).functionName, "startswith")
    assertEquals(fn(col("a").endsWith(col("b"))).functionName, "endswith")
    assertEquals(fn(col("a").equalTo(1)).functionName, "=")
    assertEquals(fn(col("a").gt(1)).functionName, ">")
    assertEquals(fn(col("a").plus(1)).functionName, "+")
    assertEquals(fn(col("a").mod(2)).functionName, "%")
    assertEquals(fn(col("a") && true).functionName, "and")
    assertEquals(col("a").outer(), col("a"))
    assertEquals(col("a").transform(_ + 1), col("a") + 1)
    val md = new MetadataBuilder().putLong("k", 1L).build()
    assert(col("a").as("b", md).expr.getAlias.metadata.isDefined)
    assertEquals(col("a").as(Array("x", "y")).expr.getAlias.name, Seq("x", "y"))
    assertEquals(new Column("a.*").expr.getUnresolvedStar.unparsedTarget, Some("a.*"))
    assertEquals(Column("a").toString, "a")
    assertEquals((col("a") + 1).as("b").toString, "(a + 1) AS b")
  }

  test("when/otherwise reject misuse like Apache Spark") {
    intercept[IllegalArgumentException](col("a").when(lit(true), 1))
    val done = when(col("a") > 1, 1).otherwise(0)
    intercept[IllegalArgumentException](done.when(lit(true), 2))
    intercept[IllegalArgumentException](done.otherwise(3))
  }

  test("ColumnName builds struct fields") {
    val spark = SparkSession.builder.remote("sc://localhost:15002").create()
    spark.releaseSessionOnClose = false
    try {
      import spark.implicits.*
      assertEquals($"age".int, StructField("age", IntegerType))
      assertEquals($"d".decimal(5, 2), StructField("d", DecimalType(5, 2)))
      assertEquals($"m".map(StringType, LongType), StructField("m", MapType(StringType, LongType)))
    } finally spark.stop()
  }

  // -- functions -------------------------------------------------------------

  test("typedLit keeps parameterized types") {
    val arr = typedLit(Seq.empty[Int]).expr.getLiteral
    assert(arr.literalType.isArray)
    assertEquals(
      arr.dataType.map(org.apache.spark.sql.connect.common.DataTypeProtoConverter.toCatalystType),
      Some(ArrayType(IntegerType, containsNull = false))
    )
    val point = typedLit(ParityPoint(1L, "a")).expr.getLiteral
    assert(point.literalType.isStruct)
    assertEquals(point.getStruct.elements.size, 2)
    val none = typedlit(Option.empty[Int]).expr.getLiteral
    assert(none.literalType.isNull)
    val map = typedLit(Map("a" -> 1L)).expr.getLiteral
    assertEquals(map.getMap.keys.size, 1)
  }

  test("nondeterministic functions fix a seed client-side") {
    Seq(rand(), randn(), uuid(), shuffle(col("a")), random(), randstr(lit(3)))
      .foreach { c =>
        assert(
          fn(c).arguments.exists(_.getLiteral.literalType.isLong),
          s"${fn(c).functionName} has no seed"
        )
      }
  }

  test("functions build the same expressions as the Apache Spark Scala client") {
    assertEquals(fn(call_function("my_fn", col("a"))).isUserDefinedFunction, true)
    assertEquals(fn(product(col("a"))).isInternal, Some(true))
    assertEquals(fn(bitwise_not(col("a"))).functionName, "~")
    assertEquals(fn(isnan(col("a"))).functionName, "isNaN")
    assertEquals(fn(first(col("a"))).arguments.size, 2)
    assertEquals(fn(lag(col("a"), 1)).arguments.size, 4)
    assertEquals(fn(round(col("a"))).arguments(1).getLiteral.getInteger, 0)
    assertEquals(fn(window(col("t"), "1 minute")).arguments.size, 4)
    assertEquals(
      fn(unix_timestamp()).arguments.head.getUnresolvedFunction.functionName,
      "current_timestamp"
    )
    assertEquals(fn(partitioning.years(col("t"))).isInternal, Some(true))
    assertEquals(fn(instr(col("s"), lit("a"), 2, 1)).arguments.size, 4)
    assertEquals(fn(max_by(col("a"), col("b"), 3)).arguments.size, 3)
    assertEquals(fn(regexp_replace(col("s"), "a", "b", 2)).arguments.size, 4)
    assertEquals(
      fn(vector_cosine_similarity(col("a"), col("b"))).functionName,
      "vector_cosine_similarity"
    )
    assertEquals(fn(time_bucket(col("w"), col("t"))).functionName, "time_bucket")
    assertEquals(fn(tuple_sketch_agg_double(col("k"), col("v"), 12, "sum")).arguments.size, 4)
    assertEquals(
      fn(from_json(col("j"), StructType(Seq(StructField("A b", LongType)))))
        .arguments(1)
        .getLiteral
        .getString,
      "STRUCT<`A b`: BIGINT>"
    )
  }

  // -- Readers / writers ------------------------------------------------------

  test("read.changes / xml(Dataset) / textFile / jdbc(predicates)") {
    withSession { spark =>
      val ch = spark.read.option("startingVersion", "1").changes("t").relation.getRelationChanges
      assertEquals(ch.unparsedIdentifier, "t")
      assertEquals(ch.options, Map("startingVersion" -> "1"))
      assert(!ch.isStreaming)

      val lines = spark.createDataset(Seq("<a>1</a>"))
      // A DDL-string schema is resolved by the server (DDLParse); see ParityIntegrationSuite.
      val typedSchema = StructType(Seq(StructField("a", IntegerType)))
      val xml = spark.read.schema(typedSchema).xml(lines).relation.getParse
      assertEquals(xml.format, proto.Parse.ParseFormat.PARSE_FORMAT_XML)
      assert(xml.getSchema.kind.isStruct)
      val json = spark.read.json(lines).relation.getParse
      assertEquals(json.format, proto.Parse.ParseFormat.PARSE_FORMAT_JSON)
      assert(json.schema.isEmpty)

      val text: Dataset[String] = spark.read.textFile("/tmp/x")
      assert(text.relation.relType.isProject)

      val jdbc = spark.read
        .jdbc("jdbc:x", "t", Array("a < 1", "a >= 1"), new java.util.Properties())
        .relation
        .getRead
        .getDataSource
      assertEquals(jdbc.predicates, Seq("a < 1", "a >= 1"))
      assertEquals(jdbc.options("dbtable"), "t")

      val e = intercept[AnalysisException](spark.read.schema("a INT").changes("t"))
      assertEquals(e.getErrorClass, "_LEGACY_ERROR_TEMP_1189")
      intercept[AnalysisException](spark.read.schema("a INT").table("t"))
    }
  }

  test("writers carry schema evolution and clustering; invalid combinations fail") {
    withSession { spark =>
      val df = spark.range(1).toDF()
      val op = df.write.withSchemaEvolution().clusterBy("id").buildWriteOperation()
      assert(op.withSchemaEvolution)
      assertEquals(op.clusteringColumns, Seq("id"))
      val e = intercept[AnalysisException](df.write.partitionBy("id").clusterBy("id"))
      assertEquals(e.getErrorClass, "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED")
      intercept[AnalysisException](df.write.bucketBy(2, "id").clusterBy("id"))

      val v2 = df
        .writeTo("t")
        .withSchemaEvolution()
        .option("a", true)
        .option("b", 1L)
        .buildWriteOperation(proto.WriteOperationV2.Mode.MODE_APPEND)
      assert(v2.withSchemaEvolution)
      assertEquals(v2.options, Map("a" -> "true", "b" -> "1"))
    }
  }

  // -- Streaming -------------------------------------------------------------

  test("readStream.name / changes and triggers") {
    withSession { spark =>
      val ds = spark.readStream.format("rate").name("my_source").load()
      assertEquals(ds.relation.getRead.getDataSource.sourceName, Some("my_source"))
      val e = intercept[AnalysisException](spark.readStream.name("bad-name"))
      assertEquals(e.getErrorClass, "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME")
      intercept[IllegalArgumentException](spark.readStream.name(""))
      val ch = spark.readStream.changes("t").relation.getRelationChanges
      assert(ch.isStreaming)
    }
    assertEquals(Trigger.RealTime("10 seconds").toProto.realTimeBatchDuration, Some("10 seconds"))
    assertEquals(Trigger.RealTime(1500L).toProto.realTimeBatchDuration, Some("1500 milliseconds"))
    assertEquals(Trigger.RealTime().toProto.realTimeBatchDuration, Some("300000 milliseconds"))
    intercept[IllegalArgumentException](Trigger.RealTime(0L))
    assertEquals(
      Trigger
        .ProcessingTime(2L, java.util.concurrent.TimeUnit.SECONDS)
        .toProto
        .processingTimeInterval,
      Some("2000 milliseconds")
    )
    assertEquals(OutputMode.Append().toString, "Append")
  }

  // -- Session ---------------------------------------------------------------

  test("tags, empty datasets, tvf, and builder()") {
    withSession { spark =>
      spark.addTag("t1")
      assertEquals(spark.getTags(), Set("t1"))
      intercept[IllegalArgumentException](spark.addTag("a,b"))
      intercept[IllegalArgumentException](spark.addTag(""))
      spark.removeTag("t1")
      assertEquals(spark.getTags(), Set.empty[String])

      val empty: Dataset[ParityPoint] = spark.emptyDataset[ParityPoint]
      assertEquals(empty.relation.getLocalRelation.schema, Some("x BIGINT NOT NULL,y STRING"))

      val tvf = spark.tvf.explode(array(lit(1))).relation.getUnresolvedTableValuedFunction
      assertEquals(tvf.functionName, "explode")
      assertEquals(spark.tvf.range(3).relation.getRange.end, 3L)
      assertEquals(
        spark.catalog.listViews("db", "v*").relation.getCatalog.getListViews.pattern,
        Some("v*")
      )
      assertEquals(
        spark.catalog.listPartitions("t").relation.getCatalog.getListPartitions.tableName,
        "t"
      )
    }
    val b = SparkSession.builder()
    assert(b.isInstanceOf[SparkSession.Builder])
  }

  // -- Types & literals -------------------------------------------------------

  test("TIME, nanosecond timestamp, and geospatial types") {
    import org.apache.spark.sql.connect.common.DataTypeProtoConverter.*
    Seq(
      TimeType(3),
      TimestampNTZNanosType(9),
      TimestampLTZNanosType(7),
      GeometryType(4326),
      GeographyType(-1)
    ).foreach(t => assertEquals(toCatalystType(toConnectProtoType(t)), t))
    assertEquals(TimeType().typeName, "time(6)")
    assertEquals(TimestampNTZNanosType().sql, "TIMESTAMP_NTZ(9)")
    assertEquals(GeographyType(-1).typeName, "geography(any)")
    val e = intercept[SparkException](TimestampLTZNanosType(6))
    assertEquals(e.getErrorClass, "INVALID_TIMESTAMP_PRECISION")
    assertEquals(
      intercept[SparkException](TimeType(10)).getErrorClass,
      "UNSUPPORTED_TIME_PRECISION"
    )
  }

  test("struct SQL / DDL rendering quotes names and keeps case") {
    val s = new StructType()
      .add("Id", LongType, nullable = false)
      .add("a b", ArrayType(StringType))
      .add(StructField("m", MapType(StringType, DecimalType(5, 2))).withComment("it's"))
    assertEquals(
      s.sql,
      "STRUCT<Id: BIGINT NOT NULL, `a b`: ARRAY<STRING>, m: MAP<STRING, DECIMAL(5,2)> " +
        "COMMENT 'it\\'s'>"
    )
    assertEquals(
      s.toDDL,
      "Id BIGINT NOT NULL,`a b` ARRAY<STRING>,m MAP<STRING, DECIMAL(5,2)> " +
        "COMMENT 'it\\'s'"
    )
    assertEquals(NullType.typeName, "void")
  }

  test("literals for java.time types and nulls") {
    val t = Column.lit(java.time.LocalTime.of(1, 2, 3)).expr.getLiteral.getTime
    assertEquals(t.nano, java.time.LocalTime.of(1, 2, 3).toNanoOfDay)
    assertEquals(t.precision, Some(6))
    assertEquals(
      Column.lit(java.time.Duration.ofSeconds(2)).expr.getLiteral.getDayTimeInterval,
      2000000L
    )
    assertEquals(Column.lit(java.time.Period.ofMonths(14)).expr.getLiteral.getYearMonthInterval, 14)
    val n = Column.lit(null).expr.getLiteral
    assert(n.literalType.isNull)
    assert(n.dataType.exists(_.kind.isNull))
  }
}
