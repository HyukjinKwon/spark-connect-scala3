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

/**
 * Table-valued functions, available as `spark.tvf`: each returns a [[DataFrame]] built from a
 * server-side table-valued function. Mirrors `org.apache.spark.sql.TableValuedFunction`.
 *
 * {{{
 *   spark.tvf.explode(array(lit(1), lit(2))).show()
 *   spark.tvf.range(5)
 * }}}
 */
class TableValuedFunction private[sql] (sparkSession: SparkSession) {

  /** A `Dataset` with a single `id` column of `[0, end)`. */
  def range(end: Long): Dataset[java.lang.Long] = sparkSession.range(end)

  /** A `Dataset` with a single `id` column of `[start, end)`. */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = sparkSession.range(start, end)

  /** A `Dataset` with a single `id` column of `[start, end)` stepping by `step`. */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] =
    sparkSession.range(start, end, step)

  /** A `Dataset` with a single `id` column of `[start, end)` in `numPartitions` partitions. */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] =
    sparkSession.range(start, end, step, numPartitions)

  private def fn(name: String, args: Seq[Column]): DataFrame =
    sparkSession.newDataFrame(
      proto.Relation.RelType.UnresolvedTableValuedFunction(
        proto.UnresolvedTableValuedFunction(functionName = name, arguments = args.map(_.expr))
      ),
      args
    )

  /** A row per element of the given array or map. */
  def explode(collection: Column): DataFrame = fn("explode", Seq(collection))

  /** Like [[explode]], but produces a row of nulls for a null or empty collection. */
  def explode_outer(collection: Column): DataFrame = fn("explode_outer", Seq(collection))

  /** A row per struct of the given array of structs. */
  def inline(input: Column): DataFrame = fn("inline", Seq(input))

  /** Like [[inline]], but produces a row of nulls for a null or empty array. */
  def inline_outer(input: Column): DataFrame = fn("inline_outer", Seq(input))

  /** A row of the given JSON fields extracted from the JSON string `input`. */
  def json_tuple(input: Column, fields: Column*): DataFrame = fn("json_tuple", input +: fields)

  /** A row per element of the given array or map, with its position. */
  def posexplode(collection: Column): DataFrame = fn("posexplode", Seq(collection))

  /** Like [[posexplode]], but produces a row of nulls for a null or empty collection. */
  def posexplode_outer(collection: Column): DataFrame =
    fn("posexplode_outer", Seq(collection))

  /** Separates `fields` into `n` rows. */
  def stack(n: Column, fields: Column*): DataFrame = fn("stack", n +: fields)

  /** The collations supported by the server. */
  def collations(): DataFrame = fn("collations", Seq.empty)

  /** The SQL keywords of the server. */
  def sql_keywords(): DataFrame = fn("sql_keywords", Seq.empty)

  /** A row per element of the given variant array or object. */
  def variant_explode(input: Column): DataFrame = fn("variant_explode", Seq(input))

  /** Like [[variant_explode]], but produces a row of nulls for a non-collection variant. */
  def variant_explode_outer(input: Column): DataFrame = fn("variant_explode_outer", Seq(input))

  /** The logs of the server's Python workers (Spark 4.1+). */
  def python_worker_logs(): DataFrame = fn("python_worker_logs", Seq.empty)
}
