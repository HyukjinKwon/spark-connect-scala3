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

package org.apache.spark.sql.connect.client

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Renders a small set of [[Row]]s as the familiar Spark `df.show()` table, both in horizontal
 * (default) and vertical layouts. The rendering mirrors `Dataset.showString` in Apache Spark.
 *
 * The caller should pass up to `numRows + 1` rows; the extra row (if present) is used only to
 * decide whether the "only showing top N rows" footer is printed and is not itself displayed.
 */
object ShowString:

  def apply(
      rows: Seq[Row],
      schema: StructType,
      numRows: Int = 20,
      truncate: Int = 20,
      vertical: Boolean = false
  ): String =
    val hasMoreData = rows.length > numRows
    val take = rows.take(numRows)
    val header = schema.fieldNames.toSeq
    val dataRows = take.map { row =>
      (0 until row.length).map(i => formatCell(row.get(i), truncate)).toSeq
    }

    val sb = new StringBuilder
    val numCols = header.length

    if !vertical then
      // Compute column widths. Spark initialises every column to a minimum width of 3.
      val widths = Array.fill(numCols)(3)
      val allRows = header +: dataRows
      allRows.foreach { r =>
        r.zipWithIndex.foreach { case (cell, i) =>
          if i < numCols then widths(i) = math.max(widths(i), cell.length)
        }
      }
      val sep = widths.map("-" * _).mkString("+", "+", "+\n")
      sb.append(sep)
      sb.append(formatRow(header, widths, truncate))
      sb.append(sep)
      dataRows.foreach(r => sb.append(formatRow(r, widths, truncate)))
      sb.append(sep)
    else
    // Vertical layout: one record block per row.
    if take.isEmpty then sb.append("(0 rows)\n")
    else
      val maxFieldName = header.map(_.length).max
      take.zipWithIndex.foreach { case (_, idx) =>
        val recordHeader = s"-RECORD $idx"
        sb.append(recordHeader).append("\n")
        val cells = dataRows(idx)
        header.zip(cells).foreach { case (name, value) =>
          sb.append(" ")
            .append(name.reverse.padTo(maxFieldName, ' ').reverse)
            .append(" | ")
            .append(value)
            .append("\n")
        }
      }

    if hasMoreData then
      sb.append(s"only showing top $numRows ${if numRows == 1 then "row" else "rows"}\n")
    sb.toString

  private def formatRow(cells: Seq[String], widths: Array[Int], truncate: Int): String =
    val padded = cells.zipWithIndex.map { case (cell, i) =>
      val w = if i < widths.length then widths(i) else cell.length
      if truncate > 0 then cell.reverse.padTo(w, ' ').reverse // right-justify
      else cell.padTo(w, ' ') // left-justify
    }
    padded.mkString("|", "|", "|\n")

  private[client] def formatCell(value: Any, truncate: Int): String =
    val str = value match
      case null => "null"
      case b: Array[Byte] => b.map("%02X".format(_)).mkString("[", " ", "]")
      case a: Array[?] => a.mkString("[", ", ", "]")
      case s: scala.collection.Seq[?] => s.mkString("[", ", ", "]")
      case m: scala.collection.Map[?, ?] =>
        m.map { case (k, v) => s"$k -> $v" }.mkString("{", ", ", "}")
      case other => other.toString
    if truncate > 0 && str.length > truncate then
      // Add ellipsis, matching Spark: keep (truncate - 3) chars when truncate > 3.
      if truncate < 4 then str.substring(0, truncate)
      else str.substring(0, truncate - 3) + "..."
    else str
