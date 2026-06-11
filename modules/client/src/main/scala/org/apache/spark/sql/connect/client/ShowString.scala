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
 * Renders rows as the familiar `df.show()` table, client-side, matching the layout of
 * `Dataset.showString` in Apache Spark (horizontal and vertical). Rendering locally from collected
 * rows keeps `show()` deterministic and exactly consistent with `collect()`.
 *
 * The caller passes up to `numRows + 1` rows; the extra row (if present) only triggers the
 * "only showing top N rows" footer and is not displayed.
 */
object ShowString {

  def apply(
      rows: Seq[Row],
      schema: StructType,
      numRows: Int,
      truncate: Int,
      vertical: Boolean): String = {
    val hasMoreData = rows.length > numRows
    val take = rows.take(numRows)
    val header = schema.fieldNames.toSeq
    val dataRows = take.map { row =>
      (0 until row.length).map(i => formatCell(row.get(i), truncate))
    }
    val sb = new StringBuilder
    val numCols = header.length

    if (!vertical) {
      // Spark initialises every column to a minimum width of 3.
      val widths = Array.fill(numCols)(3)
      (header +: dataRows).foreach { r =>
        r.zipWithIndex.foreach { case (cell, i) =>
          if (i < numCols) widths(i) = math.max(widths(i), cell.length)
        }
      }
      val sep = widths.map("-" * _).mkString("+", "+", "+\n")
      sb.append(sep)
      sb.append(formatRow(header, widths, truncate))
      sb.append(sep)
      dataRows.foreach(r => sb.append(formatRow(r, widths, truncate)))
      sb.append(sep)
    } else {
      if (take.isEmpty) {
        sb.append("(0 rows)\n")
      } else {
        val maxFieldName = if (header.isEmpty) 0 else header.map(_.length).max
        take.indices.foreach { idx =>
          sb.append(s"-RECORD $idx").append("\n")
          header.zip(dataRows(idx)).foreach { case (name, value) =>
            sb.append(" ")
              .append(name.reverse.padTo(maxFieldName, ' ').reverse)
              .append(" | ")
              .append(value)
              .append("\n")
          }
        }
      }
    }

    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }
    sb.toString
  }

  private def formatRow(cells: Seq[String], widths: Array[Int], truncate: Int): String = {
    val padded = cells.zipWithIndex.map { case (cell, i) =>
      val w = if (i < widths.length) widths(i) else cell.length
      if (truncate > 0) cell.reverse.padTo(w, ' ').reverse // right-justify
      else cell.padTo(w, ' ') // left-justify
    }
    padded.mkString("|", "|", "|\n")
  }

  private[client] def formatCell(value: Any, truncate: Int): String = {
    val str = value match {
      case null => "null"
      case b: Array[Byte] => b.map("%02X".format(_)).mkString("[", " ", "]")
      case a: Array[_] => a.mkString("[", ", ", "]")
      case s: scala.collection.Seq[_] => s.mkString("[", ", ", "]")
      case m: scala.collection.Map[_, _] =>
        m.map { case (k, v) => s"$k -> $v" }.mkString("{", ", ", "}")
      case other => other.toString
    }
    if (truncate > 0 && str.length > truncate) {
      if (truncate < 4) str.substring(0, truncate)
      else str.substring(0, truncate - 3) + "..."
    } else str
  }
}
