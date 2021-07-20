/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.tool.profiling

import org.apache.commons.lang3.StringUtils

object ProfileOutputWriter {

  /**
   * Regular expression matching full width characters.
   *
   * Looked at all the 0x0000-0xFFFF characters (unicode) and showed them under Xshell.
   * Found all the full width characters, then get the regular expression.
   */
  private val fullWidthRegex = ("""[""" +
    // scalastyle:off nonascii
    "\u1100-\u115F" +
    "\u2E80-\uA4CF" +
    "\uAC00-\uD7A3" +
    "\uF900-\uFAFF" +
    "\uFE10-\uFE19" +
    "\uFE30-\uFE6F" +
    "\uFF00-\uFF60" +
    "\uFFE0-\uFFE6" +
    // scalastyle:on nonascii
    """]""").r

  /**
   * Return the number of half widths in a given string. Note that a full width character
   * occupies two half widths.
   *
   * For a string consisting of 1 million characters, the execution of this method requires
   * about 50ms.
   */
  def stringHalfWidth(str: String): Int = {
    if (str == null) 0 else str.length + fullWidthRegex.findAllIn(str).size
  }

  def showString(
      _numRows: Int,
      truncate: Int = 20,
      schema: List[String],
      rows: List[List[String]]): String = {
    val numRows = _numRows.max(0).min(2147483632 - 1)
    val hasMoreData = rows.length - 1 > numRows

    val sb = new StringBuilder
    val numCols = schema.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    // Initialise the width of each column to a minimum value
    val colWidths = Array.fill(numCols)(minimumColWidth)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), stringHalfWidth(cell))
      }
    }

    val paddedRows = rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i) - stringHalfWidth(cell) + cell.length)
        } else {
          StringUtils.rightPad(cell, colWidths(i) - stringHalfWidth(cell) + cell.length)
        }
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)


    // Print a footer
    if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

}
