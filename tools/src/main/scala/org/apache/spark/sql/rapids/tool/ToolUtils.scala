/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.sql.DataFrame

object ToolUtils extends Logging {

  def isPluginEnabled(properties: Map[String, String]): Boolean = {
    (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
      && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean)
  }

  def showString(df: DataFrame, numRows: Int) = {
    df.showString(numRows, 0)
  }

  // given to duration values, calculate a human readable percent
  // rounded to 2 decimal places. ie 39.12%
  def calculateDurationPercent(first: Long, total: Long): Double = {
    val firstDec = BigDecimal.decimal(first)
    val totalDec = BigDecimal.decimal(total)
    if (firstDec == 0 || totalDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec / totalDec) * 100
      formatDoubleValue(res, 2)
    }
  }

  // given to duration values, calculate a human average
  // rounded to specified number of decimal places.
  def calculateAverage(first: Double, size: Long, places: Int): Double = {
    val firstDec = BigDecimal.decimal(first)
    val sizeDec = BigDecimal.decimal(size)
    if (firstDec == 0 || sizeDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec / sizeDec)
      formatDoubleValue(res, places)
    }
  }

  def formatDoubleValue(bigValNum: BigDecimal, places: Int): Double = {
    bigValNum.setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def formatDoublePrecision(valNum: Double): String = {
    truncateDoubleToTwoDecimal(valNum).toString
  }

  def truncateDoubleToTwoDecimal(valNum: Double): Double = {
    // floor is applied after multiplying by 100. This keeps the number "as is" up-to two decimal.
    math.floor(valNum * 100) / 100
  }

  def escapeMetaCharacters(str: String): String = {
    str.replaceAll("\n", "\\\\n")
      .replaceAll("\r", "\\\\r")
      .replaceAll("\t", "\\\\t")
      .replaceAll("\f", "\\\\f")
      .replaceAll("\b", "\\\\b")
      .replaceAll("\u000B", "\\\\v")
      .replaceAll("\u0007", "\\\\a")
  }

  /**
   * Converts a sequence of elements to a single string that can be appended to a formatted text.
   * Delegates to [[com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter]] to
   * replace what is used as a text delimiter with something else.
   *
   * @param values the sequence of elements to join together.
   * @param separator the separator string to use.
   * @param txtDelimiter the delimiter used by the output file format (i.e., comma for CSV).
   * @return a string representation of the input sequence value. In the resulting string the string
   *         representations (w.r.t. the method toString) of all elements are separated by
   *         the string sep.
   */
  def renderTextField(values: Seq[Any], separator: String, txtDelimiter: String): String = {
    replaceDelimiter(values.mkString(separator), txtDelimiter)
  }

  def formatComplexTypes(
      values: Seq[String], fileDelimiter: String = QualOutputWriter.CSV_DELIMITER): String = {
    renderTextField(values, ";", fileDelimiter)
  }

  def formatPotentialProblems(
      values: Seq[String], fileDelimiter: String = QualOutputWriter.CSV_DELIMITER): String = {
    renderTextField(values, ":", fileDelimiter)
  }
}

case class GpuEventLogException(message: String) extends Exception(message)
