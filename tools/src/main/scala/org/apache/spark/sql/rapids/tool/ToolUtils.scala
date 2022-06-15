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

  /**
   * Convert a sequence to a single string that can be appended to a formatted text.
   *
   * @param values Input array to be converted.
   * @param separator Input array to be converted.
   * @param txtDelimiter The delimiter used by the output file format (i.e., comma for CSV).
   *                     All occurrences of this delimiter are replaced by ";".
   * @return A single string in the form of item1[separator]item2 with all txtDelimiter replaced by
   *         semi-colons.
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
