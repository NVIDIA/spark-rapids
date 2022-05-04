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

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.Utils

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
      val resScale = res.setScale(2, BigDecimal.RoundingMode.HALF_UP)
      resScale.toDouble
    }
  }

  // given to duration values, calculate a human average
  // rounded to specified number of decimal places.
  def calculateAverage(first: Long, size: Long, places: Int): Double = {
    val firstDec = BigDecimal.decimal(first)
    val sizeDec = BigDecimal.decimal(size)
    if (firstDec == 0 || sizeDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec / sizeDec)
      val resScale = res.setScale(places, BigDecimal.RoundingMode.HALF_UP)
      resScale.toDouble
    }
  }
}

case class GpuEventLogException(message: String) extends Exception(message)
