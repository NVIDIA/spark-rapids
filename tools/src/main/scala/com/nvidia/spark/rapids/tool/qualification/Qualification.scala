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

package com.nvidia.spark.rapids.tool.qualification

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogInfo, ToolTextFileWriter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._

/**
 * Ranks the applications for GPU acceleration.
 */
class Qualification(outputDir: String) extends Logging {

  val finalOutputDir = s"$outputDir/rapids_4_spark_qualification_output"
  val logFileName = "rapids_4_spark_qualification_output"

  def qualifyApps(
      allPaths: Seq[EventLogInfo],
      numRows: Int): ArrayBuffer[QualificationSummaryInfo] = {
    val allAppsSum: ArrayBuffer[QualificationSummaryInfo] = ArrayBuffer[QualificationSummaryInfo]()

    val textFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.log")
    val appsSorted = try {
      writeTextHeader(textFileWriter)
      val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv")
      try {
        writeCSVHeader(csvFileWriter)

        allPaths.foreach { path =>
          val (app, _) = QualAppInfo.createApp(path, numRows)
          if (!app.isDefined) {
            logWarning("No Applications found that contain SQL!")
          } else {
            val qualSumInfo = app.get.aggregateStats()
            if (qualSumInfo.isDefined) {
              allAppsSum += qualSumInfo.get
              // write entire info to csv
              writeCSV(csvFileWriter, qualSumInfo.get)
            } else {
              logWarning(s"No aggregated stats for event log at: $path")
            }
          }
        }
        val sorted = allAppsSum.sortBy(sum => (-sum.score, -sum.sqlDataFrameDuration))
        writeTextSummary(textFileWriter, sorted)
        sorted
      } finally {
        csvFileWriter.close()
      }
    } finally {
      writeTextFooter(textFileWriter)
      textFileWriter.close()
    }
    appsSorted
  }

  def headerCSV: String = {
    "App Name,App ID,Score,Potential Problems,SQL Dataframe Duration," +
      "App Duration,Executor CPU Time Percent,App Duration Estimated," +
      "SQL Duration with Potential Problems\n"
    // TODO - just do what was there for testing
    // ,SQL Duration For Problematic"
  }

  def headerText: String = {
    "|App ID                 |App Duration|SQL Dataframe Duration|SQL Duration For Problematic|\n"
  }

  def writeCSVHeader(writer: ToolTextFileWriter): Unit = {
    writer.write(headerCSV)
  }

  def writeCSV(writer: ToolTextFileWriter, sumInfo: QualificationSummaryInfo): Unit = {
    writer.write(sumInfo.toCSV + "\n")
  }

  val textSeperator = "+---------------------+-----------------------+-----+------------------+--" +
    "--------------------+------------+-------------------------+----------------------+\n"

  def writeTextHeader(writer: ToolTextFileWriter): Unit = {
    writer.write(textSeperator)
    writer.write(headerText)
    writer.write(textSeperator)
  }

  def writeTextFooter(writer: ToolTextFileWriter): Unit = {
    writer.write(textSeperator)
  }

  def writeTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo]): Unit = {
    sums.foreach { sumInfo =>
      writer.write(sumInfo.toString + "\n")
    }
  }
}
