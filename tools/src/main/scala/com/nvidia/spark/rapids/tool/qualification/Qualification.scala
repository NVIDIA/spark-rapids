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


    val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv")
    val appsSorted = try {
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
      val textFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.log")
      try {
        writeTextSummary(textFileWriter, sorted)
      } finally {
        textFileWriter.close()
      }
      sorted
    } finally {
      csvFileWriter.close()
    }
    appsSorted
  }

  val problemDurStr = "SQL Duration For Problematic"
  val headers = Array("App ID", "App Duration", "SQL Dataframe Duration", problemDurStr)

  private def getTextSpacing(sums: Seq[QualificationSummaryInfo]): (Int, Int)= {
    val sizePadLongs = problemDurStr.size
    val appIdMaxSize = sums.map(_.appId.size).max
    (appIdMaxSize, sizePadLongs)
  }

  def headerCSV: String = {
    "App Name,App ID,Score,Potential Problems,SQL Dataframe Duration," +
      "App Duration,Executor CPU Time Percent,App Duration Estimated," +
      "SQL Duration with Potential Problems\n"
    // TODO - just do what was there for testing
    // ,SQL Duration For Problematic"
  }

  def writeCSVHeader(writer: ToolTextFileWriter): Unit = {
    writer.write(headerCSV)
  }

  def writeCSV(writer: ToolTextFileWriter, sumInfo: QualificationSummaryInfo): Unit = {
    writer.write(sumInfo.toCSV + "\n")
  }

  def writeTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo]): Unit = {
    val (appIdMaxSize, sizePadLongs) = getTextSpacing(sums)
    val entireHeader = new StringBuffer
    entireHeader.append("|")
    val appIdSpaces = " " * (appIdMaxSize - headers(0).size - 1)
    entireHeader.append(s"$appIdSpaces${headers(0)}|")
    entireHeader.append(s"${" " * (sizePadLongs - headers(1).size - 1)}${headers(1)}|")
    entireHeader.append(s"${" " * (sizePadLongs - headers(2).size - 1)}${headers(2)}|")
    entireHeader.append(s"${" " * (sizePadLongs - headers(3).size - 1)}${headers(3)}|")
    entireHeader.append("\n")
    writer.write(entireHeader.toString)
    sums.foreach { sumInfo =>
      val appId = sumInfo.appId
      val appPad = " " * (appIdMaxSize - appId.size - 1)
      val appDur = sumInfo.appDuration.toString
      val sqlDur = sumInfo.sqlDataFrameDuration.toString
      val sqlProbDur = sumInfo.sqlDurationForProblematic.toString
      val appDurPad = " " * (sizePadLongs - appDur.size)
      val sqlDurPad = " " * (sizePadLongs - sqlDur.size)
      val sqlProbDurPad = " " * (sizePadLongs - sqlProbDur.size)
      val writeStr = s"$appPad$appId|$appDurPad$appDur|$sqlDurPad$sqlDur|$sqlProbDurPad$sqlProbDur|"
      writer.write(writeStr + "\n")
    }
  }
}
