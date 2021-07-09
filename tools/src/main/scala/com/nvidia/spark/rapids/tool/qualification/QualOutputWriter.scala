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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo

/**
 * This class handles the output files for qualification.
 * It can write both a raw csv file and then a text summary report.
 *
 * @param outputDir The directory to output the files to
 * @param reportReacSchema Whether to include the read data source schema in csv output
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 */
class QualOutputWriter(outputDir: String, reportReadSchema: Boolean, printStdout: Boolean) {

  private val finalOutputDir = s"$outputDir/rapids_4_spark_qualification_output"
  // a file extension will be added to this later
  private val logFileName = "rapids_4_spark_qualification_output"

  private val problemDurStr = "Problematic Duration"
  private val appIdStr = "App ID"
  private val appDurStr = "App Duration"
  private val sqlDurStr = "SQL DF Duration"
  private val taskDurStr = "SQL Dataframe Task Duration"

  private val headerCSV = {
    val initHeader = s"App Name,$appIdStr,Score,Potential Problems,$sqlDurStr,$taskDurStr," +
      s"$appDurStr,Executor CPU Time Percent,App Duration Estimated," +
      "SQL Duration with Potential Problems,SQL Ids with Failures,Read Score Percent," +
      "ReadFileFormat Score"
    if (reportReadSchema) {
      initHeader + ",Read Schema Info"
    } else {
      initHeader
    }
  }

  private def getAppidSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    val appIdMaxSize = if (sizes.size > 0) sizes.max else appIdStr.size
    appIdMaxSize
  }

  private def writeCSVHeader(writer: ToolTextFileWriter): Unit = {
    writer.write(headerCSV + "\n")
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  private def toCSV(appSum: QualificationSummaryInfo): String = {
    val probStr = stringIfempty(appSum.potentialProblems)
    val appIdStr = stringIfempty(appSum.appId)
    val appNameStr = stringIfempty(appSum.appName)
    val failedIds = stringIfempty(appSum.failedSQLIds)
    // since csv, replace any commas with ; in the schema
    val readFileFormats = stringIfempty(appSum.readFileFormats.replace(",", ";"))
    val readFileScoreRounded = f"${appSum.readFileFormatScore}%1.2f"

    val initRow = s"$appNameStr,$appIdStr,${appSum.score},$probStr," +
      s"${appSum.sqlDataFrameDuration},${appSum.sqlDataframeTaskDuration}," +
      s"${appSum.appDuration},${appSum.executorCpuTimePercent}," +
      s"${appSum.endDurationEstimated},${appSum.sqlDurationForProblematic},$failedIds," +
      s"${appSum.readScorePercent},${appSum.readFileFormatScore}"
    if (reportReadSchema) {
      initRow + s", $readFileFormats"
    } else {
      initRow
    }
  }

  def writeCSV(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv", "CSV")
    try {
      writeCSVHeader(csvFileWriter)
      sums.foreach { appSum =>
        csvFileWriter.write(toCSV(appSum) + "\n")
      }
    } finally {
      csvFileWriter.close()
    }
  }

  // write the text summary report
  def writeReport(summaries: Seq[QualificationSummaryInfo], numOutputRows: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.log",
      "Summary report")
    try {
      writeTextSummary(textFileWriter, summaries, numOutputRows)
    } finally {
      textFileWriter.close()
    }
  }

  private def writeTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo], numOutputRows: Int): Unit = {
    val appIdMaxSize = getAppidSize(sums)
    val entireHeader = new StringBuffer

    val appDurStrSize = appDurStr.size
    val sqlDurStrSize = sqlDurStr.size
    val problemStrSize = problemDurStr.size
    entireHeader.append(s"|%${appIdMaxSize}s|".format(appIdStr))
    entireHeader.append(s"%${appDurStrSize}s|".format(appDurStr))
    entireHeader.append(s"%${sqlDurStrSize}s|".format(sqlDurStr))
    entireHeader.append(s"%${problemStrSize}s|".format(problemDurStr))
    entireHeader.append("\n")
    val sep = "=" * (appIdMaxSize + (appDurStrSize + sqlDurStrSize + problemStrSize) + 5)
    writer.write(s"$sep\n")
    writer.write(entireHeader.toString)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print(s"$sep\n")
      print(entireHeader.toString)
      print(s"$sep\n")
    }

    val finalSums = sums.take(numOutputRows)
    finalSums.foreach { sumInfo =>
      val appId = sumInfo.appId
      val appIdStrV = s"%${appIdMaxSize}s".format(appId)
      val appDur = sumInfo.appDuration.toString
      val appDurStrV = s"%${appDurStrSize}s".format(appDur)
      val sqlDur = sumInfo.sqlDataFrameDuration.toString
      val taskDur = sumInfo.sqlDataframeTaskDuration.toString
      val sqlDurStrV = s"%${sqlDurStrSize}s".format(sqlDur)
      val sqlProbDur = sumInfo.sqlDurationForProblematic.toString
      val sqlProbDurStrV = s"%${problemStrSize}s".format(sqlProbDur)
      val wStr = s"|$appIdStrV|$appDurStrV|$sqlDurStrV|$sqlProbDurStrV|"
      writer.write(wStr + "\n")
      if (printStdout) print(wStr + "\n")
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }
}
