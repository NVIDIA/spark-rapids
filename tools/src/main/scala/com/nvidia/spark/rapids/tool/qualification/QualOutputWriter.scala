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
 * @param reportReadSchema Whether to include the read data source schema in csv output
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 */
class QualOutputWriter(outputDir: String, reportReadSchema: Boolean, printStdout: Boolean) {

  private val finalOutputDir = s"$outputDir/rapids_4_spark_qualification_output"
  // a file extension will be added to this later
  private val logFileName = "rapids_4_spark_qualification_output"

  private def writeCSVHeader(writer: ToolTextFileWriter): Unit = {
    writer.write(QualOutputWriter.headerCSV(reportReadSchema) + "\n")
  }

  def writeCSV(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv", "CSV")
    try {
      writeCSVHeader(csvFileWriter)
      sums.foreach { appSum =>
        csvFileWriter.write(QualOutputWriter.toCSV(appSum, reportReadSchema) + "\n")
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
    val entireHeader = QualOutputWriter.constructHeaderTextString(appIdMaxSize)
    val sep = "=" * entireHeader.size
    writer.write(s"$sep\n")
    writer.write(entireHeader)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print(s"$sep\n")
      print(entireHeader)
      print(s"$sep\n")
    }
    val finalSums = sums.take(numOutputRows)
    finalSums.foreach { sumInfo =>
      val wStr = QualOutputWriter.constructAppInfoTextString(sumInfo, appIdMaxSize)
      writer.write(wStr + "\n")
      if (printStdout) print(wStr + "\n")
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }
}

object QualOutputWriter {
  val problemDurStr = "Problematic Duration"
  val appIdStr = "App ID"
  val appDurStr = "App Duration"
  val sqlDurStr = "SQL DF Duration"
  val taskDurStr = "SQL Dataframe Task Duration"
  val appDurStrSize = appDurStr.size
  val sqlDurStrSize = sqlDurStr.size
  val problemStrSize = problemDurStr.size

  def getAppidSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    val appIdMaxSize = if (sizes.size > 0) sizes.max else QualOutputWriter.appIdStr.size
    appIdMaxSize
  }

  def constructAppInfoTextString(sumInfo: QualificationSummaryInfo, appIdMaxSize: Int): String = {
    val appId = sumInfo.appId
    val appIdStrV = s"%${appIdMaxSize}s".format(appId)
    val appDur = sumInfo.appDuration.toString
    val appDurStrV = s"%${QualOutputWriter.appDurStrSize}s".format(appDur)
    val sqlDur = sumInfo.sqlDataFrameDuration.toString
    val sqlDurStrV = s"%${QualOutputWriter.sqlDurStrSize}s".format(sqlDur)
    val sqlProbDur = sumInfo.sqlDurationForProblematic.toString
    val sqlProbDurStrV = s"%${QualOutputWriter.problemStrSize}s".format(sqlProbDur)
    s"|$appIdStrV|$appDurStrV|$sqlDurStrV|$sqlProbDurStrV|"
  }

  def constructHeaderTextString(appIdMaxSize: Int): String = {
    val entireHeader = new StringBuffer
    entireHeader.append(s"|%${appIdMaxSize}s|".format(QualOutputWriter.appIdStr))
    entireHeader.append(s"%${appDurStrSize}s|".format(QualOutputWriter.appDurStr))
    entireHeader.append(s"%${sqlDurStrSize}s|".format(QualOutputWriter.sqlDurStr))
    entireHeader.append(s"%${problemStrSize}s|".format(QualOutputWriter.problemDurStr))
    entireHeader.append("\n")
    entireHeader.toString
  }

  def headerCSV(reportReadSchema: Boolean): String = {
    val initHeader = s"App Name,${appIdStr},Score," +
      s"Potential Problems,${sqlDurStr},${taskDurStr}," +
      s"${appDurStr},Executor CPU Time Percent,App Duration Estimated," +
      "SQL Duration with Potential Problems,SQL Ids with Failures,Read Score Percent," +
      "Read File Format Score,Unsupported Read File Formats and Types," +
      "Unsupported Write Data Format,Unsupported Nested Types"
    if (reportReadSchema) {
      initHeader + ",Read Schema"
    } else {
      initHeader
    }
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  def toCSV(appSum: QualificationSummaryInfo, reportReadSchema: Boolean): String = {
    val probStr = stringIfempty(appSum.potentialProblems)
    val appIdStr = stringIfempty(appSum.appId)
    val appNameStr = stringIfempty(appSum.appName)
    val failedIds = stringIfempty(appSum.failedSQLIds)
    // since csv, replace any commas with ; in the schema
    val readFileFormats = stringIfempty(appSum.readFileFormats.replace(",", ";"))
    val complexTypes = stringIfempty(appSum.complexTypes.replace(",", ";"))
    val nestedComplexTypes = stringIfempty(appSum.nestedComplexTypes.replace(",", ";"))
    val readFormatNS = stringIfempty(appSum.readFileFormatAndTypesNotSupported)
    val writeDataFormat = stringIfempty(appSum.writeDataFormat)
    val initRow = s"$appNameStr,$appIdStr,${appSum.score},$probStr," +
      s"${appSum.sqlDataFrameDuration},${appSum.sqlDataframeTaskDuration}," +
      s"${appSum.appDuration},${appSum.executorCpuTimePercent}," +
      s"${appSum.endDurationEstimated},${appSum.sqlDurationForProblematic},$failedIds," +
      s"${appSum.readScorePercent},${appSum.readFileFormatScore}," +
      s"${readFormatNS},${writeDataFormat},${complexTypes},${nestedComplexTypes}"
    if (reportReadSchema) {
      initRow + s",$readFileFormats"
    } else {
      initRow
    }
  }

}
