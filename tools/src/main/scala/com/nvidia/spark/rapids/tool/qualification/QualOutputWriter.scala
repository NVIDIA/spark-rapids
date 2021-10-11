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

import scala.collection.mutable.LinkedHashMap

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
    writer.write(QualOutputWriter.headerCSVDetailed(reportReadSchema) + "\n")
  }

  def writeCSV(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv", "CSV")
    try {
      writeCSVHeader(csvFileWriter)
      sums.foreach { appSum =>
        csvFileWriter.write(QualOutputWriter.toCSVDetailed(appSum, reportReadSchema) + "\n")
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
    val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
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
  val appNameStr = "App Name"
  val appDurStr = "App Duration"
  val sqlDurStr = "SQL DF Duration"
  val taskDurStr = "SQL Dataframe Task Duration"
  val scoreStr = "Score"
  val potProblemsStr = "Potential Problems"
  val execCPUPercentStr = "Executor CPU Time Percent"
  val appDurEstimatedStr = "App Duration Estimated"
  val sqlDurPotProbsStr = "SQL Duration with Potential Problems"
  val sqlIdsFailuresStr = "SQL Ids with Failures"
  val readScorePercentStr = "Read Score Percent"
  val readFileFormatScoreStr = "Read File Format Score"
  val readFileFormatAndTypesStr = "Unsupported Read File Formats and Types"
  val writeDataFormatStr = "Unsupported Write Data Format"
  val nestedTypesStr = "Unsupported Nested Types"
  val readSchemaStr = "Read Schema"
  val appDurStrSize = appDurStr.size
  val sqlDurStrSize = sqlDurStr.size
  val problemStrSize = problemDurStr.size

  def getAppIdSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    getMaxSizeForHeader(sizes, QualOutputWriter.appIdStr)
  }

  def getMaxSizeForHeader(sizes: Seq[Int], headerTxtStr: String): Int = {
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
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
    entireHeader.append(s"|%${appIdMaxSize}s|".format(QualOutputWriter.appNameStr))
    entireHeader.append(s"%${appDurStrSize}s|".format(QualOutputWriter.appDurStr))
    entireHeader.append(s"%${sqlDurStrSize}s|".format(QualOutputWriter.sqlDurStr))
    entireHeader.append(s"%${problemStrSize}s|".format(QualOutputWriter.problemDurStr))
    entireHeader.append("\n")
    entireHeader.toString
  }

  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      appNameStr -> getMaxSizeForHeader(appInfos.map(_.appName.size), appNameStr),
      appIdStr -> QualOutputWriter.getAppIdSize(appInfos),
      scoreStr -> getMaxSizeForHeader(appInfos.map(_.score.toString.size), scoreStr),
      potProblemsStr -> getMaxSizeForHeader(appInfos.map(_.potentialProblems.size), potProblemsStr),
      sqlDurStr -> sqlDurStr.size,
      taskDurStr -> taskDurStr.size,
      appDurStr -> appDurStr.size,
      execCPUPercentStr -> execCPUPercentStr.size,
      appDurEstimatedStr -> appDurEstimatedStr.size,
      sqlDurPotProbsStr -> sqlDurPotProbsStr.size,
      sqlIdsFailuresStr -> getMaxSizeForHeader(appInfos.map(_.failedSQLIds.size),
        sqlIdsFailuresStr),
      readScorePercentStr -> readScorePercentStr.size,
      readFileFormatScoreStr -> readFileFormatScoreStr.size,
      readFileFormatAndTypesStr -> getMaxSizeForHeader(appInfos.map(_.readFileFormats.size),
        readFileFormatAndTypesStr),
      writeDataFormatStr -> getMaxSizeForHeader(appInfos.map(_.writeDataFormat.size),
        writeDataFormatStr),
      nestedTypesStr -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        nestedTypesStr),
    )
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (readSchemaStr -> getMaxSizeForHeader(appInfos.map(_.readFileFormats.size), readSchemaStr))
    }
    detailedHeadersAndFields
  }

  // ordered hashmap contains each header string and the size to use
  def constructHeaderTextStringDetailed(strAndSizes: LinkedHashMap[String, Int]): String = {
    val entireHeader = new StringBuffer
    entireHeader.append("|")
    strAndSizes.foreach { case (str, strSize) =>
      entireHeader.append(s"%${strSize}s|".format(str))
    }
    entireHeader.append("\n")
    entireHeader.toString
  }

  def constructAppInfoTextStringDetailed(sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int], reportReadSchema: Boolean = false): String = {
    val details = Seq(s"%${headersAndSizes(appNameStr)}s".format(sumInfo.appName),
      s"%${headersAndSizes(appIdStr)}s".format(sumInfo.appId),
      s"%${headersAndSizes(scoreStr)}s".format(sumInfo.score.toString),
      s"%${headersAndSizes(potProblemsStr)}s".format(sumInfo.potentialProblems),
      s"%${headersAndSizes(sqlDurStr)}s".format(sumInfo.sqlDataFrameDuration.toString),
      s"%${headersAndSizes(taskDurStr)}s".format(sumInfo.sqlDataframeTaskDuration.toString),
      s"%${headersAndSizes(appDurStr)}s".format(sumInfo.appDuration.toString),
      s"%${headersAndSizes(execCPUPercentStr)}s".format(sumInfo.executorCpuTimePercent.toString),
      s"%${headersAndSizes(appDurEstimatedStr)}s".format(sumInfo.endDurationEstimated.toString),
      s"%${headersAndSizes(sqlDurPotProbsStr)}s".format(sumInfo.potentialProblems),
      s"%${headersAndSizes(sqlIdsFailuresStr)}s".format(sumInfo.failedSQLIds),
      s"%${headersAndSizes(readScorePercentStr)}s".format(sumInfo.readScorePercent.toString),
      s"%${headersAndSizes(readFileFormatScoreStr)}s".format(sumInfo.readFileFormatScore.toString),
      s"%${headersAndSizes(readFileFormatAndTypesStr)}s"
        .format(sumInfo.readFileFormatAndTypesNotSupported),
      s"%${headersAndSizes(writeDataFormatStr)}s".format(sumInfo.writeDataFormat),
      s"%${headersAndSizes(nestedTypesStr)}s".format(sumInfo.nestedComplexTypes)
    )
    val finalDetails = if (reportReadSchema) {
      details :+ s"%${headersAndSizes(readSchemaStr)}s".format(sumInfo.readFileFormats)
    } else {
      details
    }
    val entireHeader = new StringBuffer
    entireHeader.append("|")
    finalDetails.foreach { str =>
      entireHeader.append(s"${str}|")
    }
    entireHeader.toString
  }

  def headerCSVDetailed(reportReadSchema: Boolean): String = {
    val initHeader = s"$appNameStr,${appIdStr},${scoreStr}," +
      s"${potProblemsStr},${sqlDurStr},${taskDurStr}," +
      s"${appDurStr},${execCPUPercentStr},${appDurEstimatedStr}," +
      s"${sqlDurPotProbsStr},${sqlIdsFailuresStr},${readScorePercentStr}," +
      s"${readFileFormatScoreStr},${readFileFormatAndTypesStr}," +
      s"${writeDataFormatStr},${nestedTypesStr}"
    if (reportReadSchema) {
      initHeader + s",${readSchemaStr}"
    } else {
      initHeader
    }
  }

  def headerCSVSummary: String = {
    val entireHeader = new StringBuffer
    entireHeader.append(s"${QualOutputWriter.appNameStr},")
    entireHeader.append(s"${QualOutputWriter.appDurStr},")
    entireHeader.append(s"${QualOutputWriter.sqlDurStr},")
    entireHeader.append(s"${QualOutputWriter.problemDurStr}")
    entireHeader.append("\n")
    entireHeader.toString
  }

  def toCSVSummary(appSum: QualificationSummaryInfo, appIdSize: Int): String = {
    constructAppInfoTextString(appSum, appIdSize)
      .replaceAll("\\s", "")
      .replace("|", ",")
      .stripPrefix(",")
      .stripSuffix(",")
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  def toCSVDetailed(appSum: QualificationSummaryInfo, reportReadSchema: Boolean): String = {
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
