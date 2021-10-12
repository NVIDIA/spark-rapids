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

import scala.collection.mutable.{Buffer, LinkedHashMap, ListBuffer}

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

  private def writeCSVHeader(writer: ToolTextFileWriter,
      appInfos: Seq[QualificationSummaryInfo],
      headersAndSizes: LinkedHashMap[String, Int]): Unit = {
    writer.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
  }

  def writeCSV(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(finalOutputDir, s"${logFileName}.csv", "CSV")
    try {
      val headersAndSizes = QualOutputWriter
        .getDetailedHeaderStringsAndSizes(sums, reportReadSchema)
      writeCSVHeader(csvFileWriter, sums, headersAndSizes)
      sums.foreach { appSum =>
        csvFileWriter.write(QualOutputWriter.constructAppDetailedInfo(appSum, headersAndSizes,
          ",", false, reportReadSchema))
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
    val entireHeader = QualOutputWriter.constructSummaryHeader(appIdMaxSize, "|", true)
    val sep = "=" * (entireHeader.size - 1)
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
      val wStr = QualOutputWriter.constructAppSummaryInfo(sumInfo, appIdMaxSize, "|", true)
      writer.write(wStr)
      if (printStdout) print(wStr)
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
  val complexTypesStr = "Complex Types"
  val nestedTypesStr = "Unsupported Nested Types"
  val readSchemaStr = "Read Schema"
  val appDurStrSize = appDurStr.size
  val sqlDurStrSize = sqlDurStr.size
  val problemStrSize = problemDurStr.size

  def getAppIdSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    getMaxSizeForHeader(sizes, QualOutputWriter.appIdStr)
  }

  private def getMaxSizeForHeader(sizes: Seq[Int], headerTxtStr: String): Int = {
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
  }

  // ordered hashmap contains each header string and the size to use
  private def constructOutputRow(
      strAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean = false): String = {
    constructOutputRow(strAndSizes.toBuffer, delimiter, prettyPrint)
  }

  private def constructOutputRow(
      strAndSizes: Buffer[(String, Int)],
      delimiter: String = "|",
      prettyPrint: Boolean = false): String = {
    val entireHeader = new StringBuffer
    if (prettyPrint) {
      entireHeader.append(delimiter)
    }
    val lastEntry = strAndSizes.last
    strAndSizes.dropRight(1).foreach { case (str, strSize) =>
      if (prettyPrint) {
        entireHeader.append(s"%${strSize}s${delimiter}".format(str))
      } else {
        entireHeader.append(s"${str}${delimiter}")
      }
    }
    // for the last element we don't want to print the delimiter at the end unless
    // pretty printing
    if (prettyPrint) {
      entireHeader.append(s"%${lastEntry._2}s${delimiter}".format(lastEntry._1))
    } else {
      entireHeader.append(s"${lastEntry._1}")
    }
    entireHeader.append("\n")
    entireHeader.toString
  }

  private def getSummaryHeaderStringsAndSizes(appIdMaxSize: Int): LinkedHashMap[String, Int] = {
    LinkedHashMap[String, Int](
      appIdStr -> appIdMaxSize,
      appDurStr -> appDurStrSize,
      sqlDurStr -> sqlDurStrSize,
      problemDurStr -> problemStrSize
    )
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
      complexTypesStr -> getMaxSizeForHeader(appInfos.map(_.complexTypes.size), complexTypesStr),
      nestedTypesStr -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        nestedTypesStr),
    )
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (readSchemaStr -> getMaxSizeForHeader(appInfos.map(_.readFileFormats.size), readSchemaStr))
    }
    detailedHeadersAndFields
  }

  def constructSummaryHeader(appIdMaxSize: Int, delimiter: String,
      prettyPrint: Boolean): String = {
    val headersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(appIdMaxSize)
    QualOutputWriter.constructOutputRow(headersAndSizes, delimiter, prettyPrint)
  }

  def constructAppSummaryInfo(sumInfo: QualificationSummaryInfo,
      appIdMaxSize: Int, delimiter: String, prettyPrint: Boolean): String = {
    val dataMap = ListBuffer[(String, Int)](
      sumInfo.appId -> appIdMaxSize,
      sumInfo.appDuration.toString -> appDurStrSize,
      sumInfo.sqlDataFrameDuration.toString -> sqlDurStrSize,
      sumInfo.sqlDurationForProblematic.toString -> problemStrSize
    )
    constructOutputRow(dataMap, delimiter, prettyPrint)
  }

  def constructDetailedHeader(headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): String = {
    QualOutputWriter.constructOutputRow(headersAndSizes, delimiter, prettyPrint)
  }

  def constructAppDetailedInfo(
      appInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean,
      reportReadSchema: Boolean = false): String = {
    val (readfileFormats, complexTypes, nestedComplexTypes) = if (delimiter.equals(",")) {
      val readFileFormats = stringIfempty(appInfo.readFileFormats.replace(",", ";"))
      val complexTypes = stringIfempty(appInfo.complexTypes.replace(",", ";"))
      val nestedComplexTypes = stringIfempty(appInfo.nestedComplexTypes.replace(",", ";"))
      (readFileFormats, complexTypes, nestedComplexTypes)
    } else {
      val readFileFormats = stringIfempty(appInfo.readFileFormats)
      val complexTypes = stringIfempty(appInfo.complexTypes)
      val nestedComplexTypes = stringIfempty(appInfo.nestedComplexTypes)
      (readFileFormats, complexTypes, nestedComplexTypes)
    }
    val dataMap = ListBuffer[(String, Int)](
      stringIfempty(appInfo.appName) -> headersAndSizes(appNameStr),
      stringIfempty(appInfo.appId) -> headersAndSizes(appIdStr),
      appInfo.score.toString -> headersAndSizes(scoreStr),
      stringIfempty(appInfo.potentialProblems) -> headersAndSizes(potProblemsStr),
      appInfo.sqlDataFrameDuration.toString -> headersAndSizes(sqlDurStr),
      appInfo.sqlDataframeTaskDuration.toString -> headersAndSizes(taskDurStr),
      appInfo.appDuration.toString -> headersAndSizes(appDurStr),
      appInfo.executorCpuTimePercent.toString -> headersAndSizes(execCPUPercentStr),
      appInfo.endDurationEstimated.toString -> headersAndSizes(appDurEstimatedStr),
      stringIfempty(appInfo.potentialProblems) -> headersAndSizes(sqlDurPotProbsStr),
      stringIfempty(appInfo.failedSQLIds) -> headersAndSizes(sqlIdsFailuresStr),
      appInfo.readScorePercent.toString -> headersAndSizes(readScorePercentStr),
      stringIfempty(appInfo.readFileFormatScore.toString) ->
        headersAndSizes(readFileFormatScoreStr),
      stringIfempty(appInfo.readFileFormatAndTypesNotSupported) ->
        headersAndSizes(readFileFormatAndTypesStr),
      stringIfempty(appInfo.writeDataFormat) -> headersAndSizes(writeDataFormatStr),
      complexTypes -> headersAndSizes(complexTypesStr),
      nestedComplexTypes -> headersAndSizes(nestedTypesStr)
    )

    if (reportReadSchema) {
      dataMap += (readfileFormats -> headersAndSizes(readSchemaStr))
    }
    constructOutputRow(dataMap, delimiter, prettyPrint)
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }
}
