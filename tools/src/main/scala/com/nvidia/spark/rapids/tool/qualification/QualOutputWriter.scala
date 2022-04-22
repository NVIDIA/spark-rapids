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

package com.nvidia.spark.rapids.tool.qualification

import scala.collection.mutable.{Buffer, LinkedHashMap, ListBuffer}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter

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

  private val finalOutputDir = outputDir
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
  val PROBLEM_DUR_STR = "Problematic Duration"
  val APP_ID_STR = "App ID"
  val APP_NAME_STR = "App Name"
  val APP_DUR_STR = "App Duration"
  val SQL_DUR_STR = "SQL DF Duration"
  val TASK_DUR_STR = "SQL Dataframe Task Duration"
  val SCORE_STR = "Score"
  val POT_PROBLEM_STR = "Potential Problems"
  val EXEC_CPU_PERCENT_STR = "Executor CPU Time Percent"
  val APP_DUR_ESTIMATED_STR = "App Duration Estimated"
  val SQL_DUR_POT_PROBLEMS = "SQL Duration with Potential Problems"
  val SQL_IDS_FAILURES_STR = "SQL Ids with Failures"
  val READ_SCORE_PERCENT_STR = "Read Score Percent"
  val READ_FILE_FORMAT_SCORE_STR = "Read File Format Score"
  val READ_FILE_FORMAT_TYPES_STR = "Unsupported Read File Formats and Types"
  val WRITE_DATA_FORMAT_STR = "Unsupported Write Data Format"
  val COMPLEX_TYPES_STR = "Complex Types"
  val NESTED_TYPES_STR = "Nested Complex Types"
  val READ_SCHEMA_STR = "Read Schema"
  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val PROBLEM_DUR_SIZE: Int = PROBLEM_DUR_STR.size

  def getAppIdSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    getMaxSizeForHeader(sizes, QualOutputWriter.APP_ID_STR)
  }

  private def getMaxSizeForHeader(sizes: Seq[Int], headerTxtStr: String): Int = {
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
  }

  // ordered hashmap contains each header string and the size to use
  private def constructOutputRowFromMap(
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

  private[qualification] def getSummaryHeaderStringsAndSizes(
      appIdMaxSize: Int): LinkedHashMap[String, Int] = {
    LinkedHashMap[String, Int](
      APP_ID_STR -> appIdMaxSize,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      PROBLEM_DUR_STR -> PROBLEM_DUR_SIZE
    )
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SCORE_STR -> getMaxSizeForHeader(appInfos.map(_.score.toString.size), SCORE_STR),
      POT_PROBLEM_STR ->
        getMaxSizeForHeader(appInfos.map(_.potentialProblems.size), POT_PROBLEM_STR),
      SQL_DUR_STR -> SQL_DUR_STR.size,
      TASK_DUR_STR -> TASK_DUR_STR.size,
      APP_DUR_STR -> APP_DUR_STR.size,
      EXEC_CPU_PERCENT_STR -> EXEC_CPU_PERCENT_STR.size,
      APP_DUR_ESTIMATED_STR -> APP_DUR_ESTIMATED_STR.size,
      SQL_DUR_POT_PROBLEMS -> SQL_DUR_POT_PROBLEMS.size,
      SQL_IDS_FAILURES_STR -> getMaxSizeForHeader(appInfos.map(_.failedSQLIds.size),
        SQL_IDS_FAILURES_STR),
      READ_SCORE_PERCENT_STR -> READ_SCORE_PERCENT_STR.size,
      READ_FILE_FORMAT_SCORE_STR -> READ_FILE_FORMAT_SCORE_STR.size,
      READ_FILE_FORMAT_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.readFileFormats.size),
        READ_FILE_FORMAT_TYPES_STR),
      WRITE_DATA_FORMAT_STR -> getMaxSizeForHeader(appInfos.map(_.writeDataFormat.size),
        WRITE_DATA_FORMAT_STR),
      COMPLEX_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.complexTypes.size), COMPLEX_TYPES_STR),
      NESTED_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        NESTED_TYPES_STR)
    )
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (READ_SCHEMA_STR ->
          getMaxSizeForHeader(appInfos.map(_.readFileFormats.size), READ_SCHEMA_STR))
    }
    detailedHeadersAndFields
  }

  def constructSummaryHeader(appIdMaxSize: Int, delimiter: String,
      prettyPrint: Boolean): String = {
    val headersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(appIdMaxSize)
    QualOutputWriter.constructOutputRowFromMap(headersAndSizes, delimiter, prettyPrint)
  }

  def constructAppSummaryInfo(sumInfo: QualificationSummaryInfo,
      appIdMaxSize: Int, delimiter: String, prettyPrint: Boolean): String = {
    val data = ListBuffer[(String, Int)](
      sumInfo.appId -> appIdMaxSize,
      sumInfo.appDuration.toString -> APP_DUR_STR_SIZE,
      sumInfo.sqlDataFrameDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.sqlDurationForProblematic.toString -> PROBLEM_DUR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def constructDetailedHeader(
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): String = {
    QualOutputWriter.constructOutputRowFromMap(headersAndSizes, delimiter, prettyPrint)
  }

  def constructAppDetailedInfo(
      appInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean,
      reportReadSchema: Boolean = false): String = {
    val readFileFormats = stringIfempty(replaceDelimiter(appInfo.readFileFormats, delimiter))
    val complexTypes = stringIfempty(replaceDelimiter(appInfo.complexTypes, delimiter))
    val nestedComplexTypes = stringIfempty(replaceDelimiter(appInfo.nestedComplexTypes, delimiter))
    val readFileFormatsNotSupported =
      stringIfempty(replaceDelimiter(appInfo.readFileFormatAndTypesNotSupported, delimiter))
    val dataWriteFormat = stringIfempty(replaceDelimiter(appInfo.writeDataFormat, delimiter))
    val potentialProbs = stringIfempty(replaceDelimiter(appInfo.potentialProblems, delimiter))
    val data = ListBuffer[(String, Int)](
      stringIfempty(appInfo.appName) -> headersAndSizes(APP_NAME_STR),
      stringIfempty(appInfo.appId) -> headersAndSizes(APP_ID_STR),
      appInfo.score.toString -> headersAndSizes(SCORE_STR),
      potentialProbs -> headersAndSizes(POT_PROBLEM_STR),
      appInfo.sqlDataFrameDuration.toString -> headersAndSizes(SQL_DUR_STR),
      appInfo.sqlDataframeTaskDuration.toString -> headersAndSizes(TASK_DUR_STR),
      appInfo.appDuration.toString -> headersAndSizes(APP_DUR_STR),
      appInfo.executorCpuTimePercent.toString -> headersAndSizes(EXEC_CPU_PERCENT_STR),
      appInfo.endDurationEstimated.toString -> headersAndSizes(APP_DUR_ESTIMATED_STR),
      stringIfempty(appInfo.sqlDurationForProblematic.toString) ->
          headersAndSizes(SQL_DUR_POT_PROBLEMS),
      stringIfempty(appInfo.failedSQLIds) -> headersAndSizes(SQL_IDS_FAILURES_STR),
      appInfo.readScorePercent.toString -> headersAndSizes(READ_SCORE_PERCENT_STR),
      stringIfempty(appInfo.readFileFormatScore.toString) ->
        headersAndSizes(READ_FILE_FORMAT_SCORE_STR),
      readFileFormatsNotSupported -> headersAndSizes(READ_FILE_FORMAT_TYPES_STR),
      dataWriteFormat -> headersAndSizes(WRITE_DATA_FORMAT_STR),
      complexTypes -> headersAndSizes(COMPLEX_TYPES_STR),
      nestedComplexTypes -> headersAndSizes(NESTED_TYPES_STR)
    )

    if (reportReadSchema) {
      data += (readFileFormats -> headersAndSizes(READ_SCHEMA_STR))
    }
    constructOutputRow(data, delimiter, prettyPrint)
  }
}
