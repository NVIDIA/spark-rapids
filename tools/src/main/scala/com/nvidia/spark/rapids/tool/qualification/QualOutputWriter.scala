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
import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo}
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

  // a file extension will be added to this later
  private val logFileName = "rapids_4_spark_qualification_output"

  def writeCSV(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}.csv", "CSV")
    try {
      val headersAndSizes = QualOutputWriter
        .getDetailedHeaderStringsAndSizes(sums, reportReadSchema)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { appSum =>
        csvFileWriter.write(QualOutputWriter.constructAppDetailedInfo(appSum, headersAndSizes,
          ",", false, reportReadSchema))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  private def getAllExecsFromPlan(plans: Seq[PlanInfo]): Seq[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
  }

  def writeExecReport(plans: Seq[PlanInfo], numOutputRows: Int) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}_execs.csv",
      "Plan Exec Info")
    try {
      val allExecs = getAllExecsFromPlan(plans)
      val headersAndSizes = QualOutputWriter
        .getDetailedExecsHeaderStringsAndSizes(allExecs, reportReadSchema)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      plans.foreach { plan =>
        val rows = QualOutputWriter.constructExecsInfo(plan, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  // write the text summary report
  def writeReport(summaries: Seq[QualificationSummaryInfo], numOutputRows: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}.log",
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
  val SQL_ID_STR = "SQL ID"
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
  val NONSQL_DUR_STR = "NONSQL Task Duration Plus Overhead"
  val ESTIMATED_DURATION_STR = "Estimated Duration"
  val UNSUPPORTED_DURATION_STR = "Unsupported Duration"
  val SPEEDUP_DURATION_STR = "Speedup Duration"
  val SPEEDUP_FACTOR_STR = "Speedup Factor"
  val TOTAL_SPEEDUP_STR = "Total Speedup"
  val SPEEDUP_BUCKET_STR = "Recommendation"
  val LONGEST_SQL_DURATION_STR = "Longest SQL Duration"
  val EXEC_STR = "Exec Name"
  val EXPR_STR = "Expression Name"
  val EXEC_DURATION = "Exec Duration"
  val EXEC_NODEID = "SQL Node Id"
  val EXEC_IS_SUPPORTED = "Exec Is Supported"
  val EXEC_STAGES = "Exec Stages"
  val EXEC_SHOULD_REMOVE = "Exec Should Remove"
  val EXEC_CHILDREN = "Exec Children"

  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val PROBLEM_DUR_SIZE: Int = PROBLEM_DUR_STR.size
  val SPEEDUP_BUCKET_STR_SIZE: Int = SPEEDUP_BUCKET_STR.size
  val TOTAL_SPEEDUP_STR_SIZE: Int = TOTAL_SPEEDUP_STR.size
  val LONGEST_SQL_DURATION_STR_SIZE: Int = LONGEST_SQL_DURATION_STR.size

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
      PROBLEM_DUR_STR -> PROBLEM_DUR_SIZE,
      TOTAL_SPEEDUP_STR -> TOTAL_SPEEDUP_STR_SIZE,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE
    )
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  private def getChildrenSize(execInfos: Seq[ExecInfo]): Seq[Int] = {
    execInfos.map(_.children.getOrElse(Seq.empty).mkString(",").size)
  }

  def getDetailedExecsHeaderStringsAndSizes(execInfos: Seq[ExecInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      SQL_ID_STR -> SQL_ID_STR.size,
      EXEC_STR -> getMaxSizeForHeader(execInfos.map(_.exec.size), EXEC_STR),
      EXPR_STR -> getMaxSizeForHeader(execInfos.map(_.expr.size), EXPR_STR),
      EXEC_DURATION -> EXEC_DURATION.size,
      EXEC_NODEID -> EXEC_NODEID.size,
      EXEC_IS_SUPPORTED -> EXEC_IS_SUPPORTED.size,
      EXEC_STAGES -> getMaxSizeForHeader(execInfos.map(_.stages.mkString(",").size), EXEC_STAGES),
      EXEC_SHOULD_REMOVE -> EXEC_SHOULD_REMOVE.size,
      EXEC_CHILDREN -> getMaxSizeForHeader(getChildrenSize(execInfos), EXEC_CHILDREN),
    )
    detailedHeadersAndFields
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
        NESTED_TYPES_STR),
      LONGEST_SQL_DURATION_STR -> LONGEST_SQL_DURATION_STR_SIZE,
      NONSQL_DUR_STR -> NONSQL_DUR_STR.size,
      ESTIMATED_DURATION_STR -> ESTIMATED_DURATION_STR.size,
      UNSUPPORTED_DURATION_STR -> UNSUPPORTED_DURATION_STR.size,
      SPEEDUP_DURATION_STR -> SPEEDUP_DURATION_STR.size,
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      TOTAL_SPEEDUP_STR -> TOTAL_SPEEDUP_STR.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR.size
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
      sumInfo.sqlDurationForProblematic.toString -> PROBLEM_DUR_SIZE,
      sumInfo.totalSpeedup.toString -> TOTAL_SPEEDUP_STR_SIZE,
      sumInfo.speedupBucket -> SPEEDUP_BUCKET_STR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def constructDetailedHeader(
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): String = {
    QualOutputWriter.constructOutputRowFromMap(headersAndSizes, delimiter, prettyPrint)
  }

  private def constructExecInfoBuffer(info: ExecInfo, delimiter: String = "|",
      prettyPrint: Boolean): String = {
    val data = ListBuffer[(String, Int)](
      info.sqlID.toString -> 1,
      stringIfempty(info.exec) -> 1,
      stringIfempty(info.expr) -> 1,
      info.speedupFactor.toString -> 1,
      info.duration.toString -> 1,
      info.nodeId.toString -> 1,
      info.isSupported.toString -> 1,
      info.stages.mkString(",") -> 1,
      info.children.getOrElse(Seq.empty).map(_.exec).mkString(",") -> 1,
      info.shouldRemove.toString -> 1)
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def constructExecsInfo(
      planInfo: PlanInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean): Seq[String] = {
    planInfo.execInfo.flatMap { info =>
      val children = info.children.map(_.map(constructExecInfoBuffer(_, delimiter, prettyPrint)))
        .getOrElse(Seq.empty)
      children :+ constructExecInfoBuffer(info, delimiter, prettyPrint)
    }
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
      nestedComplexTypes -> headersAndSizes(NESTED_TYPES_STR),
      appInfo.longestSqlDuration.toString -> headersAndSizes(LONGEST_SQL_DURATION_STR),
      appInfo.nonSqlTaskDurationAndOverhead.toString -> headersAndSizes(NONSQL_DUR_STR),
      appInfo.estimatedDuration.toString -> headersAndSizes(ESTIMATED_DURATION_STR),
      appInfo.unsupportedDuration.toString ->
        headersAndSizes(UNSUPPORTED_DURATION_STR),
      appInfo.speedupDuration.toString -> headersAndSizes(SPEEDUP_DURATION_STR),
      appInfo.speedupFactor.toString -> headersAndSizes(SPEEDUP_FACTOR_STR),
      appInfo.totalSpeedup.toString -> headersAndSizes(TOTAL_SPEEDUP_STR),
      stringIfempty(appInfo.speedupBucket) -> headersAndSizes(SPEEDUP_BUCKET_STR)
    )

    if (reportReadSchema) {
      data += (readFileFormats -> headersAndSizes(READ_SCHEMA_STR))
    }
    constructOutputRow(data, delimiter, prettyPrint)
  }
}
