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

import org.apache.spark.sql.rapids.tool.qualification.{EstimatedSummaryInfo, QualificationSummaryInfo, Recommendation}
import org.apache.spark.sql.rapids.tool.ui.QualificationReportGenerator

/**
 * This class handles the output files for qualification.
 * It can write both a raw csv file and then a text summary report.
 *
 * @param outputDir The directory to output the files to
 * @param reportReadSchema Whether to include the read data source schema in csv output
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 */
class QualOutputWriter(outputDir: String, reportReadSchema: Boolean, printStdout: Boolean,
    uiEnabled: Boolean) {

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

  def writeStageReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}_stages.csv",
      "Stage Exec Info")
    try {
      val headersAndSizes = QualOutputWriter
        .getDetailedStagesHeaderStringsAndSizes(sums)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructStagesInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeExecReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}_execs.csv",
      "Plan Exec Info")
    try {
      val plans = sums.flatMap(_.planInfo)
      val allExecs = QualOutputWriter.getAllExecsFromPlan(plans)
      val headersAndSizes = QualOutputWriter
        .getDetailedExecsHeaderStringsAndSizes(sums, allExecs)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructExecsInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }

  def writeDetailedReport(sums: Seq[QualificationSummaryInfo]): Unit = {
    writeCSV(sums)
    if (uiEnabled) {
      QualificationReportGenerator.generateDashBoard(outputDir, sums)
    }
  }

  // write the text summary report
  def writeReport(sums: Seq[QualificationSummaryInfo], estSums: Seq[EstimatedSummaryInfo],
      numOutputRows: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir, s"${logFileName}.log",
      "Summary report")
    try {
      writeTextSummary(textFileWriter, sums, estSums, numOutputRows)
    } finally {
      textFileWriter.close()
    }
  }

  private def writeTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo], estSum: Seq[EstimatedSummaryInfo],
      numOutputRows: Int): Unit = {
    val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
    val headersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(sums, appIdMaxSize)
    val entireHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes, "|", true)
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
    val finalSums = estSum.take(numOutputRows)
    estSum.foreach { sumInfo =>
      val wStr = QualOutputWriter.constructAppSummaryInfo(sumInfo, headersAndSizes,
        appIdMaxSize, "|", true)
      writer.write(wStr)
      if (printStdout) print(wStr)
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }
}

object QualOutputWriter {
  val NON_SQL_TASK_DURATION_STR = "NonSQL Task Duration"
  val SQL_ID_STR = "SQL ID"
  val STAGE_ID_STR = "Stage ID"
  val APP_ID_STR = "App ID"
  val APP_NAME_STR = "App Name"
  val APP_DUR_STR = "App Duration"
  val SQL_DUR_STR = "SQL DF Duration"
  val TASK_DUR_STR = "SQL Dataframe Task Duration"
  val POT_PROBLEM_STR = "Potential Problems"
  val EXEC_CPU_PERCENT_STR = "Executor CPU Time Percent"
  val APP_DUR_ESTIMATED_STR = "App Duration Estimated"
  val SQL_IDS_FAILURES_STR = "SQL Ids with Failures"
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
  val AVERAGE_SPEEDUP_STR = "Average Speedup Factor"
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
  val EXEC_CHILDREN_NODE_IDS = "Exec Children Node Ids"
  val GPU_OPPORTUNITY_STR = "GPU Opportunity"
  val ESTIMATED_GPU_DURATION = "Estimated GPU Duration"
  val ESTIMATED_GPU_SPEEDUP = "Estimated GPU Speedup"
  val ESTIMATED_GPU_TIMESAVED = "Estimated GPU Time Saved"

  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val NON_SQL_TASK_DURATION_SIZE: Int = NON_SQL_TASK_DURATION_STR.size
  val SPEEDUP_BUCKET_STR_SIZE: Int = Recommendation.Strongly_Recommended.toString.size
  val TOTAL_SPEEDUP_STR_SIZE: Int = TOTAL_SPEEDUP_STR.size
  val LONGEST_SQL_DURATION_STR_SIZE: Int = LONGEST_SQL_DURATION_STR.size
  val GPU_OPPORTUNITY_STR_SIZE: Int = GPU_OPPORTUNITY_STR.size

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
  def constructOutputRowFromMap(
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

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SQL_DUR_STR -> SQL_DUR_STR.size,
      TASK_DUR_STR -> TASK_DUR_STR.size,
      NON_SQL_TASK_DURATION_STR -> NON_SQL_TASK_DURATION_SIZE,
      APP_DUR_STR -> APP_DUR_STR.size,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      EXEC_CPU_PERCENT_STR -> EXEC_CPU_PERCENT_STR.size,
      SQL_IDS_FAILURES_STR -> getMaxSizeForHeader(appInfos.map(_.failedSQLIds.size),
        SQL_IDS_FAILURES_STR),
      READ_FILE_FORMAT_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.readFileFormats.size),
        READ_FILE_FORMAT_TYPES_STR),
      WRITE_DATA_FORMAT_STR -> getMaxSizeForHeader(appInfos.map(_.writeDataFormat.size),
        WRITE_DATA_FORMAT_STR),
      COMPLEX_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.complexTypes.size), COMPLEX_TYPES_STR),
      NESTED_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        NESTED_TYPES_STR),
      POT_PROBLEM_STR ->
        getMaxSizeForHeader(appInfos.map(_.potentialProblems.size), POT_PROBLEM_STR),
      LONGEST_SQL_DURATION_STR -> LONGEST_SQL_DURATION_STR_SIZE,
      NONSQL_DUR_STR -> NONSQL_DUR_STR.size,
      ESTIMATED_DURATION_STR -> ESTIMATED_DURATION_STR.size,
      UNSUPPORTED_DURATION_STR -> UNSUPPORTED_DURATION_STR.size,
      SPEEDUP_DURATION_STR -> SPEEDUP_DURATION_STR.size,
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      TOTAL_SPEEDUP_STR -> TOTAL_SPEEDUP_STR.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR.size,
      APP_DUR_ESTIMATED_STR -> APP_DUR_ESTIMATED_STR.size
    )
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (READ_SCHEMA_STR ->
          getMaxSizeForHeader(appInfos.map(_.readFileFormats.size), READ_SCHEMA_STR))
    }
    detailedHeadersAndFields
  }

  private[qualification] def getSummaryHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo],
      appIdMaxSize: Int): LinkedHashMap[String, Int] = {
    LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> appIdMaxSize,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE
    )
  }

  def constructAppSummaryInfo(
      sumInfo: EstimatedSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      appIdMaxSize: Int,
      delimiter: String,
      prettyPrint: Boolean): String = {
    val data = ListBuffer[(String, Int)](
      sumInfo.appName -> headersAndSizes(APP_NAME_STR),
      sumInfo.appId -> appIdMaxSize,
      sumInfo.appDur.toString -> APP_DUR_STR_SIZE,
      sumInfo.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      f"${sumInfo.estimatedGpuDur}%1.2f" -> ESTIMATED_GPU_DURATION.size,
      f"${sumInfo.estimatedGpuSpeedup}%1.2f" -> ESTIMATED_GPU_SPEEDUP.size,
      f"${sumInfo.estimatedGpuTimeSaved}%1.2f" -> ESTIMATED_GPU_TIMESAVED.size,
      sumInfo.recommendation.toString -> SPEEDUP_BUCKET_STR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def constructDetailedHeader(
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean): String = {
    QualOutputWriter.constructOutputRowFromMap(headersAndSizes, delimiter, prettyPrint)
  }

  private def getChildrenSize(execInfos: Seq[ExecInfo]): Seq[Int] = {
    execInfos.map(_.children.getOrElse(Seq.empty).mkString(",").size)
  }

  private def getChildrenNodeIdsSize(execInfos: Seq[ExecInfo]): Seq[Int] = {
    execInfos.map(_.children.getOrElse(Seq.empty).map(_.nodeId).mkString(",").size)
  }

  def getDetailedExecsHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      execInfos: Seq[ExecInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SQL_ID_STR -> SQL_ID_STR.size,
      EXEC_STR -> getMaxSizeForHeader(execInfos.map(_.exec.size), EXEC_STR),
      EXPR_STR -> getMaxSizeForHeader(execInfos.map(_.expr.size), EXPR_STR),
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      EXEC_DURATION -> EXEC_DURATION.size,
      EXEC_NODEID -> EXEC_NODEID.size,
      EXEC_IS_SUPPORTED -> EXEC_IS_SUPPORTED.size,
      EXEC_STAGES -> getMaxSizeForHeader(execInfos.map(_.stages.mkString(",").size), EXEC_STAGES),
      EXEC_CHILDREN -> getMaxSizeForHeader(getChildrenSize(execInfos), EXEC_CHILDREN),
      EXEC_CHILDREN_NODE_IDS -> getMaxSizeForHeader(getChildrenNodeIdsSize(execInfos),
        EXEC_CHILDREN_NODE_IDS),
      EXEC_SHOULD_REMOVE -> EXEC_SHOULD_REMOVE.size
    )
    detailedHeadersAndFields
  }

  private def constructExecInfoBuffer(info: ExecInfo, appId: String, delimiter: String = "|",
      prettyPrint: Boolean, headersAndSizes: LinkedHashMap[String, Int]): String = {
    val data = ListBuffer[(String, Int)](
      stringIfempty(appId) -> headersAndSizes(APP_ID_STR),
      info.sqlID.toString -> headersAndSizes(SQL_ID_STR),
      stringIfempty(info.exec) -> headersAndSizes(EXEC_STR),
      stringIfempty(info.expr) -> headersAndSizes(EXEC_STR),
      f"${info.speedupFactor}%1.2f" -> headersAndSizes(SPEEDUP_FACTOR_STR),
      info.duration.getOrElse(0).toString -> headersAndSizes(EXEC_DURATION),
      info.nodeId.toString -> headersAndSizes(EXEC_NODEID),
      info.isSupported.toString -> headersAndSizes(EXEC_IS_SUPPORTED),
      info.stages.mkString(":") -> headersAndSizes(EXEC_STAGES),
      info.children.getOrElse(Seq.empty).map(_.exec).mkString(":") ->
        headersAndSizes(EXEC_CHILDREN),
      info.children.getOrElse(Seq.empty).map(_.nodeId).mkString(":") ->
        headersAndSizes(EXEC_CHILDREN_NODE_IDS),
      info.shouldRemove.toString -> headersAndSizes(EXEC_SHOULD_REMOVE))
    constructOutputRow(data, delimiter, prettyPrint)
  }

  def getDetailedStagesHeaderStringsAndSizes(
      appInfos: Seq[QualificationSummaryInfo]): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      STAGE_ID_STR -> STAGE_ID_STR.size,
      AVERAGE_SPEEDUP_STR -> AVERAGE_SPEEDUP_STR.size,
      TASK_DUR_STR -> TASK_DUR_STR.size,
      UNSUPPORTED_DURATION_STR -> UNSUPPORTED_DURATION_STR.size
    )
    detailedHeadersAndFields
  }

  def constructStagesInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean): Seq[String] = {
    val appId = sumInfo.appId
    sumInfo.stageInfo.map { info =>
      val data = ListBuffer[(String, Int)](
        stringIfempty(appId) -> headersAndSizes(APP_ID_STR),
        info.stageId.toString -> headersAndSizes(STAGE_ID_STR),
        f"${info.averageSpeedup}%1.2f"  -> headersAndSizes(AVERAGE_SPEEDUP_STR),
        info.stageTaskTime.toString -> headersAndSizes(TASK_DUR_STR),
        info.unsupportedTaskDur.toString -> headersAndSizes(UNSUPPORTED_DURATION_STR))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Seq[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
  }

  def constructExecsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = "|",
      prettyPrint: Boolean): Seq[String] = {
    val allExecs = getAllExecsFromPlan(sumInfo.planInfo)
    val appId = sumInfo.appId
    allExecs.flatMap { info =>
      val children = info.children
        .map(_.map(constructExecInfoBuffer(_, appId, delimiter, prettyPrint, headersAndSizes)))
        .getOrElse(Seq.empty)
      children :+ constructExecInfoBuffer(info, appId, delimiter, prettyPrint, headersAndSizes)
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
    val speedupOpportunityWallClock = if (appInfo.sqlDataframeTaskDuration > 0) {
      val estimatedRatio = (appInfo.speedupOpportunity / appInfo.sqlDataframeTaskDuration)
      appInfo.sqlDataFrameDuration * estimatedRatio
    } else {
      0
    }
    val data = ListBuffer[(String, Int)](
      stringIfempty(appInfo.appName) -> headersAndSizes(APP_NAME_STR),
      stringIfempty(appInfo.appId) -> headersAndSizes(APP_ID_STR),
      appInfo.sqlDataFrameDuration.toString -> headersAndSizes(SQL_DUR_STR),
      appInfo.sqlDataframeTaskDuration.toString -> headersAndSizes(TASK_DUR_STR),
      appInfo.appDuration.toString -> headersAndSizes(APP_DUR_STR),
      speedupOpportunityWallClock.toString -> GPU_OPPORTUNITY_STR_SIZE,
      appInfo.executorCpuTimePercent.toString -> headersAndSizes(EXEC_CPU_PERCENT_STR),
      stringIfempty(appInfo.failedSQLIds) -> headersAndSizes(SQL_IDS_FAILURES_STR),
      readFileFormatsNotSupported -> headersAndSizes(READ_FILE_FORMAT_TYPES_STR),
      dataWriteFormat -> headersAndSizes(WRITE_DATA_FORMAT_STR),
      complexTypes -> headersAndSizes(COMPLEX_TYPES_STR),
      nestedComplexTypes -> headersAndSizes(NESTED_TYPES_STR),
      potentialProbs -> headersAndSizes(POT_PROBLEM_STR),
      appInfo.longestSqlDuration.toString -> headersAndSizes(LONGEST_SQL_DURATION_STR),
      appInfo.nonSqlTaskDurationAndOverhead.toString -> headersAndSizes(NONSQL_DUR_STR),
      f"${appInfo.estimatedTaskDuration}%1.2f" -> headersAndSizes(ESTIMATED_DURATION_STR),
      appInfo.unsupportedTaskDuration.toString ->
        headersAndSizes(UNSUPPORTED_DURATION_STR),
      appInfo.speedupOpportunity.toString -> headersAndSizes(SPEEDUP_DURATION_STR),
      f"${appInfo.speedupFactor}%1.2f" -> headersAndSizes(SPEEDUP_FACTOR_STR),
      f"${appInfo.totalSpeedup}%1.2f" -> headersAndSizes(TOTAL_SPEEDUP_STR),
      stringIfempty(appInfo.recommendation.toString) -> headersAndSizes(SPEEDUP_BUCKET_STR),
      appInfo.endDurationEstimated.toString -> headersAndSizes(APP_DUR_ESTIMATED_STR)
    )

    if (reportReadSchema) {
      data += (readFileFormats -> headersAndSizes(READ_SCHEMA_STR))
    }
    constructOutputRow(data, delimiter, prettyPrint)
  }
}
