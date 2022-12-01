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
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.{CLUSTER_ID, CLUSTER_ID_STR_SIZE, JOB_ID, JOB_ID_STR_SIZE, RUN_NAME, RUN_NAME_STR_SIZE, TEXT_DELIMITER}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.{EstimatedPerSQLSummaryInfo, EstimatedSummaryInfo, QualificationAppInfo, QualificationSummaryInfo}
/**
 * This class handles the output files for qualification.
 * It can write both a raw csv file and then a text summary report.
 *
 * @param outputDir The directory to output the files to
 * @param reportReadSchema Whether to include the read data source schema in csv output
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 * @param prettyPrintOrder The order in which to print the Text output
 * @param hadoopConf Optional Hadoop Configuration to use
 */
class QualOutputWriter(outputDir: String, reportReadSchema: Boolean,
    printStdout: Boolean, prettyPrintOrder: String,
    hadoopConf: Option[Configuration] = None) {

  def writeDetailedCSVReport(sums: Seq[QualificationSummaryInfo]): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}.csv", "CSV", hadoopConf)
    try {
      writeDetailedCSVReport(csvFileWriter, sums)
    } finally {
      csvFileWriter.close()
    }
  }

  protected def writeDetailedCSVReport(csvFileWriter: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo]): Unit = {
    val headersAndSizes = QualOutputWriter.getDetailedHeaderStringsAndSizes(sums,
      reportReadSchema)
    csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes,
      QualOutputWriter.CSV_DELIMITER, false))
    sums.foreach { sum =>
      csvFileWriter.write(QualOutputWriter.constructAppDetailedInfo(sum, headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, false, reportReadSchema))
    }
  }

  // write the text summary report
  def writeTextReport(sums: Seq[QualificationSummaryInfo], estSums: Seq[EstimatedSummaryInfo],
      numOutputRows: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir, s"${QualOutputWriter.LOGFILE_NAME}.log",
      "Summary Report", hadoopConf)
    try {
      writeTextReport(textFileWriter, sums, estSums, numOutputRows)
    } finally {
      textFileWriter.close()
    }
  }

  protected def writeTextReport(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo], estSum: Seq[EstimatedSummaryInfo],
      numOutputRows: Int): Unit = {
    val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
    val unSupExecMaxSize = QualOutputWriter.getunSupportedMaxSize(
      sums.map(_.unSupportedExecs.size),
      QualOutputWriter.UNSUPPORTED_EXECS_MAX_SIZE,
      QualOutputWriter.UNSUPPORTED_EXECS.size)
    val unSupExprMaxSize = QualOutputWriter.getunSupportedMaxSize(
      sums.map(_.unSupportedExprs.size),
      QualOutputWriter.UNSUPPORTED_EXPRS_MAX_SIZE,
      QualOutputWriter.UNSUPPORTED_EXPRS.size)
    val appNameMaxSize = QualOutputWriter.getAppNameSize(sums)
    val hasClusterTags = sums.exists(_.clusterTags.nonEmpty)
    val (clusterIdMaxSize, jobIdMaxSize, runNameMaxSize) = if (hasClusterTags) {
      (QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
        CLUSTER_ID, "").size), QualOutputWriter.CLUSTER_ID),
        QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
          JOB_ID, "").size), QualOutputWriter.JOB_ID),
        QualOutputWriter.getMaxSizeForHeader(sums.map(_.allClusterTagsMap.getOrElse(
          RUN_NAME, "").size), QualOutputWriter.RUN_NAME))
    } else {
      (CLUSTER_ID_STR_SIZE, JOB_ID_STR_SIZE, RUN_NAME_STR_SIZE)
    }
    val headersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(
      appNameMaxSize, appIdMaxSize, unSupExecMaxSize, unSupExprMaxSize, hasClusterTags,
      clusterIdMaxSize, jobIdMaxSize, runNameMaxSize)
    val entireHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
      TEXT_DELIMITER, true)
    val sep = "=" * (entireHeader.size - 1)
    writer.write(s"$sep\n")
    writer.write(entireHeader)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print("APPLICATION SUMMARY:\n")
      print(s"$sep\n")
      print(entireHeader)
      print(s"$sep\n")
    }
    val finalSums = estSum.take(numOutputRows)
    finalSums.foreach { sumInfo =>
      val wStr = QualOutputWriter.constructAppSummaryInfo(sumInfo, headersAndSizes,
        appIdMaxSize, unSupExecMaxSize, unSupExprMaxSize, hasClusterTags, clusterIdMaxSize,
        jobIdMaxSize, runNameMaxSize, TEXT_DELIMITER, true)
      writer.write(wStr)
      if (printStdout) print(wStr)
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }

  def writeStageReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_stages.csv",
      "Stage Exec Info", hadoopConf)
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

  def writePerSqlCSVReport(sums: Seq[QualificationSummaryInfo], maxSQLDescLength: Int): Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_persql.csv",
      "Per SQL CSV Report", hadoopConf)
    try {
      val appNameSize = QualOutputWriter.getAppNameSize(sums)
      val appIdSize = QualOutputWriter.getAppIdSize(sums)
      val sqlDescSize =
        QualOutputWriter.getSqlDescSize(sums, maxSQLDescLength, QualOutputWriter.CSV_DELIMITER)
      val headersAndSizes =
        QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize, appIdSize, sqlDescSize)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes,
        QualOutputWriter.CSV_DELIMITER, false))
      val appIdMaxSize = QualOutputWriter.getAppIdSize(sums)
      val sortedInfo = sortPerSqlInfo(sums)
      sortedInfo.foreach { sumInfo =>
        val row = QualOutputWriter.constructPerSqlSummaryInfo(sumInfo, headersAndSizes,
          appIdMaxSize, ",", false, maxSQLDescLength)
        csvFileWriter.write(row)
      }
    } finally {
      csvFileWriter.close()
    }
  }

  private def sortPerSqlInfo(
      sums: Seq[QualificationSummaryInfo]): Seq[EstimatedPerSQLSummaryInfo] = {
    val estSumPerSql = sums.flatMap(_.perSQLEstimatedInfo).flatten
    val sortedAsc = estSumPerSql.sortBy(sum => {
      (sum.info.recommendation, sum.info.estimatedGpuSpeedup,
        sum.info.estimatedGpuTimeSaved, sum.info.appDur, sum.info.appId)
    })
    if (QualificationArgs.isOrderAsc(prettyPrintOrder)) {
      sortedAsc
    } else {
      sortedAsc.reverse
    }
  }
  private def writePerSqlTextSummary(writer: ToolTextFileWriter,
      sums: Seq[QualificationSummaryInfo],
      numOutputRows: Int, maxSQLDescLength: Int): Unit = {
    val appNameSize = QualOutputWriter.getAppNameSize(sums)
    val appIdSize = QualOutputWriter.getAppIdSize(sums)
    val sqlDescSize =
      QualOutputWriter.getSqlDescSize(sums, maxSQLDescLength, QualOutputWriter.TEXT_DELIMITER)
    val headersAndSizes =
      QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize, appIdSize, sqlDescSize)
    val entireHeader = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
      TEXT_DELIMITER, true)
    val sep = "=" * (entireHeader.size - 1)
    writer.write(s"$sep\n")
    writer.write(entireHeader)
    writer.write(s"$sep\n")
    // write to stdout as well
    if (printStdout) {
      print("PER SQL SUMMARY:\n")
      print(s"$sep\n")
      print(entireHeader)
      print(s"$sep\n")
    }
    val sortedInfo = sortPerSqlInfo(sums)
    val finalSums = sortedInfo.take(numOutputRows)
    finalSums.foreach { estInfo =>
      val wStr = QualOutputWriter.constructPerSqlSummaryInfo(estInfo, headersAndSizes,
        appIdSize, TEXT_DELIMITER, true, maxSQLDescLength)
      writer.write(wStr)
      if (printStdout) print(wStr)
    }
    writer.write(s"$sep\n")
    if (printStdout) print(s"$sep\n")
  }

  def writePerSqlTextReport(sums: Seq[QualificationSummaryInfo], numOutputRows: Int,
      maxSQLDescLength: Int) : Unit = {
    val textFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_persql.log",
      "Per SQL Summary Report", hadoopConf)
    try {
      writePerSqlTextSummary(textFileWriter, sums, numOutputRows, maxSQLDescLength)
    } finally {
      textFileWriter.close()
    }
  }

  def writeExecReport(sums: Seq[QualificationSummaryInfo], order: String) : Unit = {
    val csvFileWriter = new ToolTextFileWriter(outputDir,
      s"${QualOutputWriter.LOGFILE_NAME}_execs.csv",
      "Plan Exec Info", hadoopConf)
    try {
      val plans = sums.flatMap(_.planInfo)
      val allExecs = QualOutputWriter.getAllExecsFromPlan(plans)
      val headersAndSizes = QualOutputWriter
        .getDetailedExecsHeaderStringsAndSizes(sums, allExecs.toSeq)
      csvFileWriter.write(QualOutputWriter.constructDetailedHeader(headersAndSizes, ",", false))
      sums.foreach { sumInfo =>
        val rows = QualOutputWriter.constructExecsInfo(sumInfo, headersAndSizes, ",", false)
        rows.foreach(csvFileWriter.write(_))
      }
    } finally {
      csvFileWriter.close()
    }
  }
}

case class FormattedQualificationSummaryInfo(
    appName: String,
    appId: String,
    recommendation: String,
    estimatedGpuSpeedup: Double,
    estimatedGpuDur: Double,
    estimatedGpuTimeSaved: Double,
    sqlDataframeDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    gpuOpportunity: Long,
    executorCpuTimePercent: Double,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    potentialProblems: String,
    longestSqlDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    endDurationEstimated: Boolean,
    unSupportedExecs: String,
    unSupportedExprs: String,
    clusterTags: Map[String, String])

object QualOutputWriter {
  val NON_SQL_TASK_DURATION_STR = "NonSQL Task Duration"
  val SQL_ID_STR = "SQL ID"
  val SQL_DESC_STR = "SQL Description"
  val STAGE_ID_STR = "Stage ID"
  val APP_ID_STR = "App ID"
  val APP_NAME_STR = "App Name"
  val APP_DUR_STR = "App Duration"
  val SQL_DUR_STR = "SQL DF Duration"
  val TASK_DUR_STR = "SQL Dataframe Task Duration"
  val STAGE_DUR_STR = "Stage Task Duration"
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
  val UNSUPPORTED_TASK_DURATION_STR = "Unsupported Task Duration"
  val SUPPORTED_SQL_TASK_DURATION_STR = "Supported SQL DF Task Duration"
  val SPEEDUP_FACTOR_STR = "Task Speedup Factor"
  val AVERAGE_SPEEDUP_STR = "Average Speedup Factor"
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
  val STAGE_ESTIMATED_STR = "Stage Estimated"
  val UNSUPPORTED_EXECS = "Unsupported Execs"
  val UNSUPPORTED_EXPRS = "Unsupported Expressions"
  val CLUSTER_TAGS = "Cluster Tags"
  val CLUSTER_ID = "ClusterId"
  val JOB_ID = "JobId"
  val RUN_NAME = "RunName"

  val APP_DUR_STR_SIZE: Int = APP_DUR_STR.size
  val SQL_DUR_STR_SIZE: Int = SQL_DUR_STR.size
  val NON_SQL_TASK_DURATION_SIZE: Int = NON_SQL_TASK_DURATION_STR.size
  val SPEEDUP_BUCKET_STR_SIZE: Int = QualificationAppInfo.STRONGLY_RECOMMENDED.size
  val LONGEST_SQL_DURATION_STR_SIZE: Int = LONGEST_SQL_DURATION_STR.size
  val GPU_OPPORTUNITY_STR_SIZE: Int = GPU_OPPORTUNITY_STR.size
  val UNSUPPORTED_EXECS_MAX_SIZE: Int = 25
  val UNSUPPORTED_EXPRS_MAX_SIZE: Int = 25
  val CLUSTER_ID_STR_SIZE: Int = CLUSTER_ID.size
  val JOB_ID_STR_SIZE: Int = JOB_ID.size
  val RUN_NAME_STR_SIZE: Int = RUN_NAME.size

  val CSV_DELIMITER = ","
  val TEXT_DELIMITER = "|"

  // a file extension will be added to this later
  val LOGFILE_NAME = "rapids_4_spark_qualification_output"

  def getAppIdSize(sums: Seq[QualificationSummaryInfo]): Int = {
    val sizes = sums.map(_.appId.size)
    getMaxSizeForHeader(sizes, QualOutputWriter.APP_ID_STR)
  }

  def getAppNameSize(sums: Seq[QualificationSummaryInfo]): Int = {
    getMaxSizeForHeader(sums.map(_.appName.size), APP_NAME_STR)
  }

  def getunSupportedMaxSize(unSupExecs: Seq[Int], maxStringSize: Int, headerSize: Int): Int = {
    val unSupportedExecsSize = unSupExecs.size
    val unSupportedExecsMax = if (unSupExecs.nonEmpty) {
      unSupExecs.max
    } else {
      0
    }
    // return maxString size if the unsupportedString exceeds maxStringSize
    if (unSupportedExecsSize > 0 && unSupportedExecsMax > maxStringSize) {
      maxStringSize
    } else if (unSupportedExecsSize > 0 && unSupportedExecsMax < maxStringSize
      && unSupportedExecsMax >= headerSize) {
      unSupportedExecsMax
    } else {
      headerSize
    }
  }

  def getSqlDescSize(sums: Seq[QualificationSummaryInfo], maxSQLDescLength: Int,
      delimiter: String): Int = {
    val sizes = sums.flatMap(_.perSQLEstimatedInfo).flatten.map{ info =>
      formatSQLDescription(info.sqlDesc, maxSQLDescLength, delimiter).size
    }
    val maxSizeOfDesc = getMaxSizeForHeader(sizes, QualOutputWriter.SQL_DESC_STR)
    Math.min(maxSQLDescLength, maxSizeOfDesc)
  }

  def getMaxSizeForHeader(sizes: Seq[Int], headerTxtStr: String): Int = {
    if (sizes.size > 0 && sizes.max > headerTxtStr.size) {
      sizes.max
    } else {
      headerTxtStr.size
    }
  }

  // ordered hashmap contains each header string and the size to use
  def constructOutputRowFromMap(
      strAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean = false): String = {
    constructOutputRow(strAndSizes.toBuffer, delimiter, prettyPrint)
  }

  private def constructOutputRow(
      strAndSizes: Buffer[(String, Int)],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean = false): String = {
    val entireHeader = new StringBuffer
    if (prettyPrint) {
      entireHeader.append(delimiter)
    }
    val lastEntry = strAndSizes.last
    strAndSizes.dropRight(1).foreach { case (str, strSize) =>
      if (prettyPrint) {
        val updatedString = stringLengthExceedsMax(str, strSize, delimiter)
        entireHeader.append(updatedString)
      } else {
        entireHeader.append(s"${str}${delimiter}")
      }
    }
    // for the last element we don't want to print the delimiter at the end unless
    // pretty printing
    if (prettyPrint) {
      val updatedString = stringLengthExceedsMax(lastEntry._1, lastEntry._2, delimiter)
      entireHeader.append(updatedString)
    } else {
      entireHeader.append(s"${lastEntry._1}")
    }
    entireHeader.append("\n")
    entireHeader.toString
  }

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) "\"\"" else str
  }

  private def stringLengthExceedsMax(str: String, strSize: Int, delimiter: String): String = {
    val prettyPrintValue = if (str.size > strSize) {
      val newStrSize = strSize - 3 // suffixing ... at the end
      s"%${newStrSize}.${newStrSize}s...${delimiter}".format(str)
    } else {
      s"%${strSize}.${strSize}s${delimiter}".format(str)
    }
    prettyPrintValue
  }

  def getDetailedHeaderStringsAndSizes(appInfos: Seq[QualificationSummaryInfo],
      reportReadSchema: Boolean): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> getMaxSizeForHeader(appInfos.map(_.appName.size), APP_NAME_STR),
      APP_ID_STR -> QualOutputWriter.getAppIdSize(appInfos),
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      TASK_DUR_STR -> TASK_DUR_STR.size,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      EXEC_CPU_PERCENT_STR -> EXEC_CPU_PERCENT_STR.size,
      SQL_IDS_FAILURES_STR -> getMaxSizeForHeader(appInfos.map(_.failedSQLIds.size),
        SQL_IDS_FAILURES_STR),
      READ_FILE_FORMAT_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.readFileFormats.map(_.length).sum),
          READ_FILE_FORMAT_TYPES_STR),
      WRITE_DATA_FORMAT_STR ->
        getMaxSizeForHeader(appInfos.map(_.writeDataFormat.map(_.length).sum),
          WRITE_DATA_FORMAT_STR),
      COMPLEX_TYPES_STR ->
        getMaxSizeForHeader(appInfos.map(_.complexTypes.size), COMPLEX_TYPES_STR),
      NESTED_TYPES_STR -> getMaxSizeForHeader(appInfos.map(_.nestedComplexTypes.size),
        NESTED_TYPES_STR),
      POT_PROBLEM_STR ->
        getMaxSizeForHeader(appInfos.map(_.potentialProblems.size), POT_PROBLEM_STR),
      LONGEST_SQL_DURATION_STR -> LONGEST_SQL_DURATION_STR_SIZE,
      NONSQL_DUR_STR -> NONSQL_DUR_STR.size,
      UNSUPPORTED_TASK_DURATION_STR -> UNSUPPORTED_TASK_DURATION_STR.size,
      SUPPORTED_SQL_TASK_DURATION_STR -> SUPPORTED_SQL_TASK_DURATION_STR.size,
      SPEEDUP_FACTOR_STR -> SPEEDUP_FACTOR_STR.size,
      APP_DUR_ESTIMATED_STR -> APP_DUR_ESTIMATED_STR.size,
      UNSUPPORTED_EXECS -> UNSUPPORTED_EXECS.size,
      UNSUPPORTED_EXPRS -> UNSUPPORTED_EXPRS.size
    )
    if (appInfos.exists(_.clusterTags.nonEmpty)) {
      detailedHeadersAndFields += (CLUSTER_TAGS -> getMaxSizeForHeader(
        appInfos.map(_.clusterTags.length), CLUSTER_TAGS))
    }
    if (reportReadSchema) {
      detailedHeadersAndFields +=
        (READ_SCHEMA_STR ->
          getMaxSizeForHeader(appInfos.map(_.readFileFormats.map(_.length).sum), READ_SCHEMA_STR))
    }
    detailedHeadersAndFields
  }

  private[qualification] def getSummaryHeaderStringsAndSizes(
      appNameMaxSize: Int,
      appIdMaxSize: Int,
      unSupExecMaxSize: Int = UNSUPPORTED_EXECS_MAX_SIZE,
      unSupExprMaxSize: Int = UNSUPPORTED_EXPRS_MAX_SIZE,
      hasClusterTags: Boolean = false,
      clusterIdMaxSize: Int = CLUSTER_ID_STR_SIZE,
      jobIdMaxSize: Int = JOB_ID_STR_SIZE,
      runNameMaxSize: Int = RUN_NAME_STR_SIZE): LinkedHashMap[String, Int] = {
    val data = LinkedHashMap[String, Int](
      APP_NAME_STR -> appNameMaxSize,
      APP_ID_STR -> appIdMaxSize,
      APP_DUR_STR -> APP_DUR_STR_SIZE,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE,
      UNSUPPORTED_EXECS -> unSupExecMaxSize,
      UNSUPPORTED_EXPRS -> unSupExprMaxSize
    )
    if (hasClusterTags) {
      data += (CLUSTER_ID -> clusterIdMaxSize)
      data += (JOB_ID -> jobIdMaxSize)
      data += (RUN_NAME -> runNameMaxSize)
    }
    data
  }

  def constructAppSummaryInfo(
      sumInfo: EstimatedSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      appIdMaxSize: Int,
      unSupExecMaxSize: Int,
      unSupExprMaxSize: Int,
      hasClusterTags: Boolean,
      clusterIdMaxSize: Int,
      jobIdMaxSize: Int,
      runNameMaxSize: Int,
      delimiter: String,
      prettyPrint: Boolean): String = {
    val data = ListBuffer[(String, Int)](
      sumInfo.appName -> headersAndSizes(APP_NAME_STR),
      sumInfo.appId -> appIdMaxSize,
      sumInfo.appDur.toString -> APP_DUR_STR_SIZE,
      sumInfo.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      ToolUtils.formatDoublePrecision(sumInfo.estimatedGpuDur) -> ESTIMATED_GPU_DURATION.size,
      ToolUtils.formatDoublePrecision(sumInfo.estimatedGpuSpeedup) -> ESTIMATED_GPU_SPEEDUP.size,
      ToolUtils.formatDoublePrecision(sumInfo.estimatedGpuTimeSaved) ->
        ESTIMATED_GPU_TIMESAVED.size,
      sumInfo.recommendation -> SPEEDUP_BUCKET_STR_SIZE,
      sumInfo.unsupportedExecs -> unSupExecMaxSize,
      sumInfo.unsupportedExprs -> unSupExprMaxSize
    )
    if (hasClusterTags) {
      data += (sumInfo.allTagsMap.getOrElse(CLUSTER_ID, "") -> clusterIdMaxSize)
      data += (sumInfo.allTagsMap.getOrElse(JOB_ID, "") -> jobIdMaxSize)
      data += (sumInfo.allTagsMap.getOrElse(RUN_NAME, "") -> runNameMaxSize)
    }
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
  def getDetailedPerSqlHeaderStringsAndSizes(
      appMaxNameSize: Int,
      appMaxIdSize: Int,
      sqlDescLength: Int): LinkedHashMap[String, Int] = {
    val detailedHeadersAndFields = LinkedHashMap[String, Int](
      APP_NAME_STR -> appMaxNameSize,
      APP_ID_STR -> appMaxIdSize,
      SQL_ID_STR -> SQL_ID_STR.size,
      SQL_DESC_STR -> sqlDescLength,
      SQL_DUR_STR -> SQL_DUR_STR_SIZE,
      GPU_OPPORTUNITY_STR -> GPU_OPPORTUNITY_STR_SIZE,
      ESTIMATED_GPU_DURATION -> ESTIMATED_GPU_DURATION.size,
      ESTIMATED_GPU_SPEEDUP -> ESTIMATED_GPU_SPEEDUP.size,
      ESTIMATED_GPU_TIMESAVED -> ESTIMATED_GPU_TIMESAVED.size,
      SPEEDUP_BUCKET_STR -> SPEEDUP_BUCKET_STR_SIZE
    )
    detailedHeadersAndFields
  }

  private def formatSQLDescription(sqlDesc: String, maxSQLDescLength: Int,
      delimiter: String): String = {
    val escapedMetaStr = ToolUtils.escapeMetaCharacters(sqlDesc).trim()
    val sqlDescTruncated = escapedMetaStr.substring(0,
      Math.min(maxSQLDescLength, escapedMetaStr.length))
    // should be a one for one replacement so length wouldn't be affected by this
    replaceDelimiter(sqlDescTruncated, delimiter)
  }

  def constructPerSqlSummaryInfo(
      sumInfo: EstimatedPerSQLSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      appIdMaxSize: Int,
      delimiter: String,
      prettyPrint: Boolean,
      maxSQLDescLength: Int): String = {
    val data = ListBuffer[(String, Int)](
      sumInfo.info.appName -> headersAndSizes(APP_NAME_STR),
      sumInfo.info.appId -> appIdMaxSize,
      sumInfo.sqlID.toString -> SQL_ID_STR.size,
      formatSQLDescription(sumInfo.sqlDesc, maxSQLDescLength, delimiter) ->
        headersAndSizes(SQL_DESC_STR),
      sumInfo.info.sqlDfDuration.toString -> SQL_DUR_STR_SIZE,
      sumInfo.info.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuDur) -> ESTIMATED_GPU_DURATION.size,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuSpeedup) ->
        ESTIMATED_GPU_SPEEDUP.size,
      ToolUtils.formatDoublePrecision(sumInfo.info.estimatedGpuTimeSaved) ->
        ESTIMATED_GPU_TIMESAVED.size,
      sumInfo.info.recommendation -> SPEEDUP_BUCKET_STR_SIZE
    )
    constructOutputRow(data, delimiter, prettyPrint)
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

  private def constructExecInfoBuffer(
      info: ExecInfo,
      appId: String,
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean,
      headersAndSizes: LinkedHashMap[String, Int]): String = {
    val data = ListBuffer[(String, Int)](
      stringIfempty(appId) -> headersAndSizes(APP_ID_STR),
      info.sqlID.toString -> headersAndSizes(SQL_ID_STR),
      stringIfempty(info.exec) -> headersAndSizes(EXEC_STR),
      stringIfempty(info.expr) -> headersAndSizes(EXEC_STR),
      ToolUtils.formatDoublePrecision(info.speedupFactor) -> headersAndSizes(SPEEDUP_FACTOR_STR),
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
      STAGE_DUR_STR -> STAGE_DUR_STR.size,
      UNSUPPORTED_TASK_DURATION_STR -> UNSUPPORTED_TASK_DURATION_STR.size,
      STAGE_ESTIMATED_STR -> STAGE_ESTIMATED_STR.size
    )
    detailedHeadersAndFields
  }

  def constructStagesInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean): Seq[String] = {
    val appId = sumInfo.appId
    sumInfo.stageInfo.map { info =>
      val data = ListBuffer[(String, Int)](
        stringIfempty(appId) -> headersAndSizes(APP_ID_STR),
        info.stageId.toString -> headersAndSizes(STAGE_ID_STR),
        ToolUtils.formatDoublePrecision(info.averageSpeedup) ->
          headersAndSizes(AVERAGE_SPEEDUP_STR),
        info.stageTaskTime.toString -> headersAndSizes(STAGE_DUR_STR),
        info.unsupportedTaskDur.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
        info.estimated.toString -> headersAndSizes(STAGE_ESTIMATED_STR))
      constructOutputRow(data, delimiter, prettyPrint)
    }
  }

  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Set[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }.toSet
  }

  def constructExecsInfo(
      sumInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String = TEXT_DELIMITER,
      prettyPrint: Boolean): Set[String] = {
    val allExecs = getAllExecsFromPlan(sumInfo.planInfo)
    val appId = sumInfo.appId
    allExecs.flatMap { info =>
      val children = info.children
        .map(_.map(constructExecInfoBuffer(_, appId, delimiter, prettyPrint, headersAndSizes)))
        .getOrElse(Seq.empty)
      children :+ constructExecInfoBuffer(info, appId, delimiter, prettyPrint, headersAndSizes)
    }
  }

  def createFormattedQualSummaryInfo(
      appInfo: QualificationSummaryInfo,
      delimiter: String = TEXT_DELIMITER) : FormattedQualificationSummaryInfo = {
    FormattedQualificationSummaryInfo(
      appInfo.appName,
      appInfo.appId,
      appInfo.estimatedInfo.recommendation,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuSpeedup),
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuDur),
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.estimatedInfo.estimatedGpuTimeSaved),
      appInfo.estimatedInfo.sqlDfDuration,
      appInfo.sqlDataframeTaskDuration,
      appInfo.estimatedInfo.appDur,
      appInfo.estimatedInfo.gpuOpportunity,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.executorCpuTimePercent),
      ToolUtils.renderTextField(appInfo.failedSQLIds, ",", delimiter),
      ToolUtils.renderTextField(appInfo.readFileFormatAndTypesNotSupported, ";", delimiter),
      ToolUtils.renderTextField(appInfo.readFileFormats, ":", delimiter),
      ToolUtils.renderTextField(appInfo.writeDataFormat, ";", delimiter).toUpperCase,
      ToolUtils.formatComplexTypes(appInfo.complexTypes, delimiter),
      ToolUtils.formatComplexTypes(appInfo.nestedComplexTypes, delimiter),
      ToolUtils.formatPotentialProblems(appInfo.potentialProblems, delimiter),
      appInfo.longestSqlDuration,
      appInfo.nonSqlTaskDurationAndOverhead,
      appInfo.unsupportedSQLTaskDuration,
      appInfo.supportedSQLTaskDuration,
      ToolUtils.truncateDoubleToTwoDecimal(appInfo.taskSpeedupFactor),
      appInfo.endDurationEstimated,
      appInfo.unSupportedExecs,
      appInfo.unSupportedExprs,
      appInfo.allClusterTagsMap
    )
  }

  private def constructDetailedAppInfoCSVRow(
      appInfo: FormattedQualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      reportReadSchema: Boolean = false): ListBuffer[(String, Int)] = {
    val data = ListBuffer[(String, Int)](
      stringIfempty(appInfo.appName) -> headersAndSizes(APP_NAME_STR),
      stringIfempty(appInfo.appId) -> headersAndSizes(APP_ID_STR),
      stringIfempty(appInfo.recommendation) -> headersAndSizes(SPEEDUP_BUCKET_STR),
      appInfo.estimatedGpuSpeedup.toString -> ESTIMATED_GPU_SPEEDUP.size,
      appInfo.estimatedGpuDur.toString -> ESTIMATED_GPU_DURATION.size,
      appInfo.estimatedGpuTimeSaved.toString -> ESTIMATED_GPU_TIMESAVED.size,
      appInfo.sqlDataframeDuration.toString -> headersAndSizes(SQL_DUR_STR),
      appInfo.sqlDataframeTaskDuration.toString -> headersAndSizes(TASK_DUR_STR),
      appInfo.appDuration.toString -> headersAndSizes(APP_DUR_STR),
      appInfo.gpuOpportunity.toString -> GPU_OPPORTUNITY_STR_SIZE,
      appInfo.executorCpuTimePercent.toString -> headersAndSizes(EXEC_CPU_PERCENT_STR),
      stringIfempty(appInfo.failedSQLIds) -> headersAndSizes(SQL_IDS_FAILURES_STR),
      stringIfempty(appInfo.readFileFormatAndTypesNotSupported) ->
        headersAndSizes(READ_FILE_FORMAT_TYPES_STR),
      stringIfempty(appInfo.writeDataFormat) -> headersAndSizes(WRITE_DATA_FORMAT_STR),
      stringIfempty(appInfo.complexTypes) -> headersAndSizes(COMPLEX_TYPES_STR),
      stringIfempty(appInfo.nestedComplexTypes) -> headersAndSizes(NESTED_TYPES_STR),
      stringIfempty(appInfo.potentialProblems) -> headersAndSizes(POT_PROBLEM_STR),
      appInfo.longestSqlDuration.toString -> headersAndSizes(LONGEST_SQL_DURATION_STR),
      appInfo.nonSqlTaskDurationAndOverhead.toString -> headersAndSizes(NONSQL_DUR_STR),
      appInfo.unsupportedSQLTaskDuration.toString -> headersAndSizes(UNSUPPORTED_TASK_DURATION_STR),
      appInfo.supportedSQLTaskDuration.toString -> headersAndSizes(SUPPORTED_SQL_TASK_DURATION_STR),
      appInfo.taskSpeedupFactor.toString -> headersAndSizes(SPEEDUP_FACTOR_STR),
      appInfo.endDurationEstimated.toString -> headersAndSizes(APP_DUR_ESTIMATED_STR),
      appInfo.unSupportedExecs -> headersAndSizes(UNSUPPORTED_EXECS),
      appInfo.unSupportedExprs -> headersAndSizes(UNSUPPORTED_EXPRS)
    )
    if (appInfo.clusterTags.nonEmpty) {
      data += appInfo.clusterTags.mkString(";") -> headersAndSizes(CLUSTER_TAGS)
    }
    if (reportReadSchema) {
      data += (stringIfempty(appInfo.readFileFormats) -> headersAndSizes(READ_SCHEMA_STR))
    }
    data
  }

  def constructAppDetailedInfo(
      summaryAppInfo: QualificationSummaryInfo,
      headersAndSizes: LinkedHashMap[String, Int],
      delimiter: String,
      prettyPrint: Boolean,
      reportReadSchema: Boolean): String = {
    val formattedAppInfo = createFormattedQualSummaryInfo(summaryAppInfo, delimiter)
    val data = constructDetailedAppInfoCSVRow(formattedAppInfo, headersAndSizes, reportReadSchema)
    constructOutputRow(data, delimiter, prettyPrint)
  }
}
