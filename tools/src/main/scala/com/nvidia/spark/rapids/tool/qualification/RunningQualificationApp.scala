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

import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser
import com.nvidia.spark.rapids.tool.qualification.QualOutputWriter.SQL_DESC_STR

import org.apache.spark.SparkEnv
import org.apache.spark.sql.rapids.tool.qualification._

/**
 * A Qualification tool application used for analyzing the application while it is
 * actively running. The qualification tool analyzes applications to determine if the
 * RAPIDS Accelerator for Apache Spark might be a good fit for those applications.
 * The standalone tool runs on Spark event logs after they have run. This class provides
 * an API to use with a running Spark application and processes events as they arrive.
 * This tool is intended to give the user a starting point and does not guarantee the
 * applications it scores high will actually be accelerated the most. When running
 * like this on a single application, the detailed output may be most useful to look
 * for potential issues and time spent in Dataframe operations.
 *
 * Please note that this will use additional memory so use with caution if using with a
 * long running application. The perSqlOnly option will allow reporting at the per
 * SQL query level without tracking all the Application information, but currently does
 * not cleanup. There is a cleanupSQL function that the user can force cleanup if required.
 *
 * Create the `RunningQualicationApp`:
 * {{{
 *   val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp()
 * }}}
 *
 * Get the event listener from it and install it as a Spark listener:
 * {{{
 *   val listener = qualApp.getEventListener
 *   spark.sparkContext.addSparkListener(listener)
 * }}}
 *
 * Run your queries and then get the Application summary or detailed output to see the results.
 * {{{
 *   // run your sql queries ...
 *   val summaryOutput = qualApp.getSummary()
 *   val detailedOutput = qualApp.getDetailed()
 * }}}
 *
 * If wanting per sql query output, run your queries and then get the output you are interested in.
 * {{{
 *   // run your sql queries ...
 *   val csvHeader = qualApp.getPerSqlCSVHeader
 *   val txtHeader = qualApp.getPerSqlTextHeader
 *   val (csvOut, txtOut) = qualApp.getPerSqlTextAndCSVSummary(sqlID)
 *   // print header and output wherever its useful
 * }}}
 *
 * @param perSqlOnly allows reporting at the SQL query level and doesn't track
 *                   the entire application
 */
class RunningQualificationApp(
    perSqlOnly: Boolean = false,
    pluginTypeChecker: PluginTypeChecker = new PluginTypeChecker())
  extends QualificationAppInfo(None, None, pluginTypeChecker, reportSqlLevel=false, perSqlOnly) {
  // note we don't use the per sql reporting providing by QualificationAppInfo so we always
  // send down false for it

  // we don't know the max sql query name size so lets cap it at 100
  private val SQL_DESC_LENGTH = 100
  private lazy val appName = appInfo.map(_.appName).getOrElse("")
  private lazy val appNameSize = if (appName.nonEmpty) appName.size else 100
  private lazy val perSqlHeadersAndSizes = {
      QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize,
        appId.size, SQL_DESC_LENGTH)
  }

  def this() = {
    this(false)
  }

  // since application is running, try to initialize current state
  private def initApp(): Unit = {
    val appName = SparkEnv.get.conf.get("spark.app.name", "")
    val appIdConf = SparkEnv.get.conf.getOption("spark.app.id")
    val appStartTime = SparkEnv.get.conf.get("spark.app.startTime", "-1")

    // start event doesn't happen so initialize it
    val thisAppInfo = QualApplicationInfo(
      appName,
      appIdConf,
      appStartTime.toLong,
      "",
      None,
      None,
      endDurationEstimated = false
    )
    appId = appIdConf.getOrElse("")
    appInfo = Some(thisAppInfo)
  }

  initApp()

  /**
   * Get the IDs of the SQL queries currently being tracked.
   * @return a sequence of SQL IDs
   */
  def getAvailableSqlIDs: Seq[Long] = {
    sqlIdToInfo.keys.toSeq
  }

  /**
   * Get the per SQL query header in CSV format.
   * @return a string with the header
   */
  def getPerSqlCSVHeader: String = {
    QualOutputWriter.constructDetailedHeader(perSqlHeadersAndSizes,
      QualOutputWriter.CSV_DELIMITER, false)
  }

  /**
   * Get the per SQL query header in TXT format for pretty printing.
   * @return a string with the header
   */
  def getPerSqlTextHeader: String = {
    QualOutputWriter.constructDetailedHeader(perSqlHeadersAndSizes,
      QualOutputWriter.TEXT_DELIMITER, true)
  }

  /**
   * Get the per SQL query header.
   * @return a string with the header
   */
  def getPerSqlHeader(delimiter: String,
      prettyPrint: Boolean, sqlDescLength: Int = SQL_DESC_LENGTH): String = {
    perSqlHeadersAndSizes(SQL_DESC_STR) = sqlDescLength
    QualOutputWriter.constructDetailedHeader(perSqlHeadersAndSizes, delimiter, prettyPrint)
  }

  /**
   * Get the per SQL query summary report in both Text and CSV format.
   * @param sqlID The sqlID of the query.
   * @return a tuple of the CSV summary followed by the Text summary.
   */
  def getPerSqlTextAndCSVSummary(sqlID: Long): (String, String) = {
    val sqlInfo = aggregatePerSQLStats(sqlID)
    val csvResult = constructPerSqlResult(sqlInfo, QualOutputWriter.CSV_DELIMITER, false)
    val textResult = constructPerSqlResult(sqlInfo, QualOutputWriter.TEXT_DELIMITER, true)
    (csvResult, textResult)
  }

  /**
   * Get the per SQL query summary report for qualification for the specified sqlID.
   * @param sqlID The sqlID of the query.
   * @param delimiter The delimiter separating fields of the summary report.
   * @param prettyPrint Whether to include the delimiter at start and end and
   *                    add spacing so the data rows align with column headings.
   * @param sqlDescLength Maximum length to use for the SQL query description.
   * @return String containing the summary report, or empty string if its not available.
   */
  def getPerSQLSummary(sqlID: Long, delimiter: String = "|",
      prettyPrint: Boolean = true, sqlDescLength: Int = SQL_DESC_LENGTH): String = {
    val sqlInfo = aggregatePerSQLStats(sqlID)
    constructPerSqlResult(sqlInfo, delimiter, prettyPrint, sqlDescLength)
  }

  private def constructPerSqlResult(
      sqlInfo: Option[EstimatedPerSQLSummaryInfo],
      delimiter: String = "|",
      prettyPrint: Boolean = true,
      sqlDescLength: Int = SQL_DESC_LENGTH): String = {
    sqlInfo match {
      case Some(info) =>
        perSqlHeadersAndSizes(SQL_DESC_STR) = sqlDescLength
        QualOutputWriter.constructPerSqlSummaryInfo(info, perSqlHeadersAndSizes,
          appId.size, delimiter, prettyPrint, sqlDescLength)
      case None =>
        logWarning(s"Unable to get qualification information for this application")
        ""
    }
  }

  /**
   * Get the summary report for qualification.
   * @param delimiter The delimiter separating fields of the summary report.
   * @param prettyPrint Whether to include the delimiter at start and end and
   *                    add spacing so the data rows align with column headings.
   * @return String containing the summary report.
   */
  def getSummary(delimiter: String = "|", prettyPrint: Boolean = true): String = {
    if (!perSqlOnly) {
      val appInfo = super.aggregateStats()
      appInfo match {
        case Some(info) =>
          val unSupExecMaxSize = QualOutputWriter.getunSupportedMaxSize(
            Seq(info).map(_.unSupportedExecs.size),
            QualOutputWriter.UNSUPPORTED_EXECS_MAX_SIZE,
            QualOutputWriter.UNSUPPORTED_EXECS.size)
          val unSupExprMaxSize = QualOutputWriter.getunSupportedMaxSize(
            Seq(info).map(_.unSupportedExprs.size),
            QualOutputWriter.UNSUPPORTED_EXPRS_MAX_SIZE,
            QualOutputWriter.UNSUPPORTED_EXPRS.size)
          val hasClusterTags = info.clusterTags.nonEmpty
          val (clusterIdMax, jobIdMax, runNameMax) = if (hasClusterTags) {
            (QualOutputWriter.getMaxSizeForHeader(Seq(info).map(
              _.allClusterTagsMap.getOrElse(QualOutputWriter.CLUSTER_ID, "").size),
              QualOutputWriter.CLUSTER_ID),
              QualOutputWriter.getMaxSizeForHeader(Seq(info).map(
                _.allClusterTagsMap.getOrElse(QualOutputWriter.JOB_ID, "").size),
                QualOutputWriter.JOB_ID),
              QualOutputWriter.getMaxSizeForHeader(Seq(info).map(
                _.allClusterTagsMap.getOrElse(QualOutputWriter.RUN_NAME, "").size),
                QualOutputWriter.RUN_NAME))
          } else {
            (QualOutputWriter.CLUSTER_ID_STR_SIZE, QualOutputWriter.JOB_ID_STR_SIZE,
              QualOutputWriter.RUN_NAME_STR_SIZE)
          }
          val appHeadersAndSizes = QualOutputWriter.getSummaryHeaderStringsAndSizes(appName.size,
            info.appId.size, unSupExecMaxSize, unSupExprMaxSize, hasClusterTags,
            clusterIdMax, jobIdMax, runNameMax))
          val headerStr = QualOutputWriter.constructOutputRowFromMap(appHeadersAndSizes,
            delimiter, prettyPrint)

          val appInfoStr = QualOutputWriter.constructAppSummaryInfo(info.estimatedInfo,
            appHeadersAndSizes, appId.size, unSupExecMaxSize, unSupExprMaxSize, hasClusterTags,
            clusterIdMax, jobIdMax, runNameMax, delimiter, prettyPrint)
          headerStr + appInfoStr
        case None =>
          logWarning(s"Unable to get qualification information for this application")
          ""
      }
    } else {
      ""
    }
  }

  /**
   * Get the detailed report for qualification.
   * @param delimiter The delimiter separating fields of the summary report.
   * @param prettyPrint Whether to include the delimiter at start and end and
   *                    add spacing so the data rows align with column headings.
   * @return String containing the detailed report.
   */
  def getDetailed(delimiter: String = "|", prettyPrint: Boolean = true,
      reportReadSchema: Boolean = false): String = {
    if (!perSqlOnly) {
      val appInfo = super.aggregateStats()
      appInfo match {
        case Some(info) =>
          val headersAndSizesToUse =
            QualOutputWriter.getDetailedHeaderStringsAndSizes(Seq(info), reportReadSchema)
          val headerStr = QualOutputWriter.constructDetailedHeader(headersAndSizesToUse,
            delimiter, prettyPrint)
          val appInfoStr = QualOutputWriter.constructAppDetailedInfo(info, headersAndSizesToUse,
            delimiter, prettyPrint, reportReadSchema)
          headerStr + appInfoStr
        case None =>
          logWarning(s"Unable to get qualification information for this application")
          ""
      }
    } else {
      ""
    }
  }

  // don't aggregate at app level, just sql level
  private def aggregatePerSQLStats(sqlID: Long): Option[EstimatedPerSQLSummaryInfo] = {
    val sqlDesc = sqlIdToInfo.get(sqlID).map(_.description)
    val origPlanInfo = sqlPlans.get(sqlID).map { plan =>
      SQLPlanParser.parseSQLPlan(appId, plan, sqlID, sqlDesc.getOrElse(""), pluginTypeChecker, this)
    }
    val perSqlInfos = origPlanInfo.flatMap { pInfo =>
      // filter out any execs that should be removed
      val planInfos = removeExecsShouldRemove(Seq(pInfo))
      // get a summary of each SQL Query
      val perSqlStageSummary = summarizeSQLStageInfo(planInfos)
      sqlIdToInfo.get(pInfo.sqlID).map { sqlInfo =>
        val wallClockDur = sqlInfo.duration.getOrElse(0L)
        // get task duration ratio
        val sqlStageSums = perSqlStageSummary.filter(_.sqlID == pInfo.sqlID)
        val estimatedInfo = getPerSQLWallClockSummary(sqlStageSums, wallClockDur,
          sqlIDtoFailures.get(pInfo.sqlID).nonEmpty, appName)
        EstimatedPerSQLSummaryInfo(pInfo.sqlID, pInfo.sqlDesc, estimatedInfo)
      }
    }
    perSqlInfos
  }
}
