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
 * Create the `RunningQualicationApp`:
 * {{{
 *   val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp()
 * }}}
 *
 *  * Create the `RunningQualicationApp` that reports for each SQL Query:
 * {{{
 *   val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp(true)
 * }}}
 *
 * Get the event listener from it and install it as a Spark listener:
 * {{{
 *   val listener = qualApp.getEventListener
 *   spark.sparkContext.addSparkListener(listener)
 * }}}
 *
 * Run your queries and then get the summary or detailed output to see the results.
 * {{{
 *   // run your sql queries ...
 *   val summaryOutput = qualApp.getSummary()
 *   val detailedOutput = qualApp.getDetailed()
 * }}}
 *
 */
class RunningQualificationApp(reportSqlLevel: Boolean)
  extends QualificationAppInfo(None, None, new PluginTypeChecker(), reportSqlLevel) {

  // we don't know the max sql query name size so lets cap it at 100
  private val SQL_DESC_LENGTH = 100
  private lazy val appName = appInfo.map(_.appName).getOrElse("")
  private lazy val appNameSize = if (appName.nonEmpty) appName.size else 100
  private lazy val headersAndSizes =
    QualOutputWriter.getDetailedPerSqlHeaderStringsAndSizes(appNameSize,
      appId.size, SQL_DESC_LENGTH)

  def this() = {
    this(false)
  }

  // since application is running, try to initialize current state
  private def initApp(): Unit = {
    val appName = SparkEnv.get.conf.get("spark.app.name", "")
    val appIdConf = SparkEnv.get.conf.getOption("spark.app.id")
    val appStartTime = SparkEnv.get.conf.get("spark.app.startTime", "-1")

    // start event doesn't happen so initial it
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
   * Get the per SQL query summary report in both Text and CSV format.
   * @param sqlID The sqlID of the query.
   * @return a tuple of the CSV summary followed by the Text summary.
   */
  def getPerSqlTextAndCSVSummary(sqlID: Long): (String, String) = {
    val appInfo = super.aggregateStats()
    val csvResult = constructPerSqlResult(sqlID, appInfo, QualOutputWriter.CSV_DELIMITER, false)
    val textResult = constructPerSqlResult(sqlID, appInfo, QualOutputWriter.TEXT_DELIMITER, true)
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
    val appInfo = super.aggregateStats()
    constructPerSqlResult(sqlID, appInfo, delimiter, prettyPrint, sqlDescLength)
  }

  private def constructPerSqlResult(
      sqlID: Long,
      appInfo: Option[QualificationSummaryInfo],
      delimiter: String = "|",
      prettyPrint: Boolean = true,
      sqlDescLength: Int = SQL_DESC_LENGTH): String = {
    appInfo match {
      case Some(info) =>
        if (info.perSQLEstimatedInfo.isDefined) {
          val res = info.perSQLEstimatedInfo.get.filter(_.sqlID == sqlID)
          if (res.isEmpty) {
            logWarning(s"Unable to get per sql qualification information for $sqlID")
            ""
          } else {
            assert(res.size == 1)
            val line = QualOutputWriter.constructPerSqlSummaryInfo(res.head, headersAndSizes,
              appId.size, delimiter, prettyPrint, SQL_DESC_LENGTH)
            line
          }
        } else {
          logWarning(s"Unable to get per sql qualification information, " +
            s"no SQL information available")
          ""
        }
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
    val appInfo = super.aggregateStats()
    appInfo match {
      case Some(info) =>
        val headerStr = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
          delimiter, prettyPrint)
        val appInfoStr = QualOutputWriter.constructAppSummaryInfo(info.estimatedInfo,
          headersAndSizes, appId.size, delimiter, prettyPrint)
        headerStr + appInfoStr
      case None =>
        logWarning(s"Unable to get qualification information for this application")
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
    val appInfo = super.aggregateStats()
    appInfo match {
      case Some(info) =>
        val headersAndSizesToUse = if (reportReadSchema) {
          QualOutputWriter.getDetailedHeaderStringsAndSizes(Seq(info), reportReadSchema)
        } else {
          headersAndSizes
        }
        val headerStr = QualOutputWriter.constructDetailedHeader(headersAndSizesToUse,
          delimiter, prettyPrint)
        val appInfoStr = QualOutputWriter.constructAppDetailedInfo(info, headersAndSizesToUse,
          delimiter, prettyPrint, reportReadSchema)
        headerStr + appInfoStr
      case None =>
        logWarning(s"Unable to get qualification information for this application")
        ""
    }
  }

  def getApplicationDetails: Option[QualificationSummaryInfo] = {
    super.aggregateStats()
  }
}
