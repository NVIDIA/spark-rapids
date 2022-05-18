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
 * for potential issues and time spent in Dataframe operations. The score could be
 * used to compare against other applications.
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
 * Run your queries and then get the summary or detailed output to see the results.
 * {{{
 *   // run your sql queries ...
 *   val summaryOutput = qualApp.getSummary()
 *   val detailedOutput = qualApp.getDetailed()
 * }}}
 *
 * @param readScorePercent The percent the read format and datatypes
 *                         apply to the score. Default is 20 percent.
 */
class RunningQualificationApp(readScorePercent: Int = QualificationArgs.DEFAULT_READ_SCORE_PERCENT)
  extends QualificationAppInfo(None, None, new PluginTypeChecker()) {

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
   * Get the summary report for qualification.
   * @param delimiter The delimiter separating fields of the summary report.
   * @param prettyPrint Whether to include the delimiter at start and end and
   *                    add spacing so the data rows align with column headings.
   * @return String containing the summary report.
   */
  def getSummary(delimiter: String = "|", prettyPrint: Boolean = true): String = {
    val appInfo = super.aggregateStats()
    appInfo match {
      case Some((info, _, _)) =>
        val appIdMaxSize = QualOutputWriter.getAppIdSize(Seq(info))
        val headersAndSizes =
          QualOutputWriter.getSummaryHeaderStringsAndSizes(Seq(info), appIdMaxSize)
        val headerStr = QualOutputWriter.constructOutputRowFromMap(headersAndSizes,
          delimiter, prettyPrint)
        val sumsToWrite = QualificationAppInfo.calculateEstimatedInfoSummary(Seq(info))
        val appInfoStr = QualOutputWriter.constructAppSummaryInfo(sumsToWrite.head,
          headersAndSizes, appIdMaxSize,
          delimiter, prettyPrint)
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
      case Some((info, _, _)) =>
        val headersAndSizes =
          QualOutputWriter.getDetailedHeaderStringsAndSizes(Seq(info),reportReadSchema )
        val headerStr = QualOutputWriter.constructDetailedHeader(headersAndSizes,
          delimiter, prettyPrint)
        val appInfoStr = QualOutputWriter.constructAppDetailedInfo(info, headersAndSizes, delimiter,
          prettyPrint, reportReadSchema)
        headerStr + appInfoStr
      case None =>
        logWarning(s"Unable to get qualification information for this application")
        ""
    }
  }
}
