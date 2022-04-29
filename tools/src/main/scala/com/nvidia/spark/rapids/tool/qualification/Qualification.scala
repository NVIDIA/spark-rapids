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

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._
import org.apache.spark.sql.rapids.tool.ui.QualificationReportGenerator

/**
 * Scores the applications for GPU acceleration and outputs the
 * reports.
 */
class Qualification(outputDir: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int, order: String,
    pluginTypeChecker: PluginTypeChecker, readScorePercent: Int,
    reportReadSchema: Boolean, printStdout: Boolean, uiEnabled: Boolean = false) extends Logging {

  private val allApps = new ConcurrentLinkedQueue[QualificationSummaryInfo]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("qualTool" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = qualifyApp(path, hadoopConf)
  }

  def qualifyApps(allPaths: Seq[EventLogInfo]): Seq[QualificationSummaryInfo] = {
    allPaths.foreach { path =>
      try {
        threadPool.submit(new QualifyThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    threadPool.shutdown()
    if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
        " stopping processing any more event logs")
      threadPool.shutdownNow()
    }

    // sort order and limit only applies to the report summary text file,
    // the csv file we write the entire data in descending order
    val allAppsSum = allApps.asScala.toSeq
    val sortedDesc = allAppsSum.sortBy(sum => {
        (-sum.score, -sum.sqlDataFrameDuration, -sum.appDuration)
    })
    val qWriter = new QualOutputWriter(getReportOutputPath, reportReadSchema, printStdout)
    qWriter.writeCSV(sortedDesc)

    val sortedForReport = if (QualificationArgs.isOrderAsc(order)) {
      allAppsSum.sortBy(sum => {
        (sum.score, sum.sqlDataFrameDuration, sum.appDuration)
      })
    } else {
      sortedDesc
    }
    qWriter.writeReport(sortedForReport, numRows)
    // At this point, we are ready to generate the UI report
    launchUIReportGenerator()
    sortedDesc
  }

  private def qualifyApp(
      path: EventLogInfo,
      hadoopConf: Configuration): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      val app = QualificationAppInfo.createApp(path, hadoopConf, pluginTypeChecker,
        readScorePercent)
      if (!app.isDefined) {
        logWarning(s"No Application found that contain SQL for ${path.eventLog.toString}!")
        None
      } else {
        val qualSumInfo = app.get.aggregateStats()
        if (qualSumInfo.isDefined) {
          allApps.add(qualSumInfo.get)
          val endTime = System.currentTimeMillis()
          logInfo(s"Took ${endTime - startTime}ms to process ${path.eventLog.toString}")
          // TODO: we should also catch the application info when exception occurs so that
          //       the report generator reports it to the users.
          collectQualificationInfoForApp(startTime, endTime, app.get)
        } else {
          logWarning(s"No aggregated stats for event log at: ${path.eventLog.toString}")
        }
      }
    } catch {
      case oom: OutOfMemoryError =>
        logError(s"OOM error while processing large file: ${path.eventLog.toString}." +
            s"Increase heap size.", oom)
        System.exit(1)
      case o: Error =>
        logError(s"Error occured while processing file: ${path.eventLog.toString}", o)
        System.exit(1)
      case e: Exception =>
        logWarning(s"Unexpected exception processing log ${path.eventLog.toString}, skipping!", e)
    }
  }

  // Define fields and helpers used for generating UI
  private val allAppsInfo = new ConcurrentLinkedQueue[QualApplicationInfo]()
  private val allDataSourceInfo = new ConcurrentLinkedQueue[AppDataSourceCase]()

  /**
   * Pull the required data from application object and report the status of the qualification run.
   * This also provides information about the analysis of the app.
   * For example, time, runtime Information, properties..etc.
   *
   * @param analysisStartTime
   * @param analysisEndTime
   * @param app
   */
  def collectQualificationInfoForApp(
      analysisStartTime: Long,
      analysisEndTime: Long,
      app: QualificationAppInfo) : Unit = {
    // add appInfo
    if (app.appInfo.isDefined) {
      allAppsInfo.add(app.appInfo.get)
    }
    allDataSourceInfo.add(AppDataSourceCase(app.appId, app.dataSourceInfo))
  }

  def launchUIReportGenerator() : Unit = {
    if (uiEnabled) {
      QualificationReportGenerator.createQualReportGenerator(this)
    }
  }

  /**
   * The outputPath of the current instance of the provider
   */
  def getReportOutputPath: String = {
    s"$outputDir/rapids_4_spark_qualification_output"
  }

  /**
   * @return all the [[QualificationSummaryInfo]] available
   */
  def getAllApplicationsInfo(): Seq[QualificationSummaryInfo] = {
    allApps.asScala.toSeq
  }

  /**
   * Returns a list of applications available for the report to show.
   * This is basically the summary of
   *
   * @return List of all known applications.
   */
  def getListing(): Seq[QualApplicationInfo] = {
    allAppsInfo.asScala.toSeq
  }

  def getDataSourceInfo(): Seq[AppDataSourceCase] = {
    allDataSourceInfo.asScala.toSeq
  }
}
