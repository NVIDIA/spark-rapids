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

class Qualification(outputDir: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int, order: String,
    pluginTypeChecker: PluginTypeChecker,
    reportReadSchema: Boolean, printStdout: Boolean, uiEnabled: Boolean) extends Logging {

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

    val allAppsSum = allApps.asScala.toSeq
    val qWriter = new QualOutputWriter(getReportOutputPath, reportReadSchema, printStdout)
    // sort order and limit only applies to the report summary text file,
    // the csv file we write the entire data in descending order
    val sortedDescDetailed = sortDescForDetailedReport(allAppsSum)
    qWriter.writeReport(allAppsSum, sortForExecutiveSummary(sortedDescDetailed, order), numRows)
    qWriter.writeDetailedReport(sortedDescDetailed)
    qWriter.writeExecReport(allAppsSum, order)
    qWriter.writeStageReport(allAppsSum, order)
    if (uiEnabled) {
      QualificationReportGenerator.generateDashBoard(outputDir, allAppsSum)
    }
    sortedDescDetailed
  }

  private def sortDescForDetailedReport(
      allAppsSum: Seq[QualificationSummaryInfo]): Seq[QualificationSummaryInfo] = {
    // Default sorting for of the csv files. Use the endTime to break the tie.
    allAppsSum.sortBy(sum => {
      (sum.estimatedInfo.recommendation, sum.estimatedInfo.estimatedGpuSpeedup,
        sum.estimatedInfo.estimatedGpuTimeSaved, sum.startTime + sum.estimatedInfo.appDur)
    }).reverse
  }

  // Sorting for the pretty printed executive summary.
  // The sums elements is ordered in descending order. so, only we need to reverse it if the order
  // is ascending
  private def sortForExecutiveSummary(appsSumDesc: Seq[QualificationSummaryInfo],
      order: String): Seq[EstimatedSummaryInfo] = {
    if (QualificationArgs.isOrderAsc(order)) {
      appsSumDesc.reverse.map(_.estimatedInfo)
    } else {
      appsSumDesc.map(_.estimatedInfo)
    }
  }

  private def qualifyApp(
      path: EventLogInfo,
      hadoopConf: Configuration): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      val app = QualificationAppInfo.createApp(path, hadoopConf, pluginTypeChecker)
      if (!app.isDefined) {
        logWarning(s"No Application found that contain SQL for ${path.eventLog.toString}!")
        None
      } else {
        val qualSumInfo = app.get.aggregateStats()
        if (qualSumInfo.isDefined) {
          allApps.add(qualSumInfo.get)
          val endTime = System.currentTimeMillis()
          logInfo(s"Took ${endTime - startTime}ms to process ${path.eventLog.toString}")
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

  /**
   * The outputPath of the current instance of the provider
   */
  def getReportOutputPath: String = {
    s"$outputDir/rapids_4_spark_qualification_output"
  }
}
