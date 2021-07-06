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

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._

/**
 * Scores the applications for GPU acceleration and outputs the
 * reports.
 */
class Qualification(outputDir: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int) extends Logging {

  private val allApps = new ConcurrentLinkedQueue[QualificationSummaryInfo]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("qualTool" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = qualifyApp(path, numRows, hadoopConf)
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
    val sorted = allAppsSum.sortBy(sum => (-sum.score, -sum.sqlDataFrameDuration, -sum.appDuration))
    val qWriter = new QualOutputWriter(outputDir, numRows)
    qWriter.writeCSV(sorted)
    qWriter.writeReport(sorted)
    sorted
  }

  private def qualifyApp(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      val app = QualAppInfo.createApp(path, numRows, hadoopConf)
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
      case e: Exception =>
        logError(s"Unexpected exception processing log ${path.eventLog.toString}, skipping!", e)
    }
  }
}
