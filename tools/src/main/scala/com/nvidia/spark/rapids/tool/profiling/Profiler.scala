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

package com.nvidia.spark.rapids.tool.profiling

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.{EventLogInfo, EventLogPathProcessor, ToolTextFileWriter}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

class Profiler(hadoopConf: Configuration, appArgs: ProfileArgs) extends Logging {

  private val nThreads = appArgs.numThreads.getOrElse(
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
  private val timeout = appArgs.timeout.toOption
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("profileTool" + "-%d").build()
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]
  private val outputDir = appArgs.outputDirectory().stripSuffix("/") +
    s"/${Profiler.SUBDIR}"
  private val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

  private val outputCSV: Boolean = appArgs.csv()

  logInfo(s"Threadpool size is $nThreads")

  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {
    if (appArgs.compare()) {
      val profileOutputWriter = new ProfileOutputWriter(outputDir,
        Profiler.COMPARE_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
      try {
        // create all the apps in parallel since we need the info for all of them to compare
        val apps = createApps(eventLogInfos)
        if (apps.size < 2) {
          logInfo("At least 2 applications are required for comparison mode. Exiting!")
        } else {
          processApps(apps, printPlans = false, profileOutputWriter)
        }
      } finally {
        profileOutputWriter.close()
      }
    } else {
      // Read each application and process it separately to save memory.
      // Memory usage will be controlled by number of threads running.
      // use appIndex as 1 for all since we output separate file for each now
      eventLogInfos.foreach { log =>
        createAppAndProcess(Seq(log), 1)
      }
      // wait for the threads to finish processing the files
      threadPool.shutdown()
      if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
        logError(s"Processing log files took longer then $waitTimeInSec seconds," +
          " stopping processing any more event logs")
        threadPool.shutdownNow()
      }
    }
  }

  private def createApps(allPaths: Seq[EventLogInfo]): Seq[ApplicationInfo] = {
    var errorCodes = ArrayBuffer[Int]()
    val allApps = new ConcurrentLinkedQueue[ApplicationInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        val appOpt = createApp(path, numOutputRows, index, hadoopConf)
        appOpt.foreach(app => allApps.add(app))
      }
    }

    var appIndex = 1
    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileThread(path, appIndex))
        appIndex += 1
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
    allApps.asScala.toSeq
  }

  private def createAppAndProcess(
      allPaths: Seq[EventLogInfo],
      startIndex: Int = 1): Unit = {
    class ProfileProcessThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        try {
          // we just skip apps that don't process cleanly
          val appOpt = createApp(path, numOutputRows, index, hadoopConf)
          appOpt match {
            case Some(app) =>
              val profileOutputWriter = new ProfileOutputWriter(outputDir,
                s"${app.appId}-${Profiler.PROFILE_LOG_NAME}", numOutputRows,
                outputCSV = outputCSV)
              try {
                processApps(Seq(appOpt.get), appArgs.printPlans(), profileOutputWriter)
              } finally {
                profileOutputWriter.close()
              }
            case None =>
              logInfo("No application to process. Exiting")
          }
        } catch {
          case e: Exception =>
            logWarning(s"Exception occurred processing file: ${path.eventLog.getName}", e)
        }
      }
    }

    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileProcessThread(path, startIndex))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
  }

  private def createApp(path: EventLogInfo, numRows: Int, index: Int,
      hadoopConf: Configuration): Option[ApplicationInfo] = {
    try {
      // This apps only contains 1 app in each loop.
      val startTime = System.currentTimeMillis()
      val app = new ApplicationInfo(numRows, hadoopConf, path, index)
      EventLogPathProcessor.logApplicationInfo(app)
      val endTime = System.currentTimeMillis()
      logInfo(s"Took ${endTime - startTime}ms to process ${path.eventLog.toString}")
      Some(app)
    } catch {
      case json: com.fasterxml.jackson.core.JsonParseException =>
        logWarning(s"Error parsing JSON: $path")
        None
      case il: IllegalArgumentException =>
        logWarning(s"Error parsing file: $path", il)
        None
      case e: Exception =>
        // catch all exceptions and skip that file
        logWarning(s"Got unexpected exception processing file: $path", e)
        None
    }
  }

  /**
   * Function to process ApplicationInfo. If it is in compare mode, then all the eventlogs are
   * evaluated at once and the output is one row per application. Else each eventlog is parsed one
   * at a time.
   */
  private def processApps(apps: Seq[ApplicationInfo], printPlans: Boolean,
      profileOutputWriter: ProfileOutputWriter): Unit = {

    profileOutputWriter.writeText("### A. Information Collected ###")
    val collect = new CollectInformation(apps)
    val appInfo = collect.getAppInfo
    profileOutputWriter.write("Application Information", appInfo,
      "No Application Information Found!")

    val dsInfo = collect.getDataSourceInfo
    profileOutputWriter.write("Data Source Information", dsInfo,
      "No Data Source Information Found!")

    val execInfo = collect.getExecutorInfo
    profileOutputWriter.write("Executor Information", execInfo,
      "No Executor Information Found!")

    val jobInfo = collect.getJobInfo
    profileOutputWriter.write("Job Information", jobInfo,
      "No Job Information Found!")

    val rapidsProps = collect.getRapidsProperties
    profileOutputWriter.write("Spark Rapids parameters set explicitly", rapidsProps,
      "No Job Information Found!")

    val rapidsJar = collect.getRapidsJARInfo
    profileOutputWriter.write("Rapids Accelerator Jar and cuDF Jar", rapidsJar,
      "No Spark Rapids parameters Found!")

    val sqlMetrics = collect.getSQLPlanMetrics
    profileOutputWriter.write("SQL Plan Metrics for Application", rapidsJar,
      "No SQL Plan Metrics Found!")

    if (printPlans) {
      collect.printSQLPlans(outputDir)
    }

    // for compare mode we just add in extra tables for matching across applications
    // the rest of the tables simply list all applications specified
    if (appArgs.compare()) {
      val compare = new CompareApplications(apps)
      val (matchingSqlIds, matchingStageIds) = compare.findMatchingStages()
      profileOutputWriter.write("Matching SQL IDs Across Applications", matchingSqlIds,
        "Not able to find Matching SQL IDs Across Applications!")
      profileOutputWriter.write("Matching Stage IDs Across Applications", matchingStageIds,
        "Not able to find Matching Stage IDs Across Applications!")
    }

    profileOutputWriter.writeText("\n### B. Analysis ###\n")
    val analysis = new Analysis(apps)
    val jsMetAgg = analysis.jobAndStageMetricsAggregation()
    profileOutputWriter.write("Job + Stage level aggregated task metrics", jsMetAgg,
      "No Job/Stage Metrics Found")

    val sqlTaskAggMetrics = analysis.sqlMetricsAggregation()
    profileOutputWriter.write("SQL level aggregated task metrics", sqlTaskAggMetrics,
      "No SQL Metrics Found")
    val durAndCpuMet = analysis.sqlMetricsAggregationDurationAndCpuTime()
    profileOutputWriter.write("SQL Duration and Executor CPU Time Percent", durAndCpuMet,
      "No SQL Duration and Executor CPU Time Percent Found")
    val skewInfo = analysis.shuffleSkewCheck()
    // TODO - to long for file name
    val skewHeader = "Shuffle Skew Check" // +
    // " (When task's Shuffle Read Size > 3 * Avg Stage-level size)"
    profileOutputWriter.write(skewHeader, skewInfo,
      "No SQL Duration and Executor CPU Time Percent Found")

    profileOutputWriter.writeText("\n### C. Health Check###\n")
    val healthCheck = new HealthCheck(apps)
    val failedTasks = healthCheck.getFailedTasks
    profileOutputWriter.write("Failed tasks", failedTasks,
      "No Failed tasks Found")

    val failedStages = healthCheck.getFailedStages
    profileOutputWriter.write("Failed stages", failedStages,
      "No Failed stages Found")

    val failedJobs = healthCheck.getFailedJobs
    profileOutputWriter.write("Failed jobs", failedJobs,
      "No Failed jobs Found")

    val removedBMs = healthCheck.getRemovedBlockManager
    profileOutputWriter.write("Removed BlockManagers", removedBMs,
      "No Removed BlockManagers Found")
    val removedExecutors = healthCheck.getRemovedExecutors
    profileOutputWriter.write("Removed Executors", removedExecutors,
      "No Removed Executors Found")

    val unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan
    profileOutputWriter.write("Unsupported SQL Plan", unsupportedOps,
      "No Unsupported SQL Ops Found")

    if (appArgs.generateDot()) {
      if (appArgs.compare()) {
        logWarning("Dot graph does not compare apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateDot(app, outputDir)
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated DOT graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }

    if (appArgs.generateTimeline()) {
      if (appArgs.compare()) {
        logWarning("Timeline graph does not compare apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateTimeline.generateFor(app, outputDir)
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated timeline graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }
  }
}

object Profiler {
  // This tool's output log file name
  val PROFILE_LOG_NAME = "profile"
  val COMPARE_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_compare"
  val SUBDIR = "rapids_4_spark_profile"
}
