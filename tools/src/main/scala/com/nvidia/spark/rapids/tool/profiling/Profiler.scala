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


class Profiler(outputDir: String, numOutputRows: Int, hadoopConf: Configuration,
    textFileWriter: ToolTextFileWriter, appArgs: ProfileArgs) extends Logging {

  val nThreads = appArgs.numThreads.getOrElse(
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
  val timeout = appArgs.timeout.toOption
  val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

  val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("profileTool" + "-%d").build()
  val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]

  logInfo(s"Threadpool size is $nThreads")


  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {

    // If compare mode is on, we need lots of memory to cache all applications then compare.
    // Suggest only enable compare mode if there is no more than 10 applications as input.
    if (appArgs.compare()) {
      // create all the apps in parallel since we need the info for all of them to compare
      val apps = createApps(eventLogInfos)
      if (apps.isEmpty) {
        logInfo("No application to process. Exiting")
      } else {
        processApps(apps, printPlans = false)
        // Show the application Id <-> appIndex mapping.
        apps.foreach(EventLogPathProcessor.logApplicationInfo(_))
      }
    } else {
      var index: Int = 1
      // Read each application and process it separately to save memory.
      // Memory usage will be controlled by number of threads running.
      eventLogInfos.foreach { log =>
        createAppAndProcess(Seq(log), index)
        index += 1
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

  def createApps(
      allPaths: Seq[EventLogInfo]): Seq[ApplicationInfo] = {
    var errorCodes = ArrayBuffer[Int]()
    val allApps = new ConcurrentLinkedQueue[ApplicationInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        val (appOpt, error) = ApplicationInfo.createApp(path, numOutputRows, index, hadoopConf)
        // TODO - just swallowing errors for now
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

  def createAppAndProcess(
      allPaths: Seq[EventLogInfo],
      startIndex: Int = 1): Unit = {
    var index: Int = startIndex
    var errorCodes = ArrayBuffer[Int]()

    class ProfileProcessThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        val (apps, error) = ApplicationInfo.createApp(path, numOutputRows, index, hadoopConf)
        // TODO - just swallowing errors for now
        if (apps.isEmpty) {
          logInfo("No application to process. Exiting")
        } else {
          processApps(Seq(apps.get), appArgs.printPlans())
        }
      }
    }

    allPaths.foreach { path =>
      try {
        threadPool.submit(new ProfileProcessThread(path, index))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
  }


  /**
   * Function to process ApplicationInfo. If it is in compare mode, then all the eventlogs are
   * evaluated at once and the output is one row per application. Else each eventlog is parsed one
   * at a time.
   */
  def processApps(apps: Seq[ApplicationInfo], printPlans: Boolean): Unit = {

    textFileWriter.write("### A. Information Collected ###")
    val collect = new CollectInformation(apps, Some(textFileWriter), numOutputRows)
    collect.printAppInfo()
    collect.printDataSourceInfo()
    collect.printExecutorInfo()
    collect.printJobInfo()
    collect.printRapidsProperties()
    collect.printRapidsJAR()
    collect.printSQLPlanMetrics()

    if (printPlans) {
      collect.printSQLPlans(outputDir)
    }

    // for compare mode we just add in extra tables for matching across applications
    // the rest of the tables simply list all applications specified
    if (appArgs.compare()) {
      val compare = new CompareApplications(apps, Some(textFileWriter), numOutputRows)
      compare.findMatchingStages()
    }

    textFileWriter.write("\n### B. Analysis ###\n")
    val analysis = new Analysis(apps, Some(textFileWriter), numOutputRows)
    analysis.jobAndStageMetricsAggregation()
    analysis.sqlMetricsAggregation()
    analysis.sqlMetricsAggregationDurationAndCpuTime()
    analysis.shuffleSkewCheck()

    textFileWriter.write("\n### C. Health Check###\n")
    val healthCheck=new HealthCheck(apps, Some(textFileWriter), numOutputRows)
    healthCheck.listFailedTasks()
    healthCheck.listFailedStages()
    healthCheck.listFailedJobs()
    healthCheck.listRemovedBlockManager()
    healthCheck.listRemovedExecutors()
    healthCheck.listPossibleUnsupportedSQLPlan()


    if (appArgs.generateDot()) {
      if (appArgs.compare()) {
        logWarning("Dot graph does not compare apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateDot(app, outputDir)
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        textFileWriter.write(s"Generated DOT graphs for app ${app.appId} " +
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
        textFileWriter.write(s"Generated timeline graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }
  }


}
