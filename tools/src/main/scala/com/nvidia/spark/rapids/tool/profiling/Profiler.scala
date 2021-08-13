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
import scala.collection.mutable.{ArrayBuffer, HashMap}

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
  private val outputCombined: Boolean = appArgs.combined()

  logInfo(s"Threadpool size is $nThreads")

  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {
    if (appArgs.compare()) {
      if (outputCombined) {
        logError("Output combined option not valid with compare mode!")
      } else {
        val apps = createApps(eventLogInfos)

        if (apps.size < 2) {
          logError("At least 2 applications are required for comparison mode. Exiting!")
        } else {
          val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/compare",
            Profiler.COMPARE_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
          try {
            // create all the apps in parallel since we need the info for all of them to compare
            val (sums, comparedRes) = processApps(apps, printPlans = false, profileOutputWriter)
            writeOutput(profileOutputWriter, Seq(sums), false)
          }
          finally {
            profileOutputWriter.close()
          }
        }
      }
    } else if (outputCombined) {
      // same as collection but combine the output so all apps are in single tables
      val sums = createAppsAndSummarize(eventLogInfos, false)
      val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/combined",
        Profiler.COMPARE_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
      try {
        writeOutput(profileOutputWriter, sums, outputCombined)
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

  private def createAppsAndSummarize(allPaths: Seq[EventLogInfo],
      printPlans: Boolean): Seq[ApplicationSummaryInfo] = {
    var errorCodes = ArrayBuffer[Int]()
    val allApps = new ConcurrentLinkedQueue[ApplicationSummaryInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        val appOpt = createApp(path, numOutputRows, index, hadoopConf)
        appOpt.foreach { app =>
          val (sum, _) = processApps(Seq(app), false, null)
          allApps.add(sum)
        }
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
              val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/${app.appId}",
                Profiler.PROFILE_LOG_NAME, numOutputRows,
                outputCSV = outputCSV)
              try {
                val (sum, _) =
                  processApps(Seq(appOpt.get), appArgs.printPlans(), profileOutputWriter)
                writeOutput(profileOutputWriter, Seq(sum), false)
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
      profileOutputWriter: ProfileOutputWriter): (ApplicationSummaryInfo,
    Option[CompareSummaryInfo]) = {

    // profileOutputWriter.writeText("### A. Information Collected ###")
    val collect = new CollectInformation(apps)
    val appInfo = collect.getAppInfo
    // profileOutputWriter.write("Application Information", appInfo)

    val dsInfo = collect.getDataSourceInfo
    // profileOutputWriter.write("Data Source Information", dsInfo)

    val execInfo = collect.getExecutorInfo
    // profileOutputWriter.write("Executor Information", execInfo)

    val jobInfo = collect.getJobInfo
    // profileOutputWriter.write("Job Information", jobInfo)

    val rapidsProps = collect.getRapidsProperties
    // profileOutputWriter.write("Spark Rapids parameters set explicitly", rapidsProps,
    //   Some("Spark Rapids parameters"))

    val rapidsJar = collect.getRapidsJARInfo
    // profileOutputWriter.write("Rapids Accelerator Jar and cuDF Jar", rapidsJar,
    //   Some("Rapids 4 Spark Jars"))

    val sqlMetrics = collect.getSQLPlanMetrics
    // profileOutputWriter.write("SQL Plan Metrics for Application", sqlMetrics,
    //   Some("SQL Plan Metrics"))

    // for compare mode we just add in extra tables for matching across applications
    // the rest of the tables simply list all applications specified
    val compareRes = if (appArgs.compare()) {
      val compare = new CompareApplications(apps)
      val (matchingSqlIds, matchingStageIds) = compare.findMatchingStages()
      // profileOutputWriter.write("Matching SQL IDs Across Applications", matchingSqlIds)
      // profileOutputWriter.write("Matching Stage IDs Across Applications", matchingStageIds)
      Some(CompareSummaryInfo(matchingSqlIds, matchingStageIds))
    } else {
      None
    }

    // profileOutputWriter.writeText("\n### B. Analysis ###\n")
    val analysis = new Analysis(apps)
    val jsMetAgg = analysis.jobAndStageMetricsAggregation()
    // profileOutputWriter.write("Job + Stage level aggregated task metrics", jsMetAgg,
    //   Some("Job/Stage Metrics"))

    val sqlTaskAggMetrics = analysis.sqlMetricsAggregation()
    // profileOutputWriter.write("SQL level aggregated task metrics", sqlTaskAggMetrics,
    //   Some("SQL Metrics"))
    val durAndCpuMet = analysis.sqlMetricsAggregationDurationAndCpuTime()
    // profileOutputWriter.write("SQL Duration and Executor CPU Time Percent", durAndCpuMet)
    val skewInfo = analysis.shuffleSkewCheck()
    val skewHeader = "Shuffle Skew Check" // +
    val skewTableDesc = "(When task's Shuffle Read Size > 3 * Avg Stage-level size)"
    // profileOutputWriter.write(skewHeader, skewInfo, tableDesc = Some(skewTableDesc))

    // profileOutputWriter.writeText("\n### C. Health Check###\n")
    val healthCheck = new HealthCheck(apps)
    val failedTasks = healthCheck.getFailedTasks
    // profileOutputWriter.write("Failed Tasks", failedTasks)

    val failedStages = healthCheck.getFailedStages
    // profileOutputWriter.write("Failed Stages", failedStages)

    val failedJobs = healthCheck.getFailedJobs
    // profileOutputWriter.write("Failed Jobs", failedJobs)

    val removedBMs = healthCheck.getRemovedBlockManager
    // profileOutputWriter.write("Removed BlockManagers", removedBMs)
    val removedExecutors = healthCheck.getRemovedExecutors
    // profileOutputWriter.write("Removed Executors", removedExecutors)

    val unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan
    // profileOutputWriter.write("Unsupported SQL Plan", unsupportedOps,
    //   Some("Unsupported SQL Ops"))

    if (printPlans) {
      CollectInformation.printSQLPlans(apps, outputDir)
    }

    if (appArgs.generateDot()) {
      if (appArgs.compare()) {
        logWarning("Dot graph does not compare apps")
      }
      apps.foreach { app =>
        val start = System.nanoTime()
        GenerateDot(app, s"$outputDir/${app.appId}")
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
        GenerateTimeline.generateFor(app, s"$outputDir/${app.appId}")
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        profileOutputWriter.writeText(s"Generated timeline graphs for app ${app.appId} " +
          s"to $outputDir in $duration second(s)\n")
      }
    }
    (ApplicationSummaryInfo(appInfo, dsInfo, execInfo, jobInfo, rapidsProps, rapidsJar,
      sqlMetrics, jsMetAgg, sqlTaskAggMetrics, durAndCpuMet, skewInfo, failedTasks, failedStages,
      failedJobs, removedBMs, removedExecutors, unsupportedOps), compareRes)
  }

  def writeOutput(profileOutputWriter: ProfileOutputWriter,
      appsSum: Seq[ApplicationSummaryInfo], outputCombined: Boolean,
      comparedRes: Option[CompareSummaryInfo] = None): Unit = {

    val sums = if (outputCombined) {
      def combineProps(sums: Seq[ApplicationSummaryInfo]): Seq[RapidsPropertyProfileResult] = {
        var numApps = 0
        val props = HashMap[String, ArrayBuffer[String]]()
        val outputHeaders = ArrayBuffer("propertyName")
        sums.foreach { app =>
          if (app.rapidsProps.nonEmpty) {
            numApps += 1
            val rapidsRelated = app.rapidsProps.map { rp =>
              rp.rows(0) -> rp.rows(1)
            }.toMap

            outputHeaders += app.rapidsProps.head.outputHeaders(1)
            logWarning("outputheaders are: " + outputHeaders.mkString(","))
            val inter = props.keys.toSeq.intersect(rapidsRelated.keys.toSeq)
            val existDiff = props.keys.toSeq.diff(inter)
            val newDiff = rapidsRelated.keys.toSeq.diff(inter)

            // first update intersecting
            inter.foreach { k =>
              val appVals = props.getOrElse(k, ArrayBuffer[String]())
              appVals += rapidsRelated.getOrElse(k, "null")
            }

            // this app doesn't contain a key that was in another app
            existDiff.foreach { k =>
              val appVals = props.getOrElse(k, ArrayBuffer[String]())
              appVals += "null"
            }

            // this app contains a key not in other apps
            newDiff.foreach { k =>
              // we need to fill if some apps didn't have it
              val appVals = ArrayBuffer[String]()
              appVals ++= Seq.fill(numApps - 1)("null")
              appVals += rapidsRelated.getOrElse(k, "null")

              props.put(k, appVals)
            }
          }
        }
        val allRows = props.map { case (k, v) => Seq(k) ++ v }.toSeq
        val resRows = allRows.map(r => RapidsPropertyProfileResult(r(0), outputHeaders, r))
        resRows.sortBy(cols => cols.key)
      }

      val sorted = appsSum.sortBy( x => x.appInfo.head.appIndex)
      val reduced = ApplicationSummaryInfo(
        appsSum.flatMap(_.appInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.dsInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.execInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.jobInfo).sortBy(_.appIndex),
        combineProps(appsSum).sortBy(_.key),
        appsSum.flatMap(_.rapidsJar).sortBy(_.appIndex),
        appsSum.flatMap(_.sqlMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.jsMetAgg).sortBy(_.appIndex),
        appsSum.flatMap(_.sqlTaskAggMetrics).sortBy(_.appIndex),
        appsSum.flatMap(_.durAndCpuMet).sortBy(_.appIndex),
        appsSum.flatMap(_.skewInfo).sortBy(_.appIndex),
        appsSum.flatMap(_.failedTasks).sortBy(_.appIndex),
        appsSum.flatMap(_.failedStages).sortBy(_.appIndex),
        appsSum.flatMap(_.failedJobs).sortBy(_.appIndex),
        appsSum.flatMap(_.removedBMs).sortBy(_.appIndex),
        appsSum.flatMap(_.removedExecutors).sortBy(_.appIndex),
        appsSum.flatMap(_.unsupportedOps).sortBy(_.appIndex)
      )
      Seq(reduced)
    } else {
      appsSum
    }
    sums.foreach { app =>
      profileOutputWriter.writeText("### A. Information Collected ###")
      profileOutputWriter.write("Application Information", app.appInfo)
      profileOutputWriter.write("Data Source Information", app.dsInfo)
      profileOutputWriter.write("Executor Information", app.execInfo)
      profileOutputWriter.write("Job Information", app.jobInfo)
      profileOutputWriter.write("Spark Rapids parameters set explicitly", app.rapidsProps,
        Some("Spark Rapids parameters"))
      profileOutputWriter.write("Rapids Accelerator Jar and cuDF Jar", app.rapidsJar,
        Some("Rapids 4 Spark Jars"))
      profileOutputWriter.write("SQL Plan Metrics for Application", app.sqlMetrics,
        Some("SQL Plan Metrics"))
      comparedRes.foreach { compareSum =>
        val matchingSqlIds = compareSum.matchingSqlIds
        val matchingStageIds = compareSum.matchingStageIds
        profileOutputWriter.write("Matching SQL IDs Across Applications", matchingSqlIds)
        profileOutputWriter.write("Matching Stage IDs Across Applications", matchingStageIds)
      }

      profileOutputWriter.writeText("\n### B. Analysis ###\n")
      profileOutputWriter.write("Job + Stage level aggregated task metrics", app.jsMetAgg,
        Some("Job/Stage Metrics"))

      profileOutputWriter.write("SQL level aggregated task metrics", app.sqlTaskAggMetrics,
        Some("SQL Metrics"))
      profileOutputWriter.write("SQL Duration and Executor CPU Time Percent", app.durAndCpuMet)
      val skewHeader = "Shuffle Skew Check" // +
    val skewTableDesc = "(When task's Shuffle Read Size > 3 * Avg Stage-level size)"
      profileOutputWriter.write(skewHeader, app.skewInfo, tableDesc = Some(skewTableDesc))

      profileOutputWriter.writeText("\n### C. Health Check###\n")
      profileOutputWriter.write("Failed Tasks", app.failedTasks)
      profileOutputWriter.write("Failed Stages", app.failedStages)

      profileOutputWriter.write("Failed Jobs", app.failedJobs)

      profileOutputWriter.write("Removed BlockManagers", app.removedBMs)
      profileOutputWriter.write("Removed Executors", app.removedExecutors)

      profileOutputWriter.write("Unsupported SQL Plan", app.unsupportedOps,
        Some("Unsupported SQL Ops"))
    }

  }
}

object Profiler {
  // This tool's output log file name
  val PROFILE_LOG_NAME = "profile"
  val COMPARE_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_compare"
  val SUBDIR = "rapids_4_spark_profile"
}
