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

package com.nvidia.spark.rapids.tool.profiling

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.{EventLogInfo, EventLogPathProcessor}
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

  private val useAutoTuner: Boolean = appArgs.autoTuner()

  logInfo(s"Threadpool size is $nThreads")

  /**
   * Profiles application according to the mode requested. The main difference in processing for
   * the modes is which parts are done in parallel. All of them create the ApplicationInfo
   * by processing the event logs in parallel but after that it depends on the mode as to
   * what else we can do in parallel.
   */
  def profile(eventLogInfos: Seq[EventLogInfo]): Unit = {
    if (appArgs.compare()) {
      if (outputCombined) {
        logError("Output combined option not valid with compare mode!")
      } else {
        // create all the apps in parallel
        val apps = createApps(eventLogInfos)

        if (apps.size < 2) {
          logError("At least 2 applications are required for comparison mode. Exiting!")
        } else {
          val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/compare",
            Profiler.COMPARE_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
          try {
            // we need the info for all of the apps to be able to compare so this happens serially
            val (sums, comparedRes) = processApps(apps, printPlans = false, profileOutputWriter)
            writeOutput(profileOutputWriter, Seq(sums), false, comparedRes)
          }
          finally {
            profileOutputWriter.close()
          }
        }
      }
    } else if (outputCombined) {
      // same as collection but combine the output so all apps are in single tables
      // We can process all the apps in parallel and get the summary for them and then
      // combine them into single tables in the output.
      val profileOutputWriter = new ProfileOutputWriter(s"$outputDir/combined",
        Profiler.COMBINED_LOG_FILE_NAME_PREFIX, numOutputRows, outputCSV = outputCSV)
      val sums = createAppsAndSummarize(eventLogInfos, false, profileOutputWriter)
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

  private def errorHandler(error: Throwable, path: EventLogInfo) = {
    error match {
      case oom: OutOfMemoryError =>
        logError(s"OOM error while processing large file: ${path.eventLog.toString}." +
            s" Increase heap size. Exiting ...", oom)
        sys.exit(1)
      case NonFatal(e) =>
        logWarning(s"Exception occurred processing file: ${path.eventLog.getName}", e)
      case o: Throwable =>
        logError(s"Error occurred while processing file: ${path.eventLog.toString}. Exiting ...", o)
        sys.exit(1)
    }
  }

  private def createApps(allPaths: Seq[EventLogInfo]): Seq[ApplicationInfo] = {
    var errorCodes = ArrayBuffer[Int]()
    val allApps = new ConcurrentLinkedQueue[ApplicationInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        try {
          val appOpt = createApp(path, index, hadoopConf)
          appOpt.foreach(app => allApps.add(app))
        } catch {
          case t: Throwable => errorHandler(t, path)
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

  private def createAppsAndSummarize(allPaths: Seq[EventLogInfo],
      printPlans: Boolean,
      profileOutputWriter: ProfileOutputWriter): Seq[ApplicationSummaryInfo] = {
    var errorCodes = ArrayBuffer[Int]()
    val allApps = new ConcurrentLinkedQueue[ApplicationSummaryInfo]()

    class ProfileThread(path: EventLogInfo, index: Int) extends Runnable {
      def run: Unit = {
        try {
          val appOpt = createApp(path, index, hadoopConf)
          appOpt.foreach { app =>
            val sum = try {
              val (s, _) = processApps(Seq(app), false, profileOutputWriter)
              Some(s)
            } catch {
              case e: Exception =>
                logWarning(s"Unexpected exception thrown ${path.eventLog.toString}, skipping! ", e)
                None
            }
            sum.foreach(allApps.add(_))
          }
        } catch {
          case t: Throwable => errorHandler(t, path)
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
          // we just skip apps that don't process cleanly and exit if heap is smaller
          val appOpt = createApp(path, index, hadoopConf)
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
          case t: Throwable => errorHandler(t, path)
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

  private def createApp(path: EventLogInfo, index: Int,
      hadoopConf: Configuration): Option[ApplicationInfo] = {
    try {
      // This apps only contains 1 app in each loop.
      val startTime = System.currentTimeMillis()
      val app = new ApplicationInfo(hadoopConf, path, index)
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
   * Function to process ApplicationInfo. Collects all the application information
   * and returns the summary information. The summary information is much smaller than
   * the ApplicationInfo because it has processed and combined many of the raw events.
   */
  private def processApps(apps: Seq[ApplicationInfo], printPlans: Boolean,
      profileOutputWriter: ProfileOutputWriter): (ApplicationSummaryInfo,
    Option[CompareSummaryInfo]) = {

    val collect = new CollectInformation(apps)
    val appInfo = collect.getAppInfo
    val dsInfo = collect.getDataSourceInfo
    val execInfo = collect.getExecutorInfo
    val jobInfo = collect.getJobInfo
    val sqlStageInfo = collect.getSQLToStage
    val rapidsProps = collect.getProperties(rapidsOnly = true)
    val sparkProps = collect.getProperties(rapidsOnly = false)
    val rapidsJar = collect.getRapidsJARInfo
    val sqlMetrics = collect.getSQLPlanMetrics
    val wholeStage = collect.getWholeStageCodeGenMapping
    // for compare mode we just add in extra tables for matching across applications
    // the rest of the tables simply list all applications specified
    val compareRes = if (appArgs.compare()) {
      val compare = new CompareApplications(apps)
      val (matchingSqlIds, matchingStageIds) = compare.findMatchingStages()
      Some(CompareSummaryInfo(matchingSqlIds, matchingStageIds))
    } else {
      None
    }

    val analysis = new Analysis(apps)
    val jsMetAgg = analysis.jobAndStageMetricsAggregation()
    val sqlTaskAggMetrics = analysis.sqlMetricsAggregation()
    val durAndCpuMet = analysis.sqlMetricsAggregationDurationAndCpuTime()
    val skewInfo = analysis.shuffleSkewCheck()

    val healthCheck = new HealthCheck(apps)
    val failedTasks = healthCheck.getFailedTasks
    val failedStages = healthCheck.getFailedStages
    val failedJobs = healthCheck.getFailedJobs
    val removedBMs = healthCheck.getRemovedBlockManager
    val removedExecutors = healthCheck.getRemovedExecutors
    val unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan
   
    if (printPlans) {
      CollectInformation.printSQLPlans(apps, outputDir)
    }

    if (appArgs.generateDot()) {
      if (appArgs.compare() || appArgs.combined()) {
        logWarning("Dot graph does not compare or combine apps")
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
      if (appArgs.compare() || appArgs.combined()) {
        logWarning("Timeline graph does not compare or combine apps")
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
      failedJobs, removedBMs, removedExecutors, unsupportedOps, sparkProps, sqlStageInfo,
      wholeStage), compareRes)
  }

  def writeOutput(profileOutputWriter: ProfileOutputWriter,
      appsSum: Seq[ApplicationSummaryInfo], outputCombined: Boolean,
      comparedRes: Option[CompareSummaryInfo] = None): Unit = {

    val sums = if (outputCombined) {
      // the properties table here has the column names as the app indexes so we have to
      // handle special
      def combineProps(rapidsOnly: Boolean,
          sums: Seq[ApplicationSummaryInfo]): Seq[RapidsPropertyProfileResult] = {
        var numApps = 0
        val props = HashMap[String, ArrayBuffer[String]]()
        val outputHeaders = ArrayBuffer("propertyName")
        sums.foreach { app =>
          val inputProps = if (rapidsOnly) {
            app.rapidsProps
          } else {
            app.sparkProps
          }
          if (inputProps.nonEmpty) {
            numApps += 1
            val appMappedProps = inputProps.map { p =>
              p.rows(0) -> p.rows(1)
            }.toMap

            outputHeaders += inputProps.head.outputHeaders(1)
            CollectInformation.addNewProps(appMappedProps, props, numApps)
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
        combineProps(rapidsOnly=true, appsSum).sortBy(_.key),
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
        appsSum.flatMap(_.unsupportedOps).sortBy(_.appIndex),
        combineProps(rapidsOnly=false, appsSum).sortBy(_.key),
        appsSum.flatMap(_.sqlStageInfo).sortBy(_.duration)(Ordering[Option[Long]].reverse),
        appsSum.flatMap(_.wholeStage).sortBy(_.appIndex)
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
      profileOutputWriter.write("SQL to Stage Information", app.sqlStageInfo)
      profileOutputWriter.write("Spark Rapids parameters set explicitly", app.rapidsProps,
        Some("Spark Rapids parameters"))
      profileOutputWriter.write("Spark Properties", app.sparkProps,
        Some("Spark Properties"))
      profileOutputWriter.write("Rapids Accelerator Jar and cuDF Jar", app.rapidsJar,
        Some("Rapids 4 Spark Jars"))
      profileOutputWriter.write("SQL Plan Metrics for Application", app.sqlMetrics,
        Some("SQL Plan Metrics"))
      profileOutputWriter.write("WholeStageCodeGen Mapping", app.wholeStage,
        Some("WholeStagecodeGen Mapping"))
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

      if (useAutoTuner) {
        val workerInfo: String = appArgs.workerInfo.getOrElse(".")
        val autoTuner: AutoTuner = new AutoTuner(app, workerInfo)
        val (properties, comments) = autoTuner.getRecommendedProperties
        profileOutputWriter.writeText("\n### D. Recommended Configuration ###\n")

        if (properties.nonEmpty) {
          val propertiesToStr = properties.map(_.toString).reduce(_ + "\n" + _)
          profileOutputWriter.writeText("\nSpark Properties:\n" + propertiesToStr + "\n")
        } else {
          profileOutputWriter.writeText("No properties to recommend\n")
        }

        // Comments are optional
        if (comments.nonEmpty) {
          val commentsToStr = comments.map(_.toString).reduce(_ + "\n" + _)
          profileOutputWriter.writeText("\nComments:\n" + commentsToStr + "\n")
        }
      }
    }
  }
}

object Profiler {
  // This tool's output log file name
  val PROFILE_LOG_NAME = "profile"
  val COMPARE_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_compare"
  val COMBINED_LOG_FILE_NAME_PREFIX = "rapids_4_spark_tools_combined"
  val SUBDIR = "rapids_4_spark_profile"
}
