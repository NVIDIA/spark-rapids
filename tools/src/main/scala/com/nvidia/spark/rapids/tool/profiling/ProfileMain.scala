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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTextFileWriter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * A profiling tool to parse Spark Event Log
 */
object ProfileMain extends Logging {
  /**
   * Entry point from spark-submit running this as the driver.
   */
  def main(args: Array[String]) {
    val sparkSession = ProfileUtils.createSparkSession
    val exitCode = mainInternal(sparkSession, new ProfileArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  val SUBDIR = "rapids_4_spark_profile"

  /**
   * Entry point for tests
   */
  def mainInternal(sparkSession: SparkSession, appArgs: ProfileArgs): Int = {

    // This tool's output log file name
    val logFileName = "rapids_4_spark_tools_output.log"

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/") +
      s"/$SUBDIR"
    val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

    // Create the FileWriter and sparkSession used for ALL Applications.
    val textFileWriter = new ToolTextFileWriter(outputDirectory, logFileName)
    try {
      // Get the event logs required to process
      val eventLogInfos = EventLogPathProcessor.processAllPaths(filterN.toOption,
        matchEventLogs.toOption, eventlogPaths, sparkSession.sparkContext.hadoopConfiguration)
      if (eventLogInfos.isEmpty) {
        logWarning("No event logs to process after checking paths, exiting!")
        return 0
      }

      // If compare mode is on, we need lots of memory to cache all applications then compare.
      // Suggest only enable compare mode if there is no more than 10 applications as input.
      if (appArgs.compare()) {
        // Create an Array of Applications(with an index starting from 1)
        val (apps, errorCode) = ApplicationInfo.createApps(eventLogInfos,
          numOutputRows, sparkSession)
        if (errorCode > 0) {
          logError(s"Error parsing one of the event logs")
          return 1
        }
        if (apps.isEmpty) {
          logInfo("No application to process. Exiting")
          return 0
        }
        processApps(apps, generateDot = false, printPlans = false)
        // Show the application Id <-> appIndex mapping.
        apps.foreach(EventLogPathProcessor.logApplicationInfo(_))
      } else {
        var index: Int = 1
        eventLogInfos.foreach { log =>
          // Only process 1 app at a time.
          val (apps, errorCode) = ApplicationInfo.createApps(ArrayBuffer(log), numOutputRows,
            sparkSession, startIndex = index)
          index += 1
          if (errorCode > 0) {
            logError(s"Error parsing ${log.eventLog}")
            return 1
          }
          if (apps.isEmpty) {
            logInfo("No application to process. Exiting")
            return 0
          }
          // This is a bit odd that we process apps individual right now due to
          // memory concerns. So the aggregation functions only aggregate single
          // application not across applications.
          processApps(apps, appArgs.generateDot(), appArgs.printPlans())
          apps.foreach(_.dropAllTempViews())
        }
      }
    } finally {
      textFileWriter.close()
    }

    /**
     * Function to process ApplicationInfo. If it is in compare mode, then all the eventlogs are
     * evaluated at once and the output is one row per application. Else each eventlog is parsed one
     * at a time.
     */
    def processApps(apps: Seq[ApplicationInfo], generateDot: Boolean, printPlans: Boolean): Unit = {
      if (appArgs.compare()) { // Compare Applications

        textFileWriter.write("### A. Compare Information Collected ###")
        val compare = new CompareApplications(apps, Some(textFileWriter))
        compare.compareAppInfo()
        compare.compareExecutorInfo()
        compare.compareJobInfo()
        compare.compareRapidsProperties()
      } else {
        val collect = new CollectInformation(apps, Some(textFileWriter))
        textFileWriter.write("### A. Information Collected ###")
        collect.printAppInfo()
        collect.printExecutorInfo()
        collect.printJobInfo()
        collect.printRapidsProperties()
        collect.printRapidsJAR()
        collect.printSQLPlanMetrics(generateDot, outputDirectory)
        if (printPlans) {
          collect.printSQLPlans(outputDirectory)
        }
      }

      textFileWriter.write("\n### B. Analysis ###\n")
      val analysis = new Analysis(apps, Some(textFileWriter))
      analysis.jobAndStageMetricsAggregation()
      val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
      sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")
      analysis.sqlMetricsAggregationDurationAndCpuTime()
      analysis.shuffleSkewCheck()

      textFileWriter.write("\n### C. Health Check###\n")
      val healthCheck=new HealthCheck(apps, textFileWriter)
      healthCheck.listFailedJobsStagesTasks()
      healthCheck.listRemovedBlockManager()
      healthCheck.listRemovedExecutors()
      healthCheck.listPossibleUnsupportedSQLPlan()

      if (appArgs.generateTimeline()) {
        if (appArgs.compare()) {
          logWarning("Timeline graph does not compare apps")
        }
        apps.foreach { app =>
          GenerateTimeline.generateFor(app, outputDirectory)
        }
      }
    }

    0
  }
}
