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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.history.EventLogFileWriter
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

  val SUBDIR = "rapids_4_spark_qualification_profile"

  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/
  // core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L67
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES = Set("lz4", "lzf", "snappy", "zstd")

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

    // Create the FileWriter and sparkSession used for ALL Applications.
    val textFileWriter = new ToolTextFileWriter(outputDirectory, logFileName)

    try {
      // Get the event logs required to process
      lazy val allPaths = ToolUtils.processAllPaths(filterN, matchEventLogs, eventlogPaths)

      val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

      def eventLogNameFilter(logFile: Path): Boolean = {
        EventLogFileWriter.codecName(logFile)
          .forall(suffix => SPARK_SHORT_COMPRESSION_CODEC_NAMES.contains(suffix))
       }

      // If compare mode is on, we need lots of memory to cache all applications then compare.
      // Suggest only enable compare mode if there is no more than 10 applications as input.
      if (appArgs.compare()) {
        // Create an Array of Applications(with an index starting from 1)
        val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
        try {
          var index: Int = 1
          for (path <- allPaths.filter(eventLogNameFilter)) {
            apps += new ApplicationInfo(numOutputRows, sparkSession, path, index)
            index += 1
          }

          //Exit if there are no applications to process.
          if (apps.isEmpty) {
            logInfo("No application to process. Exiting")
            return 0
          }
          processApps(apps, generateDot = false, printPlans = false)
        } catch {
          case e: com.fasterxml.jackson.core.JsonParseException =>
            textFileWriter.close()
            logError(s"Error parsing JSON", e)
            return 1
        }
        // Show the application Id <-> appIndex mapping.
        for (app <- apps) {
          logApplicationInfo(app)
        }
      } else {
        // This mode is to process one application at one time.
        var index: Int = 1
        try {
          for (path <- allPaths.filter(eventLogNameFilter)) {
            // This apps only contains 1 app in each loop.
            val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
            val app = new ApplicationInfo(numOutputRows, sparkSession, path, index)
            apps += app
            logApplicationInfo(app)
            // This is a bit odd that we process apps individual right now due to
            // memory concerns. So the aggregation functions only aggregate single
            // application not across applications.
            processApps(apps, appArgs.generateDot(), appArgs.printPlans())
            app.dropAllTempViews()
            index += 1
          }
        } catch {
          case e: com.fasterxml.jackson.core.JsonParseException =>
            textFileWriter.close()
            logError(s"Error parsing JSON", e)
            return 1
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
    def processApps(apps: ArrayBuffer[ApplicationInfo], generateDot: Boolean,
        printPlans: Boolean): Unit = {
      if (appArgs.compare()) { // Compare Applications

        textFileWriter.write("### A. Compare Information Collected ###")
        val compare = new CompareApplications(apps, textFileWriter)
        compare.compareAppInfo()
        compare.compareExecutorInfo()
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
    }

    def logApplicationInfo(app: ApplicationInfo) = {
      logInfo("========================================================================")
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
      logInfo("========================================================================")
    }

    0
  }
}
