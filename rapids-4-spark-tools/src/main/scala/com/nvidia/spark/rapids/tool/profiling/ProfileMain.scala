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

import java.io.FileWriter

import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

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

  /**
   * Entry point for tests
   */
  def mainInternal(sparkSession: SparkSession, appArgs: ProfileArgs): Int = {

    // This tool's output log file name
    val logFileName = "rapids_4_spark_tools_output.log"

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")

    // Create the FileWriter and sparkSession used for ALL Applications.
    val fileWriter = new FileWriter(s"$outputDirectory/$logFileName")
    logInfo(s"Output directory:  $outputDirectory")

    // Convert the input path string to Path(s)
    val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
    for (pathString <- eventlogPaths) {
      val paths = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPaths ++= paths
      }
    }
    val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

    // If compare mode is on, we need lots of memory to cache all applications then compare.
    // Suggest only enable compare mode if there is no more than 10 applications as input.
    if (appArgs.compare()) {
      // Create an Array of Applications(with an index starting from 1)
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      var index: Int = 1
      for (path <- allPaths.filter(p => !p.getName.contains("."))) {
        apps += new ApplicationInfo(numOutputRows, sparkSession, path, index)
        index += 1
      }

      //Exit if there are no applications to process.
      if (apps.isEmpty) {
        logInfo("No application to process. Exiting")
        return 0
      }
      processApps(apps, generateDot = false)
      // Show the application Id <-> appIndex mapping.
      for (app <- apps) {
        logApplicationInfo(app)
      }
    } else {
      // This mode is to process one application at one time.
      var index: Int = 1
      for (path <- allPaths.filter(p => !p.getName.contains("."))) {
        // This apps only contains 1 app in each loop.
        val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
        val app = new ApplicationInfo(numOutputRows, sparkSession, path, index)
        apps += app
        logApplicationInfo(app)
        // This is a bit odd that we process apps individual right now due to
        // memory concerns. So the aggregation functions only aggregate single
        // application not across applications.
        processApps(apps, appArgs.generateDot())
        app.dropAllTempViews()
        index += 1
      }
    }

    logInfo(s"Output log location:  $outputDirectory/$logFileName")

    fileWriter.flush()
    fileWriter.close()

    /**
     * Function to process ApplicationInfo. If it is in compare mode, then all the eventlogs are
     * evaluated at once and the output is one row per application. Else each eventlog is parsed one
     * at a time.
     */
    def processApps(apps: ArrayBuffer[ApplicationInfo], generateDot: Boolean): Unit = {
      if (appArgs.compare()) { // Compare Applications
        fileWriter.write("### A. Compare Information Collected ###")
        val compare = new CompareApplications(apps, fileWriter)
        compare.compareAppInfo()
        compare.compareExecutorInfo()
        compare.compareRapidsProperties()
      } else {
        val collect = new CollectInformation(apps, fileWriter)
        fileWriter.write("### A. Information Collected ###")
        collect.printAppInfo()
        collect.printExecutorInfo()
        collect.printRapidsProperties()
        collect.printRapidsJAR()
        collect.printSQLPlanMetrics(generateDot, appArgs.outputDirectory())
      }

      fileWriter.write("\n### B. Analysis ###\n")
      val analysis = new Analysis(apps, Some(fileWriter))
      analysis.jobAndStageMetricsAggregation()
      analysis.sqlMetricsAggregation()
      analysis.shuffleSkewCheck()
    }

    def logApplicationInfo(app: ApplicationInfo) = {
      logInfo("========================================================================")
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
      logInfo("========================================================================")
    }

    0
  }
}
