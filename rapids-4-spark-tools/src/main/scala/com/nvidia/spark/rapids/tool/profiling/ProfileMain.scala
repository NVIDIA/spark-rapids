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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * A profiling tool to parse Spark Event Log
 * This is the Main function.
 */
object ProfileMain extends Logging {
  def main(args: Array[String]) {

    // This tool's output log file name
    val logFileName = "rapids_4_spark_tools_output.log"

    // Parsing args
    val appArgs = new ProfileArgs(args)
    val eventlogPaths = appArgs.eventlog()
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")

    // Create the FileWriter and sparkSession used for ALL Applications.
    val fileWriter = new FileWriter(s"$outputDirectory/$logFileName")
    val sparkSession = ProfileUtils.createSparkSession
    logInfo(s"Output directory:  $outputDirectory")

    // Convert the input path string to Path(s)
    val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
    for (pathString <- eventlogPaths) {
      val paths = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPaths ++= paths
      }
    }

    // If compare mode is on, we need lots of memory to cache all applications then compare.
    // Suggest only enable compare mode if there is no more than 10 applications as input.
    if (appArgs.compare()) {
      // Create an Array of Applications(with an index starting from 1)
      val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      var index: Int = 1
      for (path <- allPaths.filter(p => !p.getName.contains("."))) {
        apps += new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
        index += 1
      }

      //Exit if there are no applications to process.
      if (apps.isEmpty) {
        logInfo("No application to process. Exiting")
        System.exit(0)
      }
      processApps(apps)
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
        val app = new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
        apps += app
        logApplicationInfo(app)
        processApps(apps)
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
    def processApps(apps: ArrayBuffer[ApplicationInfo]): Unit = {
      if (appArgs.compare()) { // Compare Applications
        logInfo(s"### A. Compare Information Collected ###")
        val compare = new CompareApplications(apps)
        compare.compareAppInfo()
        compare.compareExecutorInfo()
        compare.compareRapidsProperties()
      } else {
        val collect = new CollectInformation(apps)
        logInfo(s"### A. Information Collected ###")
        collect.printAppInfo()
        collect.printExecutorInfo()
        collect.printRapidsProperties()
      }

      logInfo(s"### B. Analysis ###")
      val analysis = new Analysis(apps)
      analysis.jobAndStageMetricsAggregation()
      val sqlAggMetricsDF = analysis.sqlMetricsAggregation()

      if (!sqlAggMetricsDF.isEmpty) {
        fileWriter.write(s"### C. Qualification ###\n")
        new Qualification(apps, sqlAggMetricsDF)
      } else {
        logInfo(s"Skip qualification part because no sqlAggMetrics DataFrame is detected.")
      }
    }

    def logApplicationInfo(app: ApplicationInfo) = {
      logInfo("========================================================================")
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
      logInfo("========================================================================")
    }
  }
}