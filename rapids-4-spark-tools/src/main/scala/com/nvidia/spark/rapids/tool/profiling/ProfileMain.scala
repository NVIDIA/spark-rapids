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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.{_}

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

    // Create an Array of Applications(with an index starting from 1)
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    var index: Int = 1
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
      index += 1
    }
    require(apps.nonEmpty)

    // If only 1 Application, collect:
    // A. Information Collected
    val collect = new CollectInformation(apps)
    if (apps.size == 1) {
      logInfo(s"### A. Information Collected ###")
      collect.printAppInfo()
      collect.printExecutorInfo()
      collect.printRapidsProperties()
    } else {
      val compare = new CompareApplications(apps)
      // Compare Applications
      logInfo(s"### A. Compare Information Collected ###")
      compare.compareAppInfo()
      compare.compareExecutorInfo()
      compare.compareRapidsProperties()
    }

    for (app <- apps) {
      logInfo("========================================================================")
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
      logInfo("========================================================================")
    }
    fileWriter.flush()
    fileWriter.close()
  }
}
