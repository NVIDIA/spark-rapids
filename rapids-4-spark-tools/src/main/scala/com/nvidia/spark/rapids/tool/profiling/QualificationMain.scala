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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * A tool to analyze Spark event logs and determine if 
 * they might be a good fit for running on the GPU.
 */
object QualificationMain extends Logging {
  /**
   * Entry point from spark-submit running this as the driver.
   */
  def main(args: Array[String]) {
    val sparkSession = ProfileUtils.createSparkSession
    val (exitCode, optDf) = mainInternal(sparkSession, new ProfileArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(sparkSession: SparkSession, appArgs: ProfileArgs,
      writeOutput: Boolean = true): (Int, Option[DataFrame]) = {

    // This tool's output log file name
    val logFileName = "rapids_4_spark_qualification.log"

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")

    // Create the FileWriter and sparkSession used for ALL Applications.
    val fileWriter = new FileWriter(s"$outputDirectory/$logFileName")

    // Convert the input path string to Path(s)
    val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
    for (pathString <- eventlogPaths) {
      val paths = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPaths ++= paths
      }
    }

    var index: Int = 1
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    for (path <- allPaths.filter(p => !p.getName.contains("."))) {
      // This apps only contains 1 app in each loop.
      val app = new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index, true)
      apps += app
      logApplicationInfo(app)
      index += 1
    }
    val analysis = new Analysis(apps)
    val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
    sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")
    fileWriter.write(s"### Qualification ###")
    val df = Qualification.qualifyApps(apps)
    if (writeOutput) {
      Qualification.writeQualification(apps, df)
    }

    logInfo(s"Output log location:  $outputDirectory/$logFileName")

    apps.foreach( _.dropAllTempViews())
    fileWriter.flush()
    fileWriter.close()
    (0, Some(df))
  }

  def logApplicationInfo(app: ApplicationInfo) = {
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }
}
