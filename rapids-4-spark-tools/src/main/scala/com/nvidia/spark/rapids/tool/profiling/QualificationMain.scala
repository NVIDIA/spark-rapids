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
import org.apache.spark.sql.SparkSession
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
    val logFileName = "rapids_4_spark_qualification.log"

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val eventLogDir = appArgs.eventlogDir
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")
    val csvLocation = appArgs.saveCsv.toOption

    // Create the FileWriter and sparkSession used for ALL Applications.
    val fileWriter = new FileWriter(s"$outputDirectory/$logFileName")
    logInfo(s"Output directory:  $outputDirectory")

    // TODO - temporary parsing of event logs, this will be replaced
    val allPaths = if (eventLogDir.isDefined) {
      val logDir = eventLogDir.get.get
      // TODO - do we need s3 options?
      val hadoopConf = new Configuration()
      val fs: FileSystem = new Path(logDir).getFileSystem(hadoopConf)
      // TODO - want to check permissions or other things?
      val updated = Option(fs.listStatus(new Path(logDir))).map(_.toSeq).getOrElse(Nil)
      updated.map(_.getPath)
    } else {
      // Convert the input path string to Path(s)
      val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
      for (pathString <- eventlogPaths) {
        val paths = ProfileUtils.stringToPath(pathString)
        if (paths.nonEmpty) {
          allPaths ++= paths
        }
      }
      allPaths
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
    fileWriter.write(s"### Qualification ###")
    new Qualification(apps, csvLocation)
    logInfo(s"Output log location:  $outputDirectory/$logFileName")

    apps.foreach( _.dropAllTempViews())
    fileWriter.flush()
    fileWriter.close()
    0
  }

  def logApplicationInfo(app: ApplicationInfo) = {
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }
}
