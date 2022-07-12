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

package com.nvidia.spark.rapids.tool

import java.io.{File, FilenameFilter, FileNotFoundException}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling.ProfileArgs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.types._

object ToolTestUtils extends Logging {

  def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  def runAndCollect(appName: String)
    (fun: SparkSession => DataFrame): String = {

    // we need to close any existing sessions to ensure that we can
    // create a session with a new event log dir
    TrampolineUtil.cleanupAnyExistingSession()

    lazy val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    // execute the query and generate events
    val df = fun(spark)
    df.collect()

    val appId = spark.sparkContext.applicationId

    // close the event log
    spark.close()
    appId
  }

  def generateEventLog(eventLogDir: File, appName: String)
      (fun: SparkSession => DataFrame): (String, String) = {

    // we need to close any existing sessions to ensure that we can
    // create a session with a new event log dir
    TrampolineUtil.cleanupAnyExistingSession()

    lazy val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", eventLogDir.getAbsolutePath)
      .getOrCreate()

    // execute the query and generate events
    val df = fun(spark)
    df.collect()

    val appId = spark.sparkContext.applicationId

    // close the event log
    spark.close()

    // find the event log
    val files = listFilesMatching(eventLogDir, !_.startsWith("."))
    if (files.length != 1) {
      throw new FileNotFoundException(s"Could not find event log in ${eventLogDir.getAbsolutePath}")
    }
    (files.head.getAbsolutePath, appId)
  }

  def listFilesMatching(dir: File, matcher: String => Boolean): Array[File] = {
    dir.listFiles(new FilenameFilter {
      override def accept(file: File, s: String): Boolean = matcher(s)
    })
  }

  def compareDataFrames(df: DataFrame, expectedDf: DataFrame): Unit = {
    val diffCount = df.except(expectedDf).union(expectedDf.except(df)).count
    if (diffCount != 0) {
      logWarning("Diff expected vs actual:")
      expectedDf.show(1000, false)
      df.show(1000, false)
    }
    assert(diffCount == 0)
  }

  def readExpectationCSV(sparkSession: SparkSession, path: String,
      schema: Option[StructType] = None): DataFrame = {
    // make sure to change null value so empty strings don't show up as nulls
    if (schema.isDefined) {
      sparkSession.read.option("header", "true").option("nullValue", "-")
        .schema(schema.get).csv(path)
    } else {
      sparkSession.read.option("header", "true").option("nullValue", "-").csv(path)
    }
  }

  def processProfileApps(logs: Array[String],
      sparkSession: SparkSession): ArrayBuffer[ApplicationInfo] = {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(logs)
    var index: Int = 1
    for (path <- appArgs.eventlog()) {
      val eventLogInfo = EventLogPathProcessor
        .getEventLogInfo(path, sparkSession.sparkContext.hadoopConfiguration)
      assert(eventLogInfo.size >= 1, s"event log not parsed as expected $path")
      apps += new ApplicationInfo(sparkSession.sparkContext.hadoopConfiguration,
        eventLogInfo.head._1, index)
      index += 1
    }
    apps
  }
}

