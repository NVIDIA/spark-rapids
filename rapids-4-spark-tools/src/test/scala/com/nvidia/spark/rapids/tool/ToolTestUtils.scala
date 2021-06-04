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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling.{ProfileArgs, ProfileUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

object ToolTestUtils extends Logging {

  def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  def compareDataFrames(df: DataFrame, expectedDf: DataFrame): Unit = {
    val diffCount = df.except(expectedDf).union(expectedDf.except(df)).count
    if (diffCount != 0) {
      logWarning("Diff expected vs actual:")
      expectedDf.show()
      df.show()
    }
    assert(diffCount == 0)
  }

  def readExpectationCSV(sparkSession: SparkSession, path: String): DataFrame = {
    // make sure to change null value so empty strings don't show up as nulls
    sparkSession.read.option("header", "true").option("nullValue", "-").csv(path)
  }

  def processProfileApps(logs: Array[String],
      sparkSession: SparkSession): ArrayBuffer[ApplicationInfo] = {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(logs)
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path)(0), index)
      index += 1
    }
    apps
  }
}
