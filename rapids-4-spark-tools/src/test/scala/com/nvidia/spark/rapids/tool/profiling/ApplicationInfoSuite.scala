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

import java.io.File

import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._

class ApplicationInfoSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }
  test("test single event") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))

    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("3.1.1"))
    assert(apps.head.gpuMode.equals(true))
    assert(apps.head.jobStart(apps.head.index).jobID.equals(1))
    assert(apps.head.stageSubmitted(apps.head.index).numTasks.equals(1))
    assert(apps.head.stageSubmitted(2).stageId.equals(2))
    assert(apps.head.taskEnd(apps.head.index).successful.equals(true))
    assert(apps.head.taskEnd(apps.head.index).endReason.equals("Success"))
    assert(apps.head.executors.head.totalCores.equals(8))
    println(apps.head.resourceProfiles.head.exec_mem)
    assert(apps.head.resourceProfiles.head.exec_mem.equals(1024L))
  }

  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      "src/test/resources/spark-events-qualification/udf_func_eventlog",
      "src/test/resources/spark-events-qualification/udf_dataset_eventlog",
      "src/test/resources/spark-events-qualification/dataset_eventlog"
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
  }

  val tempFile1 = File.createTempFile("tempOutputFile1", null)
  val tempFile2 = File.createTempFile("tempOutputFile2", null)
  val tempFile3 = File.createTempFile("tempOutputFile3", null)
  val tempFile4 = File.createTempFile("tempOutputFile3", null)

  tempFile1.setLastModified(98765432)  // newest file
  tempFile2.setLastModified(12324567)  // oldest file
  tempFile3.setLastModified(34567891)  // second newest file
  tempFile4.setLastModified(23456789)

  test("test filter file newest") {
    val filterNew = "2-newest"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterNew,
      tempFile1.toString,
      tempFile2.toString,
      tempFile3.toString
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
    // Validate 2 newest files
    assert(result(0).getName.equals(tempFile1.getName))
    assert(result(1).getName.equals(tempFile3.getName))

    tempFile1.deleteOnExit()
    tempFile2.deleteOnExit()
    tempFile3.deleteOnExit()
  }

  test("test filter file oldest and file name match") {
    val filterOld = "3-oldest"
    val matchFileName = "temp"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterOld,
      "--match-event-logs",
      matchFileName,
      tempFile1.toString,
      tempFile2.toString,
      tempFile3.toString,
      tempFile4.toString
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 3)
    // Validate 3 oldest files
    assert(result(0).getName.equals(tempFile2.getName))
    assert(result(1).getName.equals(tempFile4.getName))
    assert(result(2).getName.equals(tempFile3.getName))

    tempFile1.deleteOnExit()
    tempFile2.deleteOnExit()
    tempFile3.deleteOnExit()
    tempFile4.deleteOnExit()
  }
}
