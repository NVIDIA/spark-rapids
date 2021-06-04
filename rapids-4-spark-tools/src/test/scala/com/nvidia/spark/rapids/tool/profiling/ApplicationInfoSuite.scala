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

import java.io.{File, FileWriter}

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

    val tempFile = File.createTempFile("tempOutputFile", null)
    try {
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
          ProfileUtils.stringToPath(path)._1(0), index)
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
    } finally {
      tempFile.deleteOnExit()
    }
  }

  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      "src/test/resources/udf_func_eventlog",
      "src/test/resources/udf_dataset_eventlog",
      "src/test/resources/dataset_eventlog"
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
  }

  test("test filter file newest") {
    val filterNew = "2-newest"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterNew,
      "src/test/resources/dataset_eventlog",
      "src/test/resources/eventlog_minimal_events",
      "src/test/resources/udf_func_eventlog",
      "src/test/resources/udf_dataset_eventlog"
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
    //Validate filenames
    assert(result.toString.contains("udf_func_eventlog"))
    assert(result.toString.contains("udf_dataset_eventlog"))
  }

  test("test filter file oldest and file name match") {
    val filterOld = "3-oldest"
    val matchFileName = "event"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterOld,
      "--match-event-logs",
      matchFileName,
      "src/test/resources/dataset_eventlog",
      "src/test/resources/eventlog_minimal_events",
      "src/test/resources/udf_func_eventlog",
      "src/test/resources/udf_dataset_eventlog"
    ))

    val result = ProfileMain.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())

    assert(result.length == 3)
    // Validate filenames
    assert(result.toString.contains("eventlog_minimal_events"))
    assert(result.toString.contains("udf_func_eventlog"))
    assert(result.toString.contains("udf_dataset_eventlog"))
  }
}
