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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

class CompareSuite extends FunSuite {

  val hadoopConf = new Configuration()
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test spark2 and spark3 event logs compare") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd",
      s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 2)
    val compare = new CompareApplications(apps)
    val (matchingSqlIdsRet, matchingStageIdsRet) = compare.findMatchingStages()
    // none match
    assert(matchingSqlIdsRet.size === 29)
    assert(matchingSqlIdsRet.head.outputHeaders.size == 2)
    assert(matchingSqlIdsRet.head.rows.size == 2)
    assert(matchingStageIdsRet.size === 73)
    assert(matchingStageIdsRet.head.outputHeaders.size == 2)
    assert(matchingStageIdsRet.head.rows.size == 2)
  }

  test("test 2 app runs event logs compare") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/rapids_join_eventlog2.zstd",
      s"$logDir/rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 2)
    val compare = new CompareApplications(apps)
    val (matchingSqlIdsRet, matchingStageIdsRet) = compare.findMatchingStages()
    // all match
    assert(matchingSqlIdsRet.size === 1)
    assert(matchingSqlIdsRet.head.outputHeaders.size == 2)
    assert(matchingSqlIdsRet.head.rows(0) == matchingSqlIdsRet.head.rows(1))
    assert(matchingStageIdsRet.size === 4)
    assert(matchingStageIdsRet.head.outputHeaders.size == 2)
    val firstRow = matchingStageIdsRet(3)
    assert(firstRow.rows(0) == firstRow.rows(1))

  }
}
