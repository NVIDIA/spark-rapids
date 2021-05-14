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

package org.apache.spark.sql.rapids.tool.profiling

import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark.internal.Logging

class ApplicationInfoSuite extends FunSuite with Logging {

  val sparkSession = ProfileUtils.createSparkSession
  var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
  val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))
  val fileWriter = new PrintWriter("src/test/resources/workload_profiling")

  test("testing single event log count") {
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
      index += 1
    }
    assert(apps.size == 1)
  }
}