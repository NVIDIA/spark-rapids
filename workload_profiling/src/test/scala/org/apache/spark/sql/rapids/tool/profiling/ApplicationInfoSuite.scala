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

import org.apache.log4j.Level
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class ApplicationInfoSuite extends FunSuite {

  val sparkSession = ProfileUtils.createSparkSession
  var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
  val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))
  val logger = ProfileUtils.createLogger("src/test/resources/", "profile_log")

  test("testing single event log count") {
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs, sparkSession, logger, path, index)
      index += 1
    }
    assert(apps.size == 1)
  }
}