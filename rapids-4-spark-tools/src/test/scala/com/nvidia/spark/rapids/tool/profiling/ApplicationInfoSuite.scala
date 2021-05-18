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

  val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }
  var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
  val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))

  test("test single event") {
    val tempFile = File.createTempFile("tempOutputFile", null)
    val fileWriter = new FileWriter(tempFile)
    try {
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs, sparkSession, fileWriter, path, index)
        index += 1
      }
      assert(apps.size == 1)
      assert(apps.head.sparkVersion.equals("3.1.1"))
      assert(apps.head.gpuMode.equals(true))

    } finally {
      fileWriter.close()
      tempFile.deleteOnExit()
    }
  }
}