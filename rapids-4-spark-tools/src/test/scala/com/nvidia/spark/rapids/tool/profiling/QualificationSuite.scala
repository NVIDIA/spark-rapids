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
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling._

class QualificationSuite extends FunSuite with Logging {

  val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }
  var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
  val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))

  private val expRoot = ProfilingTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ProfilingTestUtils.getTestResourcePath("spark-events-qualification")

  test("test udf event logs") {

    val resultExpectation = "eventlog_minimal_events_expectation.csv"
    TrampolineUtil.withTempPath { csvOutpath =>
      val file = new File(expRoot, "qual_test_simple_expectation.csv")

      val appArgs = new ProfileArgs(Array(
        "--eventlog-dir",
        logDir
      ))

      val (exit, dfQualOpt) =
        QualificationMain.mainInternal(sparkSession, appArgs, writeOutput=false)

      val dfExpect = sparkSession.read.option("header", "true").csv(resultExpectation)
      val diffCount = dfQualOpt.map { dfQual =>
        dfQual.except(dfExpect).union(dfExpect.except(dfExpect)).count
      }.getOrElse(-1)
      assert(diffCount == 0)
    }
  }
}
