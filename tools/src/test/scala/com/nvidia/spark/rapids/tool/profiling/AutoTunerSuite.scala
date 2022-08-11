/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.FunSuite
import scala.io.Source.fromFile
import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}

class AutoTunerSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private val autoTunerLogDir =
    ToolTestUtils.getTestResourcePath("AutoTuner/EventLogs")
  private val expectedDir =
    ToolTestUtils.getTestResourcePath("AutoTuner/RecommenderExpectations")
  private val systemPropsDir = ToolTestUtils.getTestResourcePath("AutoTuner/SystemProperties")

  test("test system property file - exists") {
    val systemPropsFilePath = s"$systemPropsDir/system_props.yaml"
    val systemInfo = AutoTuner.parseSystemInfo(systemPropsFilePath)
    assert(systemInfo != null)
  }

  test("test system property file - does not exists") {
    val systemPropsFilePath = s"$systemPropsDir/system_props_fail.yaml"
    val systemInfo = AutoTuner.parseSystemInfo(systemPropsFilePath)
    assert(systemInfo == null)
  }

  test("test system property file - without gpu") {
    val systemPropsFilePath = s"$systemPropsDir/system_props_without_gpu.yaml"
    val systemInfo = AutoTuner.parseSystemInfo(systemPropsFilePath)
    assert(systemInfo != null)
    assert(systemInfo.gpu_props == null)
  }

  test("test system property file - without num-workers") {
    val systemPropsFilePath = s"$systemPropsDir/system_props_without_num_workers.yaml"
    val systemInfo = AutoTuner.parseSystemInfo(systemPropsFilePath)
    assert(systemInfo != null)
    assert(systemInfo.num_workers.isEmpty)
  }

  test("test convert from human readable size") {
    val rand = new Random()
    AutoTuner.SUPPORTED_UNITS.zipWithIndex.foreach {
      case (size, index) =>
        val randomValue = "%.3f".format(10 * rand.nextDouble())
        val testSize = s"$randomValue$size"
        val expectedSize = (randomValue.toDouble * Math.pow(1024, index)).toLong
        val resultSize = AutoTuner.convertFromHumanReadableSize(testSize)
        assert(resultSize == expectedSize)
    }
  }

  test("test convert to human readable size") {
    val rand = new Random()
    AutoTuner.SUPPORTED_UNITS.zipWithIndex.foreach {
      case (size, index) =>
      val randomValue = 1 + rand.nextInt(9)
      val testSize = (randomValue * Math.pow(1024, index)).toLong
      val expectedSize = s"$randomValue$size"
      val resultSize = AutoTuner.convertToHumanReadableSize(testSize)
      assert(resultSize == expectedSize)
    }
  }

  test("test compare spark version") {
    assert(AutoTuner.compareSparkVersion("3", "1") > 0)
    assert(AutoTuner.compareSparkVersion("3.3.1", "3.0.0") > 0)
    assert(AutoTuner.compareSparkVersion("4.1.5", "4.15") < 0)
    assert(AutoTuner.compareSparkVersion("2.0", "2.0.0") == 0)
    assert(AutoTuner.compareSparkVersion("2", "2.0.0") == 0)
  }

  private def testSingleEvent(
    eventLog: String,
    systemPropsFilePath: String,
    expectedFilePath: String): Unit = {

    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--auto-tuner",
        "--worker-info",
        systemPropsFilePath,
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val resultsFile = fromFile(tempDir + s"/${Profiler.SUBDIR}/local-1655514789396/profile.log")
      val recommendedResults = resultsFile.getLines().toSeq

      val expectedFile = fromFile(expectedFilePath)
      val expectedResults = expectedFile.getLines().toSeq

      val startIndex =
        recommendedResults.indexWhere(line => line.contains("Recommended Configuration"))
      val endIndex = expectedResults.size
      val filteredResults = recommendedResults.slice(startIndex, startIndex + endIndex)
      assert(filteredResults == expectedResults)
    }
  }

  test("test recommended configuration") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val systemPropsFilePath = s"$systemPropsDir/system_props.yaml"
    val expectedFilePath = s"$expectedDir/test_recommended_conf.txt"
    testSingleEvent(eventLog, systemPropsFilePath, expectedFilePath)
  }

  test("test recommended configuration - without system properties") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val expectedFilePath = s"$expectedDir/test_recommended_conf_without_system_prop.txt"
    testSingleEvent(eventLog, "", expectedFilePath)
  }

  test("test recommended configuration - without gpu properties") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val systemPropsFilePath = s"$systemPropsDir/system_props_without_gpu.yaml"
    val expectedFilePath = s"$expectedDir/test_recommended_conf_without_gpu_prop.txt"
    testSingleEvent(eventLog, systemPropsFilePath, expectedFilePath)
  }

  test("test recommended configuration - low memory") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val systemPropsFilePath = s"$systemPropsDir/system_props_low_memory.yaml"
    val expectedFilePath = s"$expectedDir/test_recommended_conf_low_memory.txt"
    testSingleEvent(eventLog, systemPropsFilePath, expectedFilePath)
  }

  test("test recommended configuration - low num cores") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val systemPropsFilePath = s"$systemPropsDir/system_props_low_num_cores.yaml"
    val expectedFilePath = s"$expectedDir/test_recommended_conf_low_num_cores.txt"
    testSingleEvent(eventLog, systemPropsFilePath, expectedFilePath)
  }
}
