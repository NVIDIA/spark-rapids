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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.FunSuite
import scala.io.Source.fromFile
import scala.util.Random

import org.apache.spark.internal.Logging
import org.apache.spark.sql.TrampolineUtil

class AutoTunerSuite extends FunSuite with Logging {
  private val autoTunerLogDir =
    ToolTestUtils.getTestResourcePath("AutoTuner")

  private def testSystemPropertyFile(systemProperties: String): (SystemProps, Option[String]) = {
    var systemInfoWithMsg:(SystemProps, Option[String]) = null
    TrampolineUtil.withTempDir { tempDir =>
      val systemPropsFile = Paths.get(tempDir.getAbsolutePath, "system_props.yaml")
      Files.write(systemPropsFile, systemProperties.getBytes(StandardCharsets.UTF_8))
      systemInfoWithMsg = AutoTuner.parseSystemInfo(systemPropsFile.toAbsolutePath.toString)
    }
    systemInfoWithMsg
  }

  test("test system property - exists") {
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 512gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count: 8
        |  memory: 32gb
        |  name: NVIDIA V100
        |""".stripMargin
    val (systemInfo, msg) = testSystemPropertyFile(systemProperties)
    assert(systemInfo != null && msg.isEmpty)
  }

  test("test system property - does not exists") {
    val systemProperties = ""
    val (systemInfo, msg) = testSystemPropertyFile(systemProperties)
    assert(systemInfo == null && msg.nonEmpty)
  }

  test("test system property - without gpu") {
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 576gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count:
        |  memory:
        |  name:
        |""".stripMargin
    val (systemInfo, msg) = testSystemPropertyFile(systemProperties)
    assert(systemInfo != null)
    assert(msg.isEmpty)
    assert(systemInfo.gpuProps == null)
  }

  test("test system property - without num-workers") {
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 512gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers:
        |gpu:
        |  count: 8
        |  memory: 32gb
        |  name: NVIDIA V100
        |""".stripMargin
    val (systemInfo, msg) = testSystemPropertyFile(systemProperties)
    assert(systemInfo != null)
    assert(msg.isEmpty)
    assert(systemInfo.numWorkers.isEmpty)
  }

  test("test convert from human readable size") {
    val rand = new Random()
    AutoTuner.SUPPORTED_SIZE_UNITS.zipWithIndex.foreach {
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
    AutoTuner.SUPPORTED_SIZE_UNITS.zipWithIndex.foreach {
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
      appId: String,
      systemProperties: String,
      expectedResults: String): Unit = {
    TrampolineUtil.withTempDir { tempDir =>
      val systemPropsFile = Paths.get(tempDir.getAbsolutePath, "system_props.yaml")
      Files.write(systemPropsFile, systemProperties.getBytes(StandardCharsets.UTF_8))

      val appArgs = new ProfileArgs(Array(
        "--auto-tuner",
        "--worker-info",
        systemPropsFile.toAbsolutePath.toString,
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val resultsFile = fromFile(tempDir + s"/${Profiler.SUBDIR}/$appId/profile.log")
      val recommendedResults = resultsFile.getLines().toSeq

      val startIndex =
        recommendedResults.indexWhere(line => line.contains("Recommended Configuration"))
      val endIndex = expectedResults.split("\r\n|\r|\n").length
      val filteredResults = recommendedResults
        .slice(startIndex, startIndex + endIndex).mkString("\n")
      assert(filteredResults == expectedResults)
    }
  }

  test("test recommended configuration") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val appId = "local-1655514789396"
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 512gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count: 8
        |  memory: 32gb
        |  name: NVIDIA V100
        |""".stripMargin
    val expectedResults =
      """
        |### D. Recommended Configuration ###
        |
        |Spark Properties:
        |--conf spark.executor.cores=8
        |--conf spark.executor.instances=32
        |--conf spark.executor.memory=63.50g
        |--conf spark.executor.memoryOverhead=8.35g
        |--conf spark.rapids.memory.pinnedPool.size=2g
        |--conf spark.rapids.sql.concurrentGpuTasks=4
        |--conf spark.sql.files.maxPartitionBytes=31.67g
        |--conf spark.sql.shuffle.partitions=200
        |--conf spark.task.resource.gpu.amount=0.125
        |
        |Comments:
        |- Although 'spark.sql.files.maxPartitionBytes' was set to 2gb, recommended value is 31.67g.
        |""".stripMargin.trim
    testSingleEvent(eventLog, appId, systemProperties, expectedResults)
  }

  test("test recommended configuration - without system properties") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val appId = "local-1655514789396"
    val systemProperties = ""
    val expectedResults =
      """
        |### D. Recommended Configuration ###
        |Cannot recommend properties. See Comments.
        |
        |Comments:
        |- System properties file was not formatted correctly.
        |- 'spark.executor.memory' should be set to at least 2GB/core.
        |- 'spark.executor.instances' should be set to 'num_gpus * num_workers'.
        |- 'spark.task.resource.gpu.amount' should be set to 1/#cores.
        |- 'spark.rapids.sql.concurrentGpuTasks' should be set to 2.
        |- 'spark.rapids.memory.pinnedPool.size' should be set to 2g.
        |""".stripMargin.trim
    testSingleEvent(eventLog, appId, systemProperties, expectedResults)
  }

  test("test recommended configuration - without gpu properties") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val appId = "local-1655514789396"
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 576gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count:
        |  memory:
        |  name:
        |""".stripMargin
    val expectedResults =
      """
        |### D. Recommended Configuration ###
        |
        |Spark Properties:
        |--conf spark.executor.cores=64
        |--conf spark.executor.instances=4
        |--conf spark.executor.memory=8.94g
        |--conf spark.executor.memoryOverhead=2.89g
        |--conf spark.rapids.memory.pinnedPool.size=2g
        |--conf spark.sql.files.maxPartitionBytes=31.67g
        |--conf spark.sql.shuffle.partitions=200
        |
        |Comments:
        |- Although 'spark.sql.files.maxPartitionBytes' was set to 2gb, recommended value is 31.67g.
        |- 'spark.task.resource.gpu.amount' should be set to 1/#cores.
        |- 'spark.rapids.sql.concurrentGpuTasks' should be set to 2.
        |- 'spark.rapids.memory.pinnedPool.size' should be set to 2g.
        |""".stripMargin.trim
    testSingleEvent(eventLog, appId, systemProperties, expectedResults)
  }

  test("test recommended configuration - low memory") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val appId = "local-1655514789396"
    val systemProperties =
      """
        |system:
        |  num_cores: 64
        |  cpu_arch: x86_64
        |  memory: 20gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count: 8
        |  memory: 32gb
        |  name: NVIDIA V100
        |""".stripMargin
    val expectedResults =
      """
        |### D. Recommended Configuration ###
        |
        |Spark Properties:
        |--conf spark.executor.cores=8
        |--conf spark.executor.instances=32
        |--conf spark.executor.memory=2g
        |--conf spark.executor.memoryOverhead=2.20g
        |--conf spark.rapids.memory.pinnedPool.size=2g
        |--conf spark.rapids.sql.concurrentGpuTasks=4
        |--conf spark.sql.files.maxPartitionBytes=31.67g
        |--conf spark.sql.shuffle.partitions=200
        |--conf spark.task.resource.gpu.amount=0.125
        |
        |Comments:
        |- Executor memory is very low. It is recommended to have at least 8g.
        |- Although 'spark.sql.files.maxPartitionBytes' was set to 2gb, recommended value is 31.67g.
        |""".stripMargin.trim
    testSingleEvent(eventLog, appId, systemProperties, expectedResults)
  }

  test("test recommended configuration - low num cores") {
    val eventLog = s"$autoTunerLogDir/auto_tuner_eventlog"
    val appId = "local-1655514789396"
    val systemPropsFilePath =
      """
        |system:
        |  num_cores: 16
        |  cpu_arch: x86_64
        |  memory: 512gb
        |  free_disk_space: 800gb
        |  time_zone: America/Los_Angeles
        |  num_workers: 4
        |gpu:
        |  count: 8
        |  memory: 32gb
        |  name: NVIDIA V100
        |""".stripMargin
    val expectedFilePath =
      """
        |### D. Recommended Configuration ###
        |
        |Spark Properties:
        |--conf spark.executor.cores=2
        |--conf spark.executor.instances=32
        |--conf spark.executor.memory=63.50g
        |--conf spark.executor.memoryOverhead=8.35g
        |--conf spark.rapids.memory.pinnedPool.size=2g
        |--conf spark.rapids.sql.concurrentGpuTasks=4
        |--conf spark.sql.files.maxPartitionBytes=31.67g
        |--conf spark.sql.shuffle.partitions=200
        |--conf spark.task.resource.gpu.amount=0.5
        |
        |Comments:
        |- Number of cores per executor is very low. It is recommended to have at least 4 cores per executor.
        |- Although 'spark.sql.files.maxPartitionBytes' was set to 2gb, recommended value is 31.67g.
        |- For the given GPU, number of CPU cores is very low. It should be at least equal to concurrent gpu tasks i.e. 4.
        |""".stripMargin.trim
    testSingleEvent(eventLog, appId, systemPropsFilePath, expectedFilePath)
  }
}
