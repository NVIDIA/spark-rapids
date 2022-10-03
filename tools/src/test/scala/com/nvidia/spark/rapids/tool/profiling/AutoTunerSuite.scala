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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import org.apache.spark.internal.Logging

class AutoTunerSuite extends FunSuite with BeforeAndAfterEach with Logging {

  val defaultDataprocProps: mutable.Map[String, String] = {
    mutable.LinkedHashMap[String, String](
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.driver.maxResultSize" -> "7680m",
      "spark.driver.memory" -> "15360m",
      "spark.executor.cores" -> "16",
      "spark.executor.instances" -> "2",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.executor.memory" -> "26742m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.executorEnv.OPENBLAS_NUM_THREADS" -> "1",
      "spark.extraListeners" -> "com.google.cloud.spark.performance.DataprocMetricsListener",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.scheduler.mode" -> "FAIR",
      "spark.sql.cbo.enabled" -> "true",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.ui.port" -> "0",
      "spark.yarn.am.memory" -> "640m"
    )
  }

  private def buildWorkerInfoAsString(
      customProps: Option[mutable.Map[String, String]],
      numCores: Option[Int] = Some(32),
      gpuCount: Option[Int] = Some(2)): String = {
    val gpuWorkerProps = new GpuWorkerProps("122880MiB", gpuCount.getOrElse(2), "T4")
    val cpuSystem = new SystemClusterProps(numCores.getOrElse(32), "122880MiB", 4)
    val systemProperties = customProps match {
      case None => defaultDataprocProps
      case Some(newProps) => defaultDataprocProps.++(newProps)
      }
    val convertedMap = new util.LinkedHashMap[String, String](systemProperties.asJava)
    val clusterProps = {
      new ClusterProperties(cpuSystem, gpuWorkerProps, convertedMap)
      // set the options to convert the object into formatted yaml content
    }
    val options = new DumperOptions()
    options.setIndent(2)
    options.setPrettyFlow(true)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(options)
    val rawString = yaml.dump(clusterProps)
    // Skip the first line as it contains "the name of the class"
    rawString.split("\n").drop(1).mkString("\n")
  }

  test("Load non-existing cluster properties") {
    val autoTuner: AutoTuner = AutoTuner.buildAutoTuner("non-existing.yaml", None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- java.io.FileNotFoundException: File non-existing.yaml does not exist
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (Gpu_memory / 8G)).
          |- 'spark.rapids.memory.pinnedPool.size' should be set up to 4096m.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing CPU cores") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(0))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- Worker info has incorrect number of cores: 0.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (Gpu_memory / 8G)).
          |- 'spark.rapids.memory.pinnedPool.size' should be set up to 4096m.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing GPU count") {
    // the gpuCount should default to 1
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(mutable.LinkedHashMap(
        "spark.executor.cores" -> "16",
        "spark.executor.memory" -> "32768m",
        "spark.executor.memoryOverhead" -> "7372m",
        "spark.rapids.memory.pinnedPool.size" -> "4096m",
        "spark.rapids.sql.concurrentGpuTasks" -> "2",
        "spark.sql.files.maxPartitionBytes" -> "512m",
        "spark.task.resource.gpu.amount" -> "0.0625")),
      Some(32), Some(0))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=32
          |--conf spark.executor.memory=65536m
          |--conf spark.executor.memoryOverhead=10649m
          |--conf spark.rapids.sql.concurrentGpuTasks=4
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.03125
          |
          |Comments:
          |- GPU count is missing. Setting default to 1.
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("test T4 dataproc cluster with dynamicAllocation enabled") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")))
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.rapids.sql.concurrentGpuTasks=4
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }

  // This mainly to test that the executorInstances will be calculated when the dynamic allocation
  // is missing.
  test("test T4 dataproc cluster with missing dynamic allocation") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(mutable.LinkedHashMap(
      "spark.dynamicAllocation.enabled" -> "false",
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")))
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
          |--conf spark.rapids.sql.concurrentGpuTasks=4
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }
}
