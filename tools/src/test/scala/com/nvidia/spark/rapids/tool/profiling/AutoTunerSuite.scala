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
      "spark.rapids.memory.pinnedPool.size" -> "2048m",
      "spark.scheduler.mode" -> "FAIR",
      "spark.sql.cbo.enabled" -> "true",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.ui.port" -> "0",
      "spark.yarn.am.memory" -> "640m"
    )
  }

  private def buildWorkerInfoAsString(
      customProps: Option[mutable.Map[String, String]] = None,
      numCores: Option[Int] = Some(32),
      systemMemory: Option[String] = Some("122880MiB"),
      numWorkers: Option[Int] = Some(4),
      gpuCount: Option[Int] = Some(2),
      gpuMemory: Option[String] = Some("15109MiB"),
      gpuDevice: Option[String] = Some("T4")): String = {
    val gpuWorkerProps = new GpuWorkerProps(
      gpuMemory.getOrElse("15109MiB"), gpuCount.getOrElse(2), gpuDevice.getOrElse("T4"))
    val cpuSystem = new SystemClusterProps(
      numCores.getOrElse(32), systemMemory.getOrElse("122880MiB"), numWorkers.getOrElse(4))
    val systemProperties = customProps match {
      case None => mutable.Map[String, String]()
      case Some(newProps) => newProps
      }
    val convertedMap = new util.LinkedHashMap[String, String](systemProperties.asJava)
    val clusterProps = new ClusterProperties(cpuSystem, gpuWorkerProps, convertedMap)
    // set the options to convert the object into formatted yaml content
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
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing CPU cores") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(0))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- Incorrect values in worker system information: {numCores: 0, memory: 122880MiB, numWorkers: 4}.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing CPU memory") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, None, Some("0m"))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- Incorrect values in worker system information: {numCores: 32, memory: 0m, numWorkers: 4}.
          |- 'spark.executor.memory' should be set to at least 2GB/core.
          |- 'spark.executor.instances' should be set to (gpuCount * numWorkers).
          |- 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
          |- 'spark.rapids.sql.concurrentGpuTasks' should be set to Max(4, (gpuMemory / 8G)).
          |- 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing number of workers") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, None, None, Some(0))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=2
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=7372m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
          |- Number of workers is missing. Setting default to 1.
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing GPU count") {
    // the gpuCount should default to 1
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), None, None, None, Some(0))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=32
          |--conf spark.executor.memory=65536m
          |--conf spark.executor.memoryOverhead=10649m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.03125
          |
          |Comments:
          |- GPU count is missing. Setting default to 1.
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with missing GPU memory") {
    // the gpu memory should be set to T4 memory settings
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo =
      buildWorkerInfoAsString(Some(sparkProps), None, None, None, None, Some("0m"))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- GPU memory is missing. Setting default to 15109m.
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with unknown GPU device") {
    // with unknown fpu device, the memory won't be set correctly, then it should default to 16G
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo =
      buildWorkerInfoAsString(Some(sparkProps), None, None, None, None, Some("0m"), Some("GPU-X"))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- GPU memory is missing. Setting default to 16384m.
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("test T4 dataproc cluster with dynamicAllocation enabled") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps))
    val expectedResults =
      s"""|
          |Spark Properties:
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
    val customProps = mutable.LinkedHashMap(
      "spark.dynamicAllocation.enabled" -> "false",
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps))
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.instances=8
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

  test("test AutoTuner with empty sparkProperties" ) {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=16
          |--conf spark.executor.instances=8
          |--conf spark.executor.memory=32768m
          |--conf spark.executor.memoryOverhead=7372m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.files.maxPartitionBytes=512m
          |--conf spark.sql.shuffle.partitions=200
          |--conf spark.task.resource.gpu.amount=0.0625
          |
          |Comments:
          |- 'spark.executor.instances' was not set.
          |- 'spark.executor.cores' was not set.
          |- 'spark.task.resource.gpu.amount' was not set.
          |- 'spark.rapids.sql.concurrentGpuTasks' was not set.
          |- 'spark.executor.memory' was not set.
          |- 'spark.rapids.memory.pinnedPool.size' was not set.
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.sql.files.maxPartitionBytes' was not set.
          |- 'spark.sql.shuffle.partitions' was not set.
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, None)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }
}
