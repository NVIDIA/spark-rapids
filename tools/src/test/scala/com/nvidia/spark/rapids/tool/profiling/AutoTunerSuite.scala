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


class AppInfoProviderMockTest(val maxInput: Double,
    val spilledMetrics: Seq[Long],
    val jvmGCFractions: Seq[Double],
    val propsFromLog: mutable.Map[String, String],
    val sparkVersion: Option[String]) extends AppSummaryInfoBaseProvider {
  override def getMaxInput: Double = maxInput
  override def getSpilledMetrics: Seq[Long] = spilledMetrics
  override def getJvmGCFractions: Seq[Double] = jvmGCFractions
  override def getProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkVersion: Option[String] = sparkVersion
}

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
      gpuMemory.getOrElse(""), gpuCount.getOrElse(0), gpuDevice.getOrElse(""))
    val cpuSystem = new SystemClusterProps(
      numCores.getOrElse(0), systemMemory.getOrElse(""), numWorkers.getOrElse(0))
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

  private def getUnusedProvider: AppSummaryInfoBaseProvider = {
    new AppSummaryInfoBaseProvider()
  }

  private def getMockInfoProvider(maxInput: Double,
      spilledMetrics: Seq[Long],
      jvmGCFractions: Seq[Double],
      propsFromLog: mutable.Map[String, String],
      sparkVersion: Option[String]): AppSummaryInfoBaseProvider = {
    new AppInfoProviderMockTest(maxInput, spilledMetrics, jvmGCFractions, propsFromLog,
      sparkVersion)
  }

  test("Load non-existing cluster properties") {
    val autoTuner = AutoTuner.buildAutoTuner("non-existing.yaml", getUnusedProvider)
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

  test("Load cluster properties with CPU cores 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(0))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with CPU memory missing") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), None)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|Cannot recommend properties. See Comments.
          |
          |Comments:
          |- Incorrect values in worker system information: {numCores: 32, memory: , numWorkers: 4}.
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

  test("Load cluster properties with CPU memory 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("0m"))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with number of workers 0") {
    val dataprocWorkerInfo = buildWorkerInfoAsString(None, Some(32), Some("122880MiB"), Some(0))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with GPU count of 0") {
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
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(0))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with GPU memory is missing") {
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
      buildWorkerInfoAsString(Some(sparkProps), Some(32), Some("122880MiB"), Some(4), Some(2), None)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with GPU memory 0") {
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
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0M"))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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

  test("Load cluster properties with GPU name missing") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), None)
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.sql.shuffle.partitions=200
          |
          |Comments:
          |- GPU device is missing. Setting default to T4.
          |- GPU memory is missing. Setting default to 15109m.
          |- 'spark.sql.shuffle.partitions' was not set.
          |""".stripMargin
    assert(expectedResults == autoTunerOutput)
  }

  test("Load cluster properties with unknown GPU device") {
    // with unknown gpu device, the memory won't be set correctly, then it should default to 16G
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "16",
      "spark.executor.memory" -> "32768m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.rapids.memory.pinnedPool.size" -> "4096m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.sql.files.maxPartitionBytes" -> "512m",
      "spark.task.resource.gpu.amount" -> "0.0625")
    val sparkProps = defaultDataprocProps.++(customProps)
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(sparkProps), Some(32),
      Some("122880MiB"), Some(4), Some(2), Some("0MiB"), Some("GPU-X"))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
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
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, getUnusedProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    assert(expectedResults == autoTunerOutput)
  }

  // Test that the properties from the eventlogs will be used to calculate the recommendations.
  // For example, the output should recommend "spark.executor.cores" -> 8. Although the cluster
  // "spark.executor.cores" is the same as the recommended value, the property is set to 16 in
  // the eventlogs.
  test("AutoTuner gives precedence to properties from eventlogs") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "200",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.0625",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.rapids.sql.concurrentGpuTasks" -> "4")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(8126464.0, Seq(0), Seq(0.004), logEventsProps,
      Some("3.1.1"))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=5734m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.files.maxPartitionBytes=4096m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  // Changing the maxInput of tasks should reflect on the maxPartitions recommendations.
  // Values used in setting the properties are taken from sample eventlogs.
  test("Recommendation of maxPartitions is calculated based on maxInput of tasks") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val autoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo,
      getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.01, 0.0), logEventsProps,
        Some("3.1.1")))
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=5734m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }

  // When GCFraction is higher AutoTuner.MAX_JVM_GCTIME_FRACTION, the output should contain
  // a comment recommending to consider different GC algorithms.
  // This test triggers that case by injecting a sequence of jvmGCFraction with average higher
  // than the static threshold of 0.3.
  test("Output contains GC comments when GC Fraction is higher than threshold") {
    val customProps = mutable.LinkedHashMap(
      "spark.executor.cores" -> "8",
      "spark.executor.memory" -> "47222m",
      "spark.rapids.sql.concurrentGpuTasks" -> "2",
      "spark.task.resource.gpu.amount" -> "0.0625")
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "16",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.resource.gpu.amount" -> "1",
        "spark.executor.instances" -> "1",
        "spark.sql.shuffle.partitions" -> "1000",
        "spark.sql.files.maxPartitionBytes" -> "1g",
        "spark.task.resource.gpu.amount" -> "0.25",
        "spark.rapids.memory.pinnedPool.size" -> "5g",
        "spark.rapids.sql.enabled" -> "true",
        "spark.rapids.sql.concurrentGpuTasks" -> "1")
    val dataprocWorkerInfo = buildWorkerInfoAsString(Some(customProps), Some(32),
      Some("212992MiB"), Some(5), Some(4), Some("15109MiB"), Some("Tesla T4"))
    val infoProvider = getMockInfoProvider(3.7449728E7, Seq(0, 0), Seq(0.4, 0.4), logEventsProps,
      Some("3.1.1"))
    val autoTuner: AutoTuner = AutoTuner.buildAutoTunerFromProps(dataprocWorkerInfo, infoProvider)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    // scalastyle:off line.size.limit
    val expectedResults =
      s"""|
          |Spark Properties:
          |--conf spark.executor.cores=8
          |--conf spark.executor.instances=20
          |--conf spark.executor.memory=16384m
          |--conf spark.executor.memoryOverhead=5734m
          |--conf spark.rapids.memory.pinnedPool.size=4096m
          |--conf spark.rapids.sql.concurrentGpuTasks=2
          |--conf spark.sql.files.maxPartitionBytes=3669m
          |--conf spark.task.resource.gpu.amount=0.125
          |
          |Comments:
          |- 'spark.executor.memoryOverhead' was not set.
          |- 'spark.executor.memoryOverhead' must be set if using 'spark.rapids.memory.pinnedPool.size
          |- 'spark.sql.adaptive.enabled' should be enabled for better performance.
          |- Average JVM GC time is very high. Other Garbage Collectors can be used for better performance.
          |""".stripMargin
    // scalastyle:on line.size.limit
    assert(expectedResults == autoTunerOutput)
  }
}
