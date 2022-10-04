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

import java.io.{BufferedReader, InputStreamReader}
import java.util

import scala.beans.BeanProperty
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.{Constructor, ConstructorException}
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.internal.Logging

/**
 * A wrapper class that stores all the GPU properties.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 */
class GpuWorkerProps(
    @BeanProperty var memory: String,
    @BeanProperty var count: Int,
    @BeanProperty var name: String) {
  def this() {
    this("0b", 0, "None")
  }
  def isMissingInfo: Boolean = {
    count == 0 || memory.startsWith("0") || name == "None"
  }
  def isEmpty: Boolean = {
    count == 0 && memory.startsWith("0") && name == "None"
  }
  /**
   * If the GPU count is missing, it will set a default value based on the number of cores in the
   * system. Adds GPU count for each 16 CPU core.
   *
   * @return true if the value has been updated.
   */
  def setDefaultGpuCountIfMissing(): Boolean = {
    if (count == 0) {
      count = AutoTuner.DEF_WORKER_GPU_COUNT
      true
    } else {
      false
    }
  }
  override def toString: String =
    s"{Count: $count, Memory: $memory, GpuDevice: $name}"
}

/**
 * A wrapper class that stores all the system properties.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 */
class SystemClusterProps(
    @BeanProperty var numCores: Int,
    @BeanProperty var memory: String,
    @BeanProperty var numWorkers: Int) {
  def this() {
    this(0, "0b", 0)
  }
  def isMissingInfo: Boolean = {
    numCores == 0 || memory.startsWith("0") || numWorkers == 0
  }
  def isEmpty: Boolean = {
    numCores == 0 && memory.startsWith("0") && numWorkers == 0
  }

  /**
   * Set the number of cores to default if it was 0.
   *
   * @return true if the value has been updated.
   */
  def setDefaultNumCoresIfMissing(coreNum: Int): Boolean = {
    if (numCores == 0) {
      numCores = coreNum
      true
    } else {
      false
    }
  }
  override def toString: String =
    s"{numCores: $numCores, Memory: $memory, numWorkers: $numWorkers}"
}

/**
 * A wrapper class that stores all the properties of the cluster.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 *
 * @param system wrapper that includes the properties related to number of cores and memory.
 * @param gpu wrapper that includes the properties related to GPU.
 * @param softwareProperties a set of software properties such as Spark properties.
 *                           The properties are typically loaded from the default cluster
 *                           configurations.
 */
class ClusterProperties(
    @BeanProperty var system: SystemClusterProps,
    @BeanProperty var gpu: GpuWorkerProps,
    @BeanProperty var softwareProperties: util.LinkedHashMap[String, String]) {

  import AutoTuner._

  def this() {
    this(new SystemClusterProps(), new GpuWorkerProps(), new util.LinkedHashMap[String, String]())
  }
  def isEmpty: Boolean = {
    system.isEmpty && gpu.isEmpty
  }
  def getTargetProperties: mutable.Map[String, String] = {
    softwareProperties.asScala.filter(entry => recommendationsTarget.contains(entry._1))
  }
  override def toString: String =
    s"{${system.toString}, ${gpu.toString}, $softwareProperties}"
}

/**
 * Wrapper to hold the recommendation of a given criterion.
 *
 * @param name the property label.
 * @param originalValue the value loaded from the spark properties.
 * @param recommended the recommended value by the AutoTuner.
 */
class RecommendationEntry(val name: String, val originalValue: String, var recommended: String) {
  def setRecommendedValue(value: String): Unit = {
    if (value != null) {
      recommended = value
    }
  }

  private def getRawValue(arg: String): String = {
    if (arg != null && AutoTuner.containsMemoryUnits(arg)) {
      // if it is memory return the bytesUnit
      s"${AutoTuner.convertFromHumanReadableSize(arg)}"
    } else {
      arg
    }
  }

  /**
   * Returns true when the recommendation is different than the original.
   */
  private def recommendsNewValue(): Boolean = {
    val convertedOriginal = getRawValue(originalValue)
    val convertedRecommended = getRawValue(recommended)
    convertedOriginal != convertedRecommended
  }

  /**
   * True or False whether the recommendation is valid. e.g., recommendations that does not change
   * the original value returns false if filter is enabled.
   * @param filterByUpdated flag to pick only the properties that would be updated by the
   *                        recommendations
   */
  def isValid(filterByUpdated: Boolean): Boolean = {
    if (recommended == null) { // no recommendations
      false
    } else {
      if (filterByUpdated) { // filter enabled
        recommendsNewValue()
      } else {
        true
      }
    }
  }
}

/**
 * AutoTuner module that uses event logs and worker's system properties to recommend Spark
 * RAPIDS configuration based on heuristics.
 *
 * Example:
 * a. Success:
 *    Input:
 *      system:
 *        num_cores: 64
 *        cpu_arch: x86_64
 *        memory: 512gb
 *        free_disk_space: 800gb
 *        time_zone: America/Los_Angeles
 *        num_workers: 4
 *      gpu:
 *        count: 8
 *        memory: 32gb
 *        name: NVIDIA V100
 *      softwareProperties:
 *        spark.driver.maxResultSize: 7680m
 *        spark.driver.memory: 15360m
 *        spark.executor.cores: '8'
 *        spark.executor.instances: '2'
 *        spark.executor.memory: 47222m
 *        spark.executorEnv.OPENBLAS_NUM_THREADS: '1'
 *        spark.extraListeners: com.google.cloud.spark.performance.DataprocMetricsListener
 *        spark.scheduler.mode: FAIR
 *        spark.sql.cbo.enabled: 'true'
 *        spark.ui.port: '0'
 *        spark.yarn.am.memory: 640m
 *
 *    Output:
 *       Spark Properties:
 *       --conf spark.executor.cores=8
 *       --conf spark.executor.instances=20
 *       --conf spark.executor.memory=16384m
 *       --conf spark.executor.memoryOverhead=5734m
 *       --conf spark.rapids.memory.pinnedPool.size=4096m
 *       --conf spark.rapids.sql.concurrentGpuTasks=2
 *       --conf spark.sql.files.maxPartitionBytes=4096m
 *       --conf spark.task.resource.gpu.amount=0.125
 *
 *       Comments:
 *       - 'spark.rapids.sql.concurrentGpuTasks' was not set.
 *       - 'spark.executor.memoryOverhead' was not set.
 *       - 'spark.rapids.memory.pinnedPool.size' was not set.
 *       - 'spark.sql.adaptive.enabled' should be enabled for better performance.
 *
 * b. Failure:
 *    Input: Incorrect File
 *    Output:
 *      Cannot recommend properties. See Comments.
 *
 *      Comments:
 *      - java.io.FileNotFoundException: File worker_info.yaml does not exist
 *      - 'spark.executor.memory' should be set to at least 2GB/core.
 *      - 'spark.executor.instances' should be set to (gpuCount * numWorkers).
 *      - 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
 *      - 'spark.rapids.sql.concurrentGpuTasks' should be set between [1, 4]
 *      - 'spark.rapids.memory.pinnedPool.size' should be set up to 4096m.
 *      - 'spark.sql.adaptive.enabled' should be enabled for better performance.
 *
 * @param clusterProps The cluster properties including cores, mem, GPU, and software
 *                     (see [[ClusterProperties]]).
 * @param appInfo the container holding the profiling result.
 */
class AutoTuner(
    val clusterProps: ClusterProperties,
    val appInfo: Option[ApplicationSummaryInfo])  extends Logging {

  import AutoTuner._

  var comments = new ListBuffer[String]()
  var recommendations: mutable.LinkedHashMap[String, RecommendationEntry] =
    mutable.LinkedHashMap[String, RecommendationEntry]()
  // list of criteria to be skipped for recommendations
  private var skipRecommendations: Option[Seq[String]] = None
  // When enabled, the profiler recommendations should only include updated settings.
  var filterByUpdatedPropertiesEnabled: Boolean = true

  private def findPropertyInProfPropertyResults(
      key: String,
      props: Seq[RapidsPropertyProfileResult]): Option[String] = {
    props.collectFirst {
      case entry: RapidsPropertyProfileResult
        if entry.key == key && entry.rows(1) != "null" => entry.rows(1)
    }
  }
  def getSparkPropertyFromProfile(propKey: String): Option[String] = {
    appInfo match {
      case None => None
      case Some(profInfo) =>
        val resFromProfSparkProps = findPropertyInProfPropertyResults(propKey, profInfo.sparkProps)
        resFromProfSparkProps match {
          case None => findPropertyInProfPropertyResults(propKey, profInfo.rapidsProps)
          case Some(_) => resFromProfSparkProps
        }
    }
  }

  def getPropertyValue(key: String): Option[String] = {
    val fromProfile = getSparkPropertyFromProfile(key)
    fromProfile match {
      case None => Option(clusterProps.softwareProperties.get(key))
      case Some(_) => fromProfile
    }
  }

  def initRecommendations(): Unit = {
    recommendationsTarget.foreach { key =>
      getPropertyValue(key).foreach { propVal =>
        val recommendationVal = new RecommendationEntry(key, propVal, null)
        recommendations(key) = recommendationVal
      }
    }
  }

  def appendRecommendation(key: String, value: String): Unit = {
    val skip = skipRecommendations match {
      case Some(seq) => seq.contains(key)
      case None => false
    }
    if (!skip) {
      val recomRecord = recommendations.getOrElseUpdate(key,
        new RecommendationEntry(key, null, null))
      recomRecord.setRecommendedValue(value)
      if (recomRecord.originalValue == null && value != null) {
        // add a comment
        appendComment(s"'$key' was not set.")
      }
    }
  }

  /**
   * Safely appends the recommendation to the given key.
   * It skips if the value is 0.
   */
  def appendRecommendation(key: String, value: Int): Unit = {
    if (value > 0) {
      appendRecommendation(key: String, s"$value")
    }
  }

  /**
   * Safely appends the recommendation to the given key.
   * It skips if the value is 0.0.
   */
  def appendRecommendation(key: String, value: Double): Unit = {
    if (value > 0.0) {
      appendRecommendation(key: String, s"$value")
    }
  }
  /**
   * Safely appends the recommendation to the given key.
   * It appends "m" to the string value. It skips if the value is 0 or null.
   */
  def appendRecommendationForMemoryMB(key: String, value: String): Unit = {
    if (value != null && value.toDouble > 0.0) {
      appendRecommendation(key, s"${value}m")
    }
  }

  /**
   * calculated 'spark.executor.instances' based on number of gpus and workers.
   */
  def calcExecInstances(): Int = {
    clusterProps.gpu.getCount * clusterProps.system.numWorkers
  }

  /**
   * Recommendation for 'spark.executor.instances' based on number of gpus and workers.
   * Assumption - If the properties include "spark.dynamicAllocation.enabled=true", then ignore
   * spark.executor.instances.
   */
  def recommendExecutorInstances(): Unit = {
    val execInstancesOpt = getPropertyValue("spark.dynamicAllocation.enabled") match {
        case Some(propValue) =>
          if (propValue.toBoolean) {
            None
          } else {
            Option(calcExecInstances())
          }
        case None => Option(calcExecInstances())
      }
    if (execInstancesOpt.isDefined) {
      appendRecommendation("spark.executor.instances", execInstancesOpt.get)
    }
  }

  /**
   * Recommendation for 'spark.executor.cores' based on number of cpu cores and gpus.
   */
  def calcNumExecutorCores: Int = {
    val executorsPerNode = clusterProps.gpu.getCount
    if (executorsPerNode == 0) {
      // Being extra caution about zero division.
      // handling 0 and default value is processed in [[processPropsAndCheck]]
      1
    } else {
      Math.max(1, clusterProps.system.getNumCores / executorsPerNode)
    }
  }

  /**
   * Recommendation for 'spark.task.resource.gpu.amount' based on num of cpu cores.
   */
  def calcTaskGPUAmount(numExecCoresCalculator: () => Int): Double = {
    val numExecutorCores =  numExecCoresCalculator.apply()
    if (numExecutorCores == 0) 0.0 else 1.0 / numExecutorCores
  }

  /**
   * Recommendation for 'spark.rapids.sql.concurrentGpuTasks' based on gpu memory.
   */
  def calcGpuConcTasks(): Long = {
    Math.min(MAX_CONC_GPU_TASKS,
      convertToMB(clusterProps.gpu.memory) / DEF_GPU_MEM_PER_TASK_MB)
  }

  /**
   * Calculates the available memory for each executor on the worker based on the number of
   * executors per node and the memory.
   */
  private def calcAvailableMemPerExec(): Double = {
    // account for system overhead
    val usableWorkerMem =
      Math.max(0, convertToMB(clusterProps.system.memory) - DEF_SYSTEM_RESERVE_MB)
    val executorsPerNode = clusterProps.gpu.getCount
    if (executorsPerNode == 0) {
      0.0
    } else {
      (1.0 * usableWorkerMem) / executorsPerNode
    }
  }

  /**
   * Recommendation for 'spark.executor.memory' based on system memory, cluster scheduler
   * and num of gpus.
   */
  def calcExecutorHeap(executorContainerMemCalculator: () => Double,
      numExecCoresCalculator: () => Int): Long = {
    // reserve 10% of heap as memory overhead
    val maxExecutorHeap = Math.max(0,
      executorContainerMemCalculator() * (1 - DEF_HEAP_OVERHEAD_FRACTION)).toInt
    // give up to 2GB of heap to each executor core
    Math.min(maxExecutorHeap, DEF_HEAP_PER_CORE_MB * numExecCoresCalculator())
  }

  /**
   * Recommendation for 'spark.rapids.memory.pinnedPool.size' and 'spark.executor.memoryOverhead'
   * based on executor memory.
   */
  def calcPinnedMemoryWithOverhead(
      execHeapCalculator: () => Long,
      containerMemCalculator: () => Double): (Long, Long) = {
    val executorHeap = execHeapCalculator()
    var executorMemOverhead = (executorHeap * DEF_HEAP_OVERHEAD_FRACTION).toLong
    // pinned memory uses any unused space up to 4GB
    val pinnedMem = Math.min(MAX_PINNED_MEMORY_MB,
      containerMemCalculator.apply() - executorHeap - executorMemOverhead).toLong
    executorMemOverhead += pinnedMem
    (pinnedMem, executorMemOverhead)
  }

  private def getSparkVersion: Option[String] = {
    appInfo match {
      case Some(app) => Option(app.appInfo.head.sparkVersion)
      case None => None
    }
  }

  /**
   * Find the label of the memory.overhead based on the spark master configuration and the spark
   * version.
   * @return "spark.executor.memoryOverhead", "spark.kubernetes.memoryOverheadFactor",
   *         or "spark.executor.memoryOverheadFactor".
   */
  def memoryOverheadLabel: String = {
    val sparkMasterConf = getPropertyValue("spark.master")
    val defaultLabel = "spark.executor.memoryOverhead"
    sparkMasterConf match {
      case None => defaultLabel
      case Some(sparkMaster) =>
        if (sparkMaster.contains("yarn")) {
          defaultLabel
        } else if (sparkMaster.contains("k8s")) {
          getSparkVersion match {
            case Some(version) =>
              if (compareSparkVersion(version, "3.3.0") > 0) {
                "spark.executor.memoryOverheadFactor"
              } else {
                "spark.kubernetes.memoryOverheadFactor"
              }
            case None => defaultLabel
          }
        } else {
          defaultLabel
        }
    }
  }

  /**
   * Flow:
   *   if "spark.rapids.memory.pinnedPool.size" is set
   *     if yarn -> recommend "spark.executor.memoryOverhead"
   *     if using k8s ->
   *         if version > 3.3.0 recommend "spark.executor.memoryOverheadFactor" and add comment
   *         else recommend "spark.kubernetes.memoryOverheadFactor" and add comment if missing
   */
  def addRecommendationForMemoryOverhead(recomValue: String): Unit = {
    val memOverheadLookup = memoryOverheadLabel
    appendRecommendationForMemoryMB(memOverheadLookup, recomValue)
    getPropertyValue("spark.rapids.memory.pinnedPool.size").foreach { lookup =>
      if (lookup != "spark.executor.memoryOverhead") {
        if (getPropertyValue(memOverheadLookup).isEmpty) {
          appendComment(s"'$memOverheadLookup' must be set if using " +
            s"'spark.rapids.memory.pinnedPool.size")
        }
      }
    }
  }

  def calculateRecommendations(): Unit = {
    recommendExecutorInstances()
    val numExecutorCores = calcNumExecutorCores
    val execCoresExpr = () => numExecutorCores

    appendRecommendation("spark.executor.cores", numExecutorCores)
    appendRecommendation("spark.task.resource.gpu.amount",
      calcTaskGPUAmount(execCoresExpr))
    appendRecommendation("spark.rapids.sql.concurrentGpuTasks",
      calcGpuConcTasks().toInt)
    val availableMemPerExec = calcAvailableMemPerExec()
    val availableMemPerExecExpr = () => availableMemPerExec
    val executorHeap = calcExecutorHeap(availableMemPerExecExpr, execCoresExpr)
    val executorHeapExpr = () => executorHeap
    appendRecommendationForMemoryMB("spark.executor.memory", s"$executorHeap")
    val (pinnedMemory, memoryOverhead) =
      calcPinnedMemoryWithOverhead(executorHeapExpr, availableMemPerExecExpr)
    appendRecommendationForMemoryMB("spark.rapids.memory.pinnedPool.size", s"$pinnedMemory")
    addRecommendationForMemoryOverhead(s"$memoryOverhead")

    recommendMaxPartitionBytes()
    recommendShufflePartitions()
    recommendGeneralProperties()
  }

  /**
   * Checks whether the cluster properties are valid.
   * If the cluster worker-info is missing entries (i.e., CPU and GPU count), it sets the entries
   * to default values. For each default value, a comment is added to the [[comments]].
   *
   * @return false if the cluster properties are not loaded. e.g, all entries are set to 0.
   *         true if the missing information were updated to default initial values.
   */
  def processPropsAndCheck: Boolean = {
    if (clusterProps.system.getNumCores <= 0) {
      if (!clusterProps.isEmpty) {
        appendComment(
          s"Worker info has incorrect number of cores: ${clusterProps.system.getNumCores}.")
      }
      false
    } else {
      if (clusterProps.system.isMissingInfo) {
        appendComment(s"CPU properties is incomplete: ${clusterProps.system}.")
      }
      if (clusterProps.gpu.isMissingInfo) {
        val gpuComment =
          if (clusterProps.gpu.setDefaultGpuCountIfMissing()) {
            s"GPU count is missing. Setting default to ${clusterProps.gpu.getCount}."
          } else {
            s"GPU properties is incomplete: ${clusterProps.gpu}."
          }
        appendComment(gpuComment)
      }
      true
    }
  }

  private def recommendGeneralProperties(): Unit = {
    val aqeEnabled = getPropertyValue("spark.sql.adaptive.enabled").getOrElse("False")
    if (aqeEnabled == "False") {
      appendComment(commentsForMissingProps("spark.sql.adaptive.enabled"))
    }
    if (appInfo.isDefined) {
      val jvmGCFraction = appInfo.get.sqlTaskAggMetrics.map {
        taskMetrics => taskMetrics.jvmGCTimeSum * 1.0 / taskMetrics.executorCpuTime
      }
      if (jvmGCFraction.nonEmpty) { // avoid zero division
        if ((jvmGCFraction.sum / jvmGCFraction.size) > MAX_JVM_GCTIME_FRACTION) {
          appendComment("Average JVM GC time is very high. " +
            "Other Garbage Collectors can be used for better performance.")
        }
      }
    }
  }

  /**
   * Calculate max partition bytes using the max task input size and existing setting
   * for maxPartitionBytes. Note that this won't apply the same on iceberg.
   * The max bytes here does not distinguish between GPU and CPU reads so we could
   * improve that in the future.
   * Eg,
   * MIN_PARTITION_BYTES_RANGE = 128m, MAX_PARTITION_BYTES_RANGE = 256m
   * (1) Input:  maxPartitionBytes = 512m
   *             taskInputSize = 12m
   *     Output: newMaxPartitionBytes = 512m * (128m/12m) = 4g (hit max value)
   * (2) Input:  maxPartitionBytes = 2g
   *             taskInputSize = 512m,
   *     Output: newMaxPartitionBytes = 2g / (512m/128m) = 512m
   */
  private def calculateMaxPartitionBytes(maxPartitionBytes: String): String = {
    val app = appInfo.get
    // Autotuner only supports a single app right now, so we get whatever value is here
    val inputBytesOnGpuMax = if (app.maxTaskInputBytesReadGpu.nonEmpty) {
      app.maxTaskInputBytesReadGpu.head.maxTaskInputBytesReadGpu / 1024 / 1024
    } else {
      0.0
    }
    val maxPartitionBytesNum = convertToMB(maxPartitionBytes)
    if (inputBytesOnGpuMax == 0.0) {
      maxPartitionBytesNum.toString
    } else {
    if (inputBytesOnGpuMax > 0 &&
      inputBytesOnGpuMax < MIN_PARTITION_BYTES_RANGE_MB) {
      // Increase partition size
      val calculatedMaxPartitionBytes = Math.min(
        maxPartitionBytesNum *
          (MIN_PARTITION_BYTES_RANGE_MB / inputBytesOnGpuMax),
        MAX_PARTITION_BYTES_BOUND_MB)
      calculatedMaxPartitionBytes.toLong.toString
    } else if (inputBytesOnGpuMax > MAX_PARTITION_BYTES_RANGE_MB) {
      // Decrease partition size
      val calculatedMaxPartitionBytes = Math.min(
        maxPartitionBytesNum /
          (inputBytesOnGpuMax / MAX_PARTITION_BYTES_RANGE_MB),
        MAX_PARTITION_BYTES_BOUND_MB)
      calculatedMaxPartitionBytes.toLong.toString
    } else {
      // Do not recommend maxPartitionBytes
      null
    }
    }
  }

  /**
   * Recommendation for 'spark.sql.files.maxPartitionBytes' based on input size for each task.
   */
  private def recommendMaxPartitionBytes(): Unit = {
    val res =
      getPropertyValue("spark.sql.files.maxPartitionBytes") match {
        case None =>
          if (appInfo.isDefined) {
            calculateMaxPartitionBytes(MAX_PARTITION_BYTES)
          } else {
            s"${convertToMB(MAX_PARTITION_BYTES)}"
          }
        case Some(maxPartitionBytes) =>
          if (appInfo.isDefined) {
            calculateMaxPartitionBytes(maxPartitionBytes)
          } else {
            s"${convertToMB(maxPartitionBytes)}"
          }
      }
    appendRecommendationForMemoryMB("spark.sql.files.maxPartitionBytes", res)
  }

  /**
   * Recommendations for "spark.sql.shuffle.partitions'.
   * Note that this only recommend the default value for now.
   * The logic to calculate teh recommendations based on spills is disabled for now.
   */
  def recommendShufflePartitions(): Unit = {
    val lookup = "spark.sql.shuffle.partitions"
    val shufflePartitions =
      getPropertyValue(lookup).getOrElse(DEF_SHUFFLE_PARTITIONS).toInt
    // TODO: Need to look at other metrics for GPU spills (DEBUG mode), and batch sizes metric
    //    if (appInfo.isDefined) {
    //      val totalSpilledMetrics = appInfo.get.sqlTaskAggMetrics.map {
    //        task => task.diskBytesSpilledSum + task.memoryBytesSpilledSum
    //      }.sum
    //      if (totalSpilledMetrics > 0) {
    //        shufflePartitions *= DEF_SHUFFLE_PARTITION_MULTIPLIER
    //        // Could be memory instead of partitions
    //        appendOptionalComment(lookup,
    //          s"'$lookup' should be increased since spilling occurred.")
    //      }
    //    }
    appendRecommendation("spark.sql.shuffle.partitions", s"$shufflePartitions")
  }

  def appendOptionalComment(lookup: String, comment: String): Unit = {
    val skip = skipRecommendations match {
      case None => false
      case Some(criteriaSeq) => criteriaSeq.contains(lookup)
    }
    if (!skip) {
      appendComment(comment)
    }
  }

  def appendComment(comment: String): Unit = {
    comments += comment
  }

  def convertClusterPropsToString(): String = {
    clusterProps.toString
  }

  private def toCommentProfileResult:
  Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult)
  }

  private def toRecommendationsProfileResult: Seq[RecommendedPropertyResult] = {
    val finalRecommendations =
      recommendations.filter(elem => elem._2.isValid(filterByUpdatedPropertiesEnabled))
    finalRecommendations.collect {
      case (key, record) => RecommendedPropertyResult(key, record.recommended)
    }.toSeq.sortBy(_.property)
  }

  /**
   * The Autotuner loads the spark properties from either the ClusterProperties or the eventlog.
   * 1- runs the calculation for each criterion and saves it as a [[RecommendationEntry]].
   * 2- The final list of recommendations include any [[RecommendationEntry]] that has a
   *    recommendation that is different from the original property.
   * 3- Null values are excluded.
   * 4- A comment is added for each missing property in the spark property.
   *
   * @param skipList a list of recommendations to be skipped. If none, all recommendations are
   *                 returned. Default is None.
   * @param showOnlyUpdatedProps When enabled, the profiler recommendations should only include
   *                             updated settings.
   */
  def getRecommendedProperties(
      skipList: Option[Seq[String]] = None, showOnlyUpdatedProps: Boolean = true):
      (Seq[RecommendedPropertyResult], Seq[RecommendedCommentResult]) = {
    filterByUpdatedPropertiesEnabled = showOnlyUpdatedProps
    skipRecommendations = skipList
    if (processPropsAndCheck) {
      initRecommendations()
      calculateRecommendations()
    } else {
      // add all default comments
      commentsForMissingProps.foreach(commentEntry => appendComment(commentEntry._2))
    }
    (toRecommendationsProfileResult, toCommentProfileResult)
  }
}

object AutoTuner extends Logging {
  // Amount of GPU memory to use per concurrent task in megabytes.
  // Using a bit less than 8GB here since Dataproc clusters advertise T4s as only having
  // around 14.75 GB and we want to run with 2 concurrent by default on T4s.
  val DEF_GPU_MEM_PER_TASK_MB = 7500L
  // Maximum number of concurrent tasks to run on the GPU
  val MAX_CONC_GPU_TASKS = 4L
  // Amount of CPU memory to reserve for system overhead (kernel, buffers, etc.) in megabytes
  val DEF_SYSTEM_RESERVE_MB: Long = 2 * 1024L
  // Fraction of the executor JVM heap size that should be additionally reserved
  // for JVM off-heap overhead (thread stacks, native libraries, etc.)
  val DEF_HEAP_OVERHEAD_FRACTION = 0.1
  val MAX_JVM_GCTIME_FRACTION = 0.3
  // Ideal amount of JVM heap memory to request per CPU core in megabytes
  val DEF_HEAP_PER_CORE_MB: Long = 2 * 1024L
  // Maximum amount of pinned memory to use per executor in MB
  val MAX_PINNED_MEMORY_MB: Long = 4 * 1024L
  // value in MB
  val MIN_PARTITION_BYTES_RANGE_MB = 128L
  // value in MB
  val MAX_PARTITION_BYTES_RANGE_MB = 256L
  // value in MB
  val MAX_PARTITION_BYTES_BOUND_MB: Int = 4 * 1024
  val MAX_PARTITION_BYTES: String = "512m"
  val DEF_SHUFFLE_PARTITIONS = "200"
  val DEF_SHUFFLE_PARTITION_MULTIPLIER: Int = 2
  // GPU count defaults to 1 if it is missing.
  val DEF_WORKER_GPU_COUNT = 1
  val DEFAULT_WORKER_INFO_PATH = "./worker_info.yaml"
  val SUPPORTED_SIZE_UNITS: Seq[String] = Seq("b", "k", "m", "g", "t", "p")

  val commentsForMissingProps: mutable.Map[String, String] = mutable.LinkedHashMap[String, String](
    "spark.executor.memory" ->
      "'spark.executor.memory' should be set to at least 2GB/core.",
    "spark.executor.instances" ->
      "'spark.executor.instances' should be set to (gpuCount * numWorkers).",
    "spark.task.resource.gpu.amount" ->
      "'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).",
    "spark.rapids.sql.concurrentGpuTasks" ->
      s"'spark.rapids.sql.concurrentGpuTasks' should be set between [1, $MAX_CONC_GPU_TASKS]",
    "spark.rapids.memory.pinnedPool.size" ->
      s"'spark.rapids.memory.pinnedPool.size' should be set up to ${MAX_PINNED_MEMORY_MB}m.",
    "spark.sql.adaptive.enabled" ->
      "'spark.sql.adaptive.enabled' should be enabled for better performance.")

  val recommendationsTarget: Seq[String] = Seq[String](
    "spark.executor.instances",
    "spark.rapids.sql.enabled",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.rapids.sql.concurrentGpuTasks",
    "spark.task.resource.gpu.amount",
    "spark.sql.shuffle.partitions",
    "spark.sql.files.maxPartitionBytes",
    "spark.rapids.memory.pinnedPool.size",
    "spark.executor.memoryOverhead",
    "spark.executor.memoryOverheadFactor",
    "spark.kubernetes.memoryOverheadFactor")

  private def handleException(
      ex: Exception,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuner = {
    logError("Exception: " + ex.getStackTrace.mkString("Array(", ", ", ")"))
    val tuning = new AutoTuner(new ClusterProperties(), appInfo)
    val msg = ex match {
      case cEx: ConstructorException => cEx.getContext
      case _ => if (ex.getCause != null) ex.getCause.toString else ex.toString
    }
    tuning.appendComment(msg)
    tuning
  }

  def loadClusterPropertiesFromContent(clusterProps: String): Option[ClusterProperties] = {
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val yamlObjNested = new Yaml(new Constructor(classOf[ClusterProperties]), representer)
    Option(yamlObjNested.load(clusterProps).asInstanceOf[ClusterProperties])
  }

  def loadClusterProps(filePath: String): Option[ClusterProperties] = {
    val path = new Path(filePath)
    var fsIs: FSDataInputStream = null
    try {
      val fs = FileSystem.get(path.toUri, new Configuration())
      fsIs = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(fsIs))
      val fileContent = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
      loadClusterPropertiesFromContent(fileContent)
    } finally {
      if (fsIs != null) {
        fsIs.close()
      }
    }
  }

  /**
   * Similar to [[buildAutoTuner]] but it allows constructing the AutoTuner without an
   * existing file. This can be used in testing.
   *
   * @param clusterProps the cluster properties as string.
   * @param appInfo Optional of the profiling container.
   * @return a new AutoTuner object.
   */
  def buildAutoTunerFromProps(
      clusterProps: String,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuner = {
    try {
      val clusterPropsOpt = loadClusterPropertiesFromContent(clusterProps)
      new AutoTuner(clusterPropsOpt.getOrElse(new ClusterProperties()), appInfo)
    } catch {
      case e: Exception =>
        handleException(e, appInfo)
    }
  }

  def buildAutoTuner(
      filePath: String,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuner = {
    try {
      val clusterPropsOpt = loadClusterProps(filePath)
      new AutoTuner(clusterPropsOpt.getOrElse(new ClusterProperties()), appInfo)
    } catch {
      case e: Exception =>
        handleException(e, appInfo)
    }
  }

  /**
   * Converts size from human readable to bytes.
   * Eg, "4m" -> 4194304.
   */
  def convertFromHumanReadableSize(size: String): Long = {
    val sizesArr = size.toLowerCase.split("(?=[a-z])")
    val sizeNum = sizesArr(0).toDouble
    val sizeUnit = sizesArr(1)
    assert(SUPPORTED_SIZE_UNITS.contains(sizeUnit), s"$size is not a valid human readable size")
    (sizeNum * Math.pow(1024, SUPPORTED_SIZE_UNITS.indexOf(sizeUnit))).toLong
  }

  def containsMemoryUnits(size: String): Boolean = {
    val sizesArr = size.toLowerCase.split("(?=[a-z])")
    if (sizesArr.length > 1) {
      SUPPORTED_SIZE_UNITS.contains(sizesArr(1))
    } else {
      false
    }
  }

  def convertToMB(size: String): Long = {
    convertFromHumanReadableSize(size) / (1024 * 1024)
  }

  /**
   * Converts size from bytes to human readable.
   * Eg, 4194304 -> "4m", 633554 -> "618.70k".
   */
  def convertToHumanReadableSize(size: Long): String = {
    if (size < 0L) {
      return "0b"
    }

    val unitIndex = (Math.log10(size) / Math.log10(1024)).toInt
    assert(unitIndex < SUPPORTED_SIZE_UNITS.size,
      s"$size is too large to convert to human readable size")

    val sizeNum = size * 1.0/Math.pow(1024, unitIndex)
    val sizeUnit = SUPPORTED_SIZE_UNITS(unitIndex)

    // If sizeNum is an integer omit fraction part
    if ((sizeNum % 1) == 0) {
      f"${sizeNum.toLong}$sizeUnit"
    } else {
      f"$sizeNum%.2f$sizeUnit"
    }
  }

  /**
   * Reference - https://stackoverflow.com/a/55246235
   */
  def compareSparkVersion(version1: String, version2: String): Int = {
    val paddedVersions = version1.split("\\.").zipAll(version2.split("\\."), "0", "0")
    val difference = paddedVersions.find { case (a, b) => a != b }
    difference.fold(0) { case (a, b) => a.toInt - b.toInt }
  }
}
