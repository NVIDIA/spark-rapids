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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.{Constructor, ConstructorException}
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.internal.Logging

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
  override def toString: String =
    s"{Count: $count, Memory: $memory, GpuDevice: $name}"
}

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
  override def toString: String =
    s"{numCores: $numCores, Memory: $memory, numWorkers: $numWorkers}"
}

class ClusterProperties(
    @BeanProperty var system: SystemClusterProps,
    @BeanProperty var gpu: GpuWorkerProps,
    @BeanProperty var softwareProperties: util.LinkedHashMap[String, String]) {

  import AutoTuning._

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

class RecommendationEntry(val name: String, val originalValue: String, var recommended: String) {
  def setRecommendedValue(value: String): Unit = {
    if (value != null) {
      recommended = value
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
 */
class AutoTuning(
    val clusterProps: ClusterProperties,
    val appInfo: Option[ApplicationSummaryInfo])  extends Logging {

  import AutoTuning._

  var comments: Seq[String] = Seq()
  var recommendations: mutable.LinkedHashMap[String, RecommendationEntry] =
    mutable.LinkedHashMap[String, RecommendationEntry]()

  def getSparkPropertyFromProfile(propKey: String): Option[String] = {
    if (appInfo.isDefined) {
      appInfo.get.sparkProps.collectFirst {
        case entry: RapidsPropertyProfileResult
          if entry.key == propKey && entry.rows(1) != "null" => entry.rows(1)
      }
    } else {
      None
    }
  }

  def getPropertyValue(key: String): Option[String] = {
    val fromProfile = getSparkPropertyFromProfile(key)
    if (fromProfile.isDefined) {
      fromProfile
    } else {
      Option(clusterProps.softwareProperties.get(key))
    }
  }

  def initRecommendations(): Unit = {
    recommendationsTarget.foreach { key =>
      val propVal = getPropertyValue(key)
      if (propVal.isDefined) {
        val recommendationVal = new RecommendationEntry(key, propVal.get, null)
        recommendations(key) = recommendationVal
      }
    }
  }

  def appendRecommendation(key: String, value: String): Unit = {
    val recomRecord = recommendations.getOrElseUpdate(key,
      new RecommendationEntry(key, null, null))
    recomRecord.setRecommendedValue(value)
    if (recomRecord.originalValue == null && value != null) {
      // add a comment
      appendComment(s"'$key' was not set.")
    }
  }

  def appendRecommendationForMemoryMB(key: String, value: String): Unit = {
    if (value != null) {
      appendRecommendation(key, s"${value}m")
    }
  }

  /**
   * Recommendation for 'spark.executor.instances' based on number of gpus and workers.
   * Assumption - In case GPU properties are not available, it assumes 1 GPU per worker.
   */
  def calcExecInstances(): Int = {
    // spark.executor.instances
    clusterProps.gpu.getCount * clusterProps.system.numWorkers
  }

  /**
   * Recommendation for 'spark.executor.cores' based on number of cpu cores and gpus.
   */
  def calcNumExecutorCores(): Int = {
    // spark.executor.cores
    val executorsPerNode = clusterProps.gpu.getCount
    if (executorsPerNode == 0) {
      1
    } else {
      Math.max(1, clusterProps.system.numCores / executorsPerNode)
    }
  }

  /**
   * Recommendation for 'spark.task.resource.gpu.amount' based on num of cpu cores.
   */
  def calcTaskGPUAmount(numExecCoresClaculator: () => Int): Double = {
    // spark.task.resource.gpu.amount
    val numExecutorCores =  numExecCoresClaculator.apply()
    if (numExecutorCores == 0) {
      0.0
    } else {
      1.0 / numExecutorCores
    }
  }

  /**
   * Recommendation for 'spark.rapids.sql.concurrentGpuTasks' based on gpu memory.
   */
  def calcGpuConcTasks(): Long = {
    // spark.rapids.sql.concurrentGpuTasks
    Math.min(MAX_CONC_GPU_TASKS,
      convertToMB(clusterProps.gpu.memory) / DEF_GPU_MEM_PER_TASK_MB)
  }

  private def calcExecutorContainerMem(): Double = {
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
  def calcExecutorMemory(executorContainerMemCalculator: () => Double,
      numExecCoresClaculator: () => Int): Long = {
    // spark.executor.memory
    // reserve 10% of heap as memory overhead
    val maxExecutorHeap = Math.max(0,
      executorContainerMemCalculator.apply() * (1 - DEF_HEAP_OVERHEAD_FRACTION)).toInt
    // give up to 2GB of heap to each executor core
    Math.min(maxExecutorHeap, DEF_HEAP_PER_CORE_MB * numExecCoresClaculator.apply())
  }

  def calcPinnedMemoryWithOverhead(
      execMemoryCalculator: (() => Double, () => Int) => Double,
      containerMemCalculator: () => Double,
      execCoresClaculator: () => Int): (Long, Long) = {
    // spark.rapids.memory.pinnedPool.size, spark.executor.memoryOverhead
    val executorHeap =
      execMemoryCalculator.apply(containerMemCalculator, execCoresClaculator)
    var executorMemOverhead = (executorHeap * DEF_HEAP_OVERHEAD_FRACTION).toLong
    // pinned memory uses any unused space up to 4GB
    val pinnedMem = Math.min(MAX_PINNED_MEMORY_MB,
      containerMemCalculator.apply() - executorHeap - executorMemOverhead).toLong
    executorMemOverhead += pinnedMem
    (pinnedMem, executorMemOverhead)
  }

  def calculateRecommendations(): Unit = {
    appendRecommendation("spark.executor.instances", s"${calcExecInstances()}")
    // spark.executor.cores
    appendRecommendation("spark.executor.cores", s"${calcNumExecutorCores()}")
    // spark.task.resource.gpu.amount
    appendRecommendation("spark.task.resource.gpu.amount",
      s"${calcTaskGPUAmount(calcNumExecutorCores)}")
    // spark.rapids.sql.concurrentGpuTasks
    appendRecommendation("spark.rapids.sql.concurrentGpuTasks",
      s"${calcGpuConcTasks()}")
    appendRecommendationForMemoryMB("spark.executor.memory",
      s"${calcExecutorMemory(calcExecutorContainerMem, calcNumExecutorCores)}")
    val (pinnedMemory, memoryOverhead) =
      calcPinnedMemoryWithOverhead(
        calcExecutorMemory, calcExecutorContainerMem, calcNumExecutorCores)
    appendRecommendationForMemoryMB("spark.executor.memoryOverhead", s"$memoryOverhead")
    appendRecommendationForMemoryMB("spark.rapids.memory.pinnedPool.size", s"$pinnedMemory")

    recommendMaxPartitionBytes()
    recommendShufflePartitions()
    recommendGeneralProperties()
  }

  def validateProperties(): Boolean = {
    if (clusterProps.isEmpty) {
      false
    } else {
      if (clusterProps.gpu.isMissingInfo) {
        appendComment("GPU properties is incomplete")
      }
      if (clusterProps.system.isMissingInfo) {
        appendComment("CPU properties is incomplete")
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
   * Calculate max partition bytes using task input size.
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
    val maxPartitionBytesNum = convertToMB(maxPartitionBytes)
    if (app.sqlTaskAggMetrics.isEmpty) { // avoid division by zero
      maxPartitionBytesNum.toString
    } else {
      val taskInputSizeInMB =
        (app.sqlTaskAggMetrics.map(_.inputBytesReadAvg).sum / app.sqlTaskAggMetrics.size) / 1024
      if (taskInputSizeInMB > 0 &&
        taskInputSizeInMB < MIN_PARTITION_BYTES_RANGE) {
        // Increase partition size
        val calculatedMaxPartitionBytes = Math.min(
          maxPartitionBytesNum *
            (MIN_PARTITION_BYTES_RANGE / taskInputSizeInMB),
          MAX_PARTITION_BYTES_BOUND)
        calculatedMaxPartitionBytes.toLong.toString
      } else if (taskInputSizeInMB > MAX_PARTITION_BYTES_RANGE) {
        // Decrease partition size
        val calculatedMaxPartitionBytes = Math.min(
          maxPartitionBytesNum /
            (taskInputSizeInMB / MAX_PARTITION_BYTES_RANGE),
          MAX_PARTITION_BYTES_BOUND)
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
            convertToMB(MAX_PARTITION_BYTES).toString
          }
        case Some(maxPartitionBytes) =>
          calculateMaxPartitionBytes(maxPartitionBytes)
      }
    appendRecommendationForMemoryMB("spark.sql.files.maxPartitionBytes", res)
  }

  private def recommendShufflePartitions(): Unit = {
    var shufflePartitions =
      getPropertyValue("spark.sql.shuffle.partitions").getOrElse(DEF_SHUFFLE_PARTITIONS).toInt
    // TODO: Need to look at other metrics for GPU spills (DEBUG mode), and batch sizes metric
    if (appInfo.isDefined) {
      val totalSpilledMetrics = appInfo.get.sqlTaskAggMetrics.map {
        task => task.diskBytesSpilledSum + task.memoryBytesSpilledSum
      }.sum
      if (totalSpilledMetrics > 0) {
        shufflePartitions *= DEF_SHUFFLE_PARTITION_MULTIPLIER
        // Could be memory instead of partitions
        appendComment("'spark.sql.shuffle.partitions' should be increased since spilling occurred.")
      }
      appendRecommendation("spark.sql.shuffle.partitions", s"$shufflePartitions")
    }
  }

  def appendComment(comment: String): Unit = {
    comments :+= comment
  }

  def convertClusterPropsToString(): String = {
    clusterProps.toString
  }

  private def toCommentProfileResult(comments:Seq[String]): Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult)
  }

  private def toRecommendationsProfileResult: Seq[RecommendedPropertyResult] = {
    val finalRecommendations =
      if (FILTER_RECOMMENDATIONS) {
        recommendations.filter(elem =>
          elem._2.recommended != null && elem._2.recommended != elem._2.originalValue)
      } else {
        recommendations
      }
    finalRecommendations.collect {
      case (key, record) => RecommendedPropertyResult(key, record.recommended)
    }.toSeq.sortBy(_.property)
  }

  def getRecommendedProperties: (Seq[RecommendedPropertyResult], Seq[RecommendedCommentResult]) = {
    if (validateProperties()) {
      initRecommendations()
      calculateRecommendations()
    } else {
      // add all default comments
      commentsForMissingProps.foreach(commentEntry => appendComment(commentEntry._2))
    }
    (toRecommendationsProfileResult, toCommentProfileResult(comments))
  }
}

object AutoTuning extends Logging {
  val FILTER_RECOMMENDATIONS = true
  val DEF_GPU_MEM_PER_TASK_MB = 7500L
  val MAX_CONC_GPU_TASKS = 4L
  // Amount of CPU memory to reserve for system overhead (kernel, buffers, etc.) in megabytes
  val DEF_SYSTEM_RESERVE_MB: Long = 2 * 1024L
  // Fraction of the executor JVM heap size that should be additionally reserved
  // for JVM off-heap overhead (thread stacks, native libraries, etc.)
  val DEF_HEAP_OVERHEAD_FRACTION = 0.1
  val MAX_JVM_GCTIME_FRACTION = 0.3
  // Ideal amount of JVM heap memory to request per CPU core in megabytes
  val DEF_HEAP_PER_CORE_MB: Long = 2 * 1024L
  // Maximum amount of pinned memory to use per executor in megabytes
  val MAX_PINNED_MEMORY_MB: Long = 4 * 1024L
  // value in MB
  val MIN_PARTITION_BYTES_RANGE = 128L
  // value in MB
  val MAX_PARTITION_BYTES_RANGE = 256L
  // value in MB
  val MAX_PARTITION_BYTES_BOUND: Int = 4 * 1024
  val MAX_PARTITION_BYTES: String = "512m"
  val DEF_SHUFFLE_PARTITIONS = "200"
  val DEF_SHUFFLE_PARTITION_MULTIPLIER: Int = 2
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
    "spark.executor.memoryOverhead")

  private def handleException(
      ex: Exception,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuning = {
    logError("Exception: " + ex.getStackTrace.mkString("Array(", ", ", ")"))
    val tuning = new AutoTuning(new ClusterProperties(), appInfo)
    val msg = ex match {
      case cEx: ConstructorException => cEx.getContext
      case _ => if (ex.getCause != null) ex.getCause.toString else ex.toString
    }
    tuning.appendComment(msg)
    tuning
  }

  def loadClusterPropertiesFromContent(clusterProps: String): Option[ClusterProperties] = {
    val representer = new Representer
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

  def buildAutoTuningFromProps(
      clusterProps: String,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuning = {
    try {
      val clusterPropsOpt = loadClusterPropertiesFromContent(clusterProps)
      new AutoTuning(clusterPropsOpt.getOrElse(new ClusterProperties()), appInfo)
    } catch {
      case e: Exception =>
        handleException(e, appInfo)
    }
  }

  def buildAutoTuning(
      filePath: String,
      appInfo: Option[ApplicationSummaryInfo]): AutoTuning = {
    try {
      val clusterPropsOpt = loadClusterProps(filePath)
      new AutoTuning(clusterPropsOpt.getOrElse(new ClusterProperties()), appInfo)
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
}
