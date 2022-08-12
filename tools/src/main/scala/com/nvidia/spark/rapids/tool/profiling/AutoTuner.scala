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

import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

/**
 * A wrapper class that stores all the properties that would be recommended by the Auto-tuner.
 * Separate getter and setter methods are specified for each property for ease of access.
 */
class Config {
  private val properties: collection.mutable.Map[String, String] = collection.mutable.Map(
    "spark.executor.instances" -> null,
    "spark.rapids.sql.enabled" -> null,
    "spark.executor.cores" -> null,
    "spark.executor.memory" -> null,
    "spark.rapids.sql.concurrentGpuTasks" -> null,
    "spark.task.resource.gpu.amount" -> null,
    "spark.sql.shuffle.partitions" -> null,
    "spark.sql.files.maxPartitionBytes" -> null,
    "spark.rapids.memory.pinnedPool.size" -> null,
    "spark.executor.memoryOverhead" -> null
  )

  def getExecutorInstances: Int = properties("spark.executor.instances").toInt

  def getExecutorCores: Int = properties("spark.executor.cores").toInt

  def getExecutorMemory: String = properties("spark.executor.memory")

  def getConcurrentGpuTasks: Int = properties("spark.rapids.sql.concurrentGpuTasks").toInt

  def getTaskResourceGpu: Double = properties("spark.task.resource.gpu.amount").toDouble

  def getShufflePartitions: Int = properties("spark.sql.shuffle.partitions").toInt

  def getMaxPartitionBytes: String = properties("spark.sql.files.maxPartitionBytes")

  def getPinnedPoolSize: String = properties("spark.rapids.memory.pinnedPool.size")

  def getExecutorMemoryOverhead: String = properties("spark.executor.memoryOverhead")

  def getAllProperties: Map[String, String] = properties.toMap

  def setExecutorInstances(numInstances: Int): Unit = {
    properties("spark.executor.instances") = numInstances.toString
  }

  def setExecutorCores(executorCores: Int): Unit = {
    properties("spark.executor.cores") = executorCores.toString
  }

  def setExecutorMemory(executorMemory: String): Unit = {
    properties("spark.executor.memory") = executorMemory
  }

  def setConcurrentGpuTasks(concurrentGpuTasks: Int): Unit = {
    properties("spark.rapids.sql.concurrentGpuTasks") = concurrentGpuTasks.toString
  }

  def setTaskResourceGpu(taskResourceGpu: Double): Unit = {
    properties("spark.task.resource.gpu.amount") = taskResourceGpu.toString
  }

  def setShufflePartitions(shufflePartitions: Int): Unit = {
    properties("spark.sql.shuffle.partitions") = shufflePartitions.toString
  }

  def setMaxPartitionBytes(maxPartitionBytes: String): Unit = {
    properties("spark.sql.files.maxPartitionBytes") = maxPartitionBytes
  }

  def setPinnedPoolSize(pinnedPoolSize: String): Unit = {
    properties("spark.rapids.memory.pinnedPool.size") = pinnedPoolSize
  }

  def setExecutorMemoryOverhead(executorMemoryOverhead: String): Unit = {
    properties("spark.executor.memoryOverhead") = executorMemoryOverhead
  }

  def setExecutorMemoryOverheadFactor(executorMemoryOverheadFactor: Double): Unit = {
    properties("spark.executor.memoryOverheadFactor") = executorMemoryOverheadFactor.toString
  }
}

/**
 * AutoTuner module that uses event logs and worker's system properties to recommend Spark
 * RAPIDS configuration based on heuristics.
 *
 * Example (Refer to test suite for more cases):
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
 *
 *    Output:
 *       Spark Properties:
 *       --conf spark.executor.cores=8
 *       --conf spark.executor.instances=32
 *       --conf spark.executor.memory=63.75g
 *       --conf spark.executor.memoryOverhead=8.38g
 *       --conf spark.rapids.memory.pinnedPool.size=2g
 *       --conf spark.rapids.sql.concurrentGpuTasks=4
 *       --conf spark.sql.files.maxPartitionBytes=31.67g
 *       --conf spark.sql.shuffle.partitions=200
 *       --conf spark.task.resource.gpu.amount=0.125
 *
 * b. Failure:
 *    Input: Empty or Incorrect System Properties
 *    Output:
 *      Comments:
 *      - 'spark.executor.memory' should be set to at least 2GB/core.
 *      - 'spark.executor.instances' should be set to 'num_gpus * num_workers'.
 *      - 'spark.task.resource.gpu.amount' should be set to 1/#cores.
 */
class AutoTuner(app: ApplicationSummaryInfo, workerInfo: String) extends Logging {
  import AutoTuner._
  val DEFAULT_SHUFFLE_PARTITION_MULTIPLIER: Int = 2
  val MAX_JVM_GCTIME_FRACTION: Double = 0.3

  val DEFAULT_CONCURRENT_GPU_TASKS_MULTIPLIER: Double = 0.125 // Currently aggressively set to 1/8
  val MAX_CONCURRENT_GPU_TASKS: Int = 4

  val DEFAULT_MAX_PARTITION_BYTES: String = "512m"
  val MAX_PARTITION_BYTES_BOUND: String = "4g"
  val MAX_PARTITION_BYTES_RANGE: String = "256m"
  val MIN_PARTITION_BYTES_RANGE: String = "128m"

  val DEFAULT_PINNED_POOL_SIZE: String = "2g"
  val DEFAULT_MEMORY_OVERHEAD_FACTOR: Double = 0.1
  val MIN_MEMORY_OVERHEAD: String = "2g"
  val MAX_EXTRA_SYSTEM_MEMORY: String = "2g"

  val MAX_PER_EXECUTOR_CORE_COUNT: Int = 16
  val MIN_PER_EXECUTOR_CORE_COUNT: Int = 4

  val MAX_EXECUTOR_MEMORY: String = "64g"
  val MIN_EXECUTOR_MEMORY: String = "8g"

  var comments: Seq[String] = Seq()

  private def recommendSparkProperties(recommendedConfig: Config,
      systemProps: SystemProps): Unit = {
    if (systemProps == null) {
      logWarning("System information is not available. Cannot recommend properties.")
      comments :+= "'spark.executor.memory' should be set to at least 2GB/core."
      comments :+= "'spark.executor.instances' should be set to 'num_gpus * num_workers'."
    } else {
      // Recommendation for 'spark.executor.instances' based on number of gpus and workers
      systemProps.numWorkers match {
        case Some(numWorkers) =>
          val numInstances = if (systemProps.gpuProps != null) {
            numWorkers * systemProps.gpuProps.count
          } else {
            numWorkers
          }

          recommendedConfig.setExecutorInstances(numInstances)
        case None =>
          val num_gpus_str = if (systemProps.gpuProps != null) {
            systemProps.gpuProps.count.toString
          } else {
            "num_gpus"
          }

          comments :+= s"'spark.executor.instances' should be set to $num_gpus_str * num_workers."
      }

      // Recommendation for 'spark.executor.cores' based on number of cpu cores and gpus
      val numCores: Int = if (systemProps.gpuProps != null) {
        Math.min(systemProps.numCores * 1.0 / systemProps.gpuProps.count,
          MAX_PER_EXECUTOR_CORE_COUNT).toInt
      } else {
        systemProps.numCores
      }

      if (numCores < MIN_PER_EXECUTOR_CORE_COUNT) {
        comments :+= s"Number of cores per executor is very low. " +
          s"It is recommended to have at least $MIN_PER_EXECUTOR_CORE_COUNT cores per executor."
      }

      if (systemProps.numWorkers.nonEmpty) {
        val numInstances = recommendedConfig.getExecutorInstances
        if (numCores * numInstances < systemProps.numCores) {
          comments :+= "Not all cores in the machine are being used. " +
            "It is recommended to use different machine."
        }
      }

      recommendedConfig.setExecutorCores(numCores)

      // Recommendation for 'spark.executor.memory' based on system memory, cluster scheduler
      // and num of gpus
      val sparkMaster = getSparkProperty(app, "spark.master")
      val systemMemoryNum: Long = convertFromHumanReadableSize(systemProps.memory)
      val extraSystemMemoryNum: Long = convertFromHumanReadableSize(MAX_EXTRA_SYSTEM_MEMORY)
      val effectiveSystemMemoryNum: Long =
        if (sparkMaster.contains("yarn") || sparkMaster.contains("k8s")) {
          systemMemoryNum - extraSystemMemoryNum -
            convertFromHumanReadableSize(MIN_MEMORY_OVERHEAD)
        } else {
          systemMemoryNum - extraSystemMemoryNum
        }
      val maxExecutorMemNum: Long = convertFromHumanReadableSize(MAX_EXECUTOR_MEMORY)

      val executorMemory: Long = if (systemProps.gpuProps != null) {
        Math.min(effectiveSystemMemoryNum * 1.0 / systemProps.gpuProps.count,
          maxExecutorMemNum).toLong
      } else {
        Math.min(effectiveSystemMemoryNum * 1.0 / numCores,
          maxExecutorMemNum).toLong
      }

      if(executorMemory < convertFromHumanReadableSize(MIN_EXECUTOR_MEMORY)) {
        comments :+= s"Executor memory is very low. " +
          s"It is recommended to have at least $MIN_EXECUTOR_MEMORY"
      }

      recommendedConfig.setExecutorMemory(convertToHumanReadableSize(executorMemory))

      // Recommendation for 'spark.sql.shuffle.partitions' based on spill size
      var shufflePartitions: Int = getSparkProperty(app, "spark.sql.shuffle.partitions")
        .getOrElse("200").toInt

      // TODO: Need to look at other metrics for GPU spills (DEBUG mode), and batch sizes metric
      val totalSpilledMetrics = app.sqlTaskAggMetrics.map {
        task => task.diskBytesSpilledSum + task.memoryBytesSpilledSum
      }.sum
      if (totalSpilledMetrics > 0) {
        shufflePartitions *= DEFAULT_SHUFFLE_PARTITION_MULTIPLIER
        // Could be memory instead of partitions
        comments :+= "\"spark.sql.shuffle.partitions\" should be increased since spilling occurred."
      }
      recommendedConfig.setShufflePartitions(shufflePartitions)

      // Recommendation for 'spark.sql.files.maxPartitionBytes' based on input size for each task
      getSparkProperty(app, "spark.sql.files.maxPartitionBytes") match {
        case None => recommendedConfig.setMaxPartitionBytes(DEFAULT_MAX_PARTITION_BYTES)
        case Some(maxPartitionBytes) =>
          val taskInputSize =
            app.sqlTaskAggMetrics.map(_.inputBytesReadAvg).sum / app.sqlTaskAggMetrics.size
          val maxPartitionBytesNum = convertFromHumanReadableSize(maxPartitionBytes)
          val newMaxPartitionBytes =
            if (taskInputSize < convertFromHumanReadableSize(MIN_PARTITION_BYTES_RANGE)) {
              // Increase partition size
              val calculatedMaxPartitionBytes = Math.max(
                maxPartitionBytesNum *
                  (convertFromHumanReadableSize(MIN_PARTITION_BYTES_RANGE) / taskInputSize),
                convertFromHumanReadableSize(MAX_PARTITION_BYTES_BOUND))

              convertToHumanReadableSize(calculatedMaxPartitionBytes.toLong)
            } else if (taskInputSize > convertFromHumanReadableSize(MAX_PARTITION_BYTES_RANGE)) {
              // Decrease partition size
              val calculatedMaxPartitionBytes = Math.max(
                maxPartitionBytesNum /
                  (taskInputSize / convertFromHumanReadableSize(MAX_PARTITION_BYTES_RANGE)),
                convertFromHumanReadableSize(MAX_PARTITION_BYTES_BOUND))

              convertToHumanReadableSize(calculatedMaxPartitionBytes.toLong)
            } else {
              // Do not recommend maxPartitionBytes
              null
            }

          recommendedConfig.setMaxPartitionBytes(newMaxPartitionBytes)
      }

      // Other general recommendations
      val aqeEnabled = getSparkProperty(app, "spark.sql.adaptive.enabled").getOrElse("False")
      if (aqeEnabled == "False") {
        comments :+= "'spark.sql.adaptive.enabled' should be enabled for better performance."
      }

      val jvmGCFraction = app.sqlTaskAggMetrics.map {
        taskMetrics => taskMetrics.jvmGCTimeSum * 1.0 / taskMetrics.executorCpuTime
      }
      if ((jvmGCFraction.sum / jvmGCFraction.size) > MAX_JVM_GCTIME_FRACTION) {
        comments :+= "Average JVM GC time is very high. " +
          "Other Garbage Collectors can be used for better performance"
      }
    }
  }

  /**
   * Recommend memory overhead as: pinnedPoolSize + (memoryOverheadFactor * executorMemory)
   */
  private def recommendMemoryOverhead(pinnedPoolSize: String, executorMemory: String): Long = {
    val pinnedPoolSizeNum = convertFromHumanReadableSize(pinnedPoolSize)
    val executorMemoryNum = convertFromHumanReadableSize(executorMemory)
    val minMemoryOverhead = convertFromHumanReadableSize(MIN_MEMORY_OVERHEAD)
    (pinnedPoolSizeNum + Math.max(minMemoryOverhead,
      DEFAULT_MEMORY_OVERHEAD_FACTOR * executorMemoryNum)).toLong
  }

  private def recommendGpuProperties(recommendedConfig: Config, systemProps: SystemProps): Unit = {
    if (systemProps == null || systemProps.gpuProps == null) {
      logWarning("GPU information is not available. Cannot recommend properties.")
      comments :+= "'spark.task.resource.gpu.amount' should be set to 1/#cores."
    } else {
      // Recommendation for 'spark.task.resource.gpu.amount' based on num of cpu cores and
      // 'spark.rapids.sql.concurrentGpuTasks' based on gpu memory
      val numGpus: Int = systemProps.gpuProps.count
      val numCores: Int = recommendedConfig.getExecutorCores

      val taskResourceGpu: Double = 1.0 / numCores
      val gpuMemoryNum: Long = convertFromHumanReadableSize(systemProps.gpuProps.memory)
      val concurrentGpuTasks: Int = Math.min(
        gpuMemoryNum * DEFAULT_CONCURRENT_GPU_TASKS_MULTIPLIER,
        MAX_CONCURRENT_GPU_TASKS).toInt

      recommendedConfig.setTaskResourceGpu(taskResourceGpu)
      recommendedConfig.setConcurrentGpuTasks(concurrentGpuTasks)

      if(numCores < concurrentGpuTasks) {
        comments :+= s"For the given GPU, number of CPU cores is very low. It should be" +
          s" at least equal to concurrent gpu tasks i.e. $concurrentGpuTasks."
      }

      // Recommendation for 'spark.executor.memoryOverhead', 'spark.executor.memoryOverheadFactor'
      // or 'spark.kubernetes.memoryOverheadFactor' based on cluster scheduler, spark
      // version. See recommendMemoryOverhead() for the calculation used.
      getSparkProperty(app, "spark.rapids.memory.pinnedPool.size") match {
        case Some(pinnedPoolSize) =>
          val sparkMaster = getSparkProperty(app, "spark.master")
          if (sparkMaster.contains("k8s")) {
            if (compareSparkVersion(app.appInfo.head.sparkVersion, "3.3.0") > 0) {
              if (getSparkProperty(app, "spark.executor.memoryOverheadFactor").isEmpty) {
                comments :+= "'spark.executor.memoryOverheadFactor' must be set " +
                  "if using 'spark.rapids.memory.pinnedPool.size'"
              }
            } else {
              if (getSparkProperty(app, "spark.kubernetes.memoryOverheadFactor").isEmpty) {
                comments :+= "'spark.kubernetes.memoryOverheadFactor' must be set " +
                  "if using 'spark.rapids.memory.pinnedPool.size'"
              }
            }
          } else if (sparkMaster.contains("yarn")) {
            if (getSparkProperty(app, "spark.executor.memoryOverhead").isEmpty) {
              val memoryOverhead = recommendMemoryOverhead(pinnedPoolSize,
                recommendedConfig.getExecutorMemory)
              recommendedConfig.setExecutorMemoryOverhead(
                convertToHumanReadableSize(memoryOverhead))
            }
          }

        case None =>
          recommendedConfig.setPinnedPoolSize(DEFAULT_PINNED_POOL_SIZE)
          val memoryOverhead = recommendMemoryOverhead(DEFAULT_PINNED_POOL_SIZE,
            recommendedConfig.getExecutorMemory)
          recommendedConfig.setExecutorMemoryOverhead(
            convertToHumanReadableSize(memoryOverhead))
      }
    }
  }

  /**
   * Entry point for generating recommendations.
   */
  def getRecommendedProperties: (Seq[RecommendedPropertyResult],
    Seq[RecommendedCommentResult]) = {
    val systemProps = parseSystemInfo(workerInfo)
    val recommendedConfig = new Config()
    recommendSparkProperties(recommendedConfig, systemProps)
    recommendGpuProperties(recommendedConfig, systemProps)
    (toPropertyProfileResult(recommendedConfig), toCommentProfileResult(comments))
  }
}

object AutoTuner extends Logging {
  val DEFAULT_WORKER_INFO: String = "."
  val SUPPORTED_SIZE_UNITS: Seq[String] = Seq("b", "k", "m", "g", "t", "p")

  /**
   * Parses the yaml file and returns system and gpu properties.
   * See [[SystemProps]] and [[GpuProps]].
   */
  def parseSystemInfo(yamlFile: String): SystemProps = {
     try {
       val yaml = new Yaml()
       val file = scala.io.Source.fromFile(yamlFile)
       val text = file.mkString
       val rawProps = yaml.load(text).asInstanceOf[java.util.Map[String, Any]]
         .asScala.toMap.filter { case (_, v) => v != null }
       val rawSystemProps = rawProps("system").asInstanceOf[java.util.Map[String, Any]]
         .asScala.toMap.filter { case (_, v) => v != null }

       if (rawSystemProps.nonEmpty) {
         val rawGpuProps = rawProps("gpu").asInstanceOf[java.util.Map[String, Any]]
           .asScala.toMap.filter { case (_, v) => v != null }

         val gpuProps = if (rawGpuProps.nonEmpty) {
           GpuProps(
             rawGpuProps("count").toString.toInt,
             rawGpuProps("memory").toString,
             rawGpuProps("name").toString)
         } else {
           null
         }

         SystemProps(
           rawSystemProps.getOrElse("num_cores", 1).toString.toInt,
           rawSystemProps.getOrElse("cpu_arch", "").toString,
           rawSystemProps.getOrElse("memory", "0b").toString,
           rawSystemProps.getOrElse("free_disk_space", "0b").toString,
           rawSystemProps.getOrElse("time_zone", "").toString,
           rawSystemProps.get("num_workers").map(_.toString.toInt),
           gpuProps)
       } else {
         null
       }
     } catch {
       case e: Exception =>
         logError("Exception: " + e.getStackTrace.mkString("Array(", ", ", ")"))
         null
     }
  }

  /**
   * Returns the value of Spark property from the application summary info.
   * [[RapidsPropertyProfileResult]] is defined as (key:key, rows: [key, value]).
   * Returns:
   * a. If the value is "null" or key is not found: None
   * b. Else: Some(value)
   */
  private def getSparkProperty(app: ApplicationSummaryInfo, property: String): Option[String] = {
    app.sparkProps.collectFirst {
      case propertyProfile: RapidsPropertyProfileResult
        if propertyProfile.key == property && propertyProfile.rows(1) != "null" =>
        propertyProfile.rows(1)
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

  /**
   * Converts size from bytes to human readable.
   * Eg, 4194304 -> "4m", 633554 -> "618.70k".
   */
  def convertToHumanReadableSize(size: Long): String = {
    if(size < 0) return "0b"

    val unitIndex = (Math.log10(size)/Math.log10(1024)).toInt
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

  private def toPropertyProfileResult(recommendedConfig:Config): Seq[RecommendedPropertyResult] = {
    val properties = recommendedConfig.getAllProperties
    properties.collect {
      case (property, value) if value != null => RecommendedPropertyResult(property, value)
    }.toSeq.sortBy(_.property)
  }

  private def toCommentProfileResult(comments:Seq[String]): Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult)
  }
}
