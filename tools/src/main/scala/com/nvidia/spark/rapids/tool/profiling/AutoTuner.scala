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

  def get_executor_instances: Int = properties("spark.executor.instances").toInt

  def get_executor_cores: Int = properties("spark.executor.cores").toInt

  def get_executor_memory: String = properties("spark.executor.memory")

  def get_concurrent_gpu_tasks: Int = properties("spark.rapids.sql.concurrentGpuTasks").toInt

  def get_task_resource_gpu: Double = properties("spark.task.resource.gpu.amount").toDouble

  def get_shuffle_partitions: Int = properties("spark.sql.shuffle.partitions").toInt

  def get_max_partition_bytes: String = properties("spark.sql.files.maxPartitionBytes")

  def get_pinned_pool_size: String = properties("spark.rapids.memory.pinnedPool.size")

  def get_executor_memory_overhead: String = properties("spark.executor.memoryOverhead")

  def get_all_properties: Map[String, String] = properties.toMap

  def set_executor_instances(numInstances: Int): Unit = {
    properties("spark.executor.instances") = numInstances.toString
  }

  def set_executor_cores(executorCores: Int): Unit = {
    properties("spark.executor.cores") = executorCores.toString
  }

  def set_executor_memory(executorMemory: String): Unit = {
    properties("spark.executor.memory") = executorMemory
  }

  def set_concurrent_gpu_tasks(concurrentGpuTasks: Int): Unit = {
    properties("spark.rapids.sql.concurrentGpuTasks") = concurrentGpuTasks.toString
  }

  def set_task_resource_gpu(taskResourceGpu: Double): Unit = {
    properties("spark.task.resource.gpu.amount") = taskResourceGpu.toString
  }

  def set_shuffle_partitions(shufflePartitions: Int): Unit = {
    properties("spark.sql.shuffle.partitions") = shufflePartitions.toString
  }

  def set_max_partition_bytes(maxPartitionBytes: String): Unit = {
    properties("spark.sql.files.maxPartitionBytes") = maxPartitionBytes
  }

  def set_pinned_pool_size(pinnedPoolSize: String): Unit = {
    properties("spark.rapids.memory.pinnedPool.size") = pinnedPoolSize
  }

  def set_executor_memory_overhead(executorMemoryOverhead: String): Unit = {
    properties("spark.executor.memoryOverhead") = executorMemoryOverhead
  }

  def set_executor_memory_overhead_factor(executorMemoryOverheadFactor: Double): Unit = {
    properties("spark.executor.memoryOverheadFactor") = executorMemoryOverheadFactor.toString
  }
}

class AutoTuner(app: ApplicationSummaryInfo, workerInfo: String) extends Logging {
  val DEFAULT_MEMORY_PER_CORE_MULTIPLIER: Int = 2
  val DEFAULT_SHUFFLE_PARTITION_MULTIPLIER: Int = 2
  val DEFAULT_CONCURRENT_GPU_TASKS_MULTIPLIER: Double = 0.125 // Currently aggressively set to 1/8
  val DEFAULT_MAX_PARTITION_BYTES: String = "512m"
  val DEFAULT_MAX_PARTITION_BYTES_MULTIPLIER: Int = 2
  val MAX_PARTITION_BYTES_RANGE: String = "256m"
  val MIN_PARTITION_BYTES_RANGE: String = "128m"
  val DEFAULT_PINNED_POOL_SIZE: String = "2g"
  val DEFAULT_MEMORY_OVERHEAD_FACTOR: Double = 0.1
  val MIN_MEMORY_OVERHEAD = "2g"
  val MAX_CONCURRENT_GPU_TASKS: Int = 4
  val MAX_PER_EXECUTOR_CORE_COUNT: Int = 16
  val MIN_PER_EXECUTOR_CORE_COUNT: Int = 4
  val MAX_EXECUTOR_MEMORY: String = "64g"
  val MIN_EXECUTOR_MEMORY: String = "8g"
  val MAX_JVM_GCTIME_FRACTION: Double = 0.3

  var comments: Seq[String] = Seq()
  var bestConfig: Config = _

  private def parseSystemInfo(inputFile: String): SystemProps = {
    val yaml = new Yaml()
    val file = scala.io.Source.fromFile(inputFile)
    val text = file.mkString
    val rawProps = yaml.load(text).asInstanceOf[java.util.Map[String, Any]]
      .asScala.toMap.filter { case (_, v) => v != null }
    val rawSystemProps = rawProps("system").asInstanceOf[java.util.Map[String, Any]]
      .asScala.toMap.filter { case (_, v) => v != null }

    if(rawSystemProps.nonEmpty) {
      val rawGpuProps = rawProps("gpu").asInstanceOf[java.util.Map[String, Any]]
        .asScala.toMap.filter { case (_, v) => v != null }

      val gpuProps = if (rawGpuProps.nonEmpty) {
        GpuProps(
          rawGpuProps("count").toString.toInt,
          rawGpuProps("memory").toString,
          rawGpuProps("name").toString)
      } else null

      SystemProps(
        rawSystemProps.getOrElse("num_cores", 1).toString.toInt,
        rawSystemProps.getOrElse("cpu_arch", "").toString,
        rawSystemProps.getOrElse("memory", "0b").toString,
        rawSystemProps.getOrElse("free_disk_space", "0b").toString,
        rawSystemProps.getOrElse("time_zone", "").toString,
        rawSystemProps.get("num_workers").map(_.toString.toInt),
        gpuProps)
    } else null
  }

  private def getSparkProperty(property: String): Option[String] = {
    app.sparkProps.collectFirst {
      case propertyProfile
        if propertyProfile.key == property && propertyProfile.rows(1) != "null" =>
        propertyProfile.rows(1)
    }
  }

  private def convertFromHumanReadableSize(size: String): Long = {
    val sizesArr = size.split("(?=\\D)")
    val sizeNum = sizesArr(0).toLong
    val sizeUnit = sizesArr(1)
    val units = Seq("b", "k", "m", "g", "t", "p", "e", "z", "y")
    assert(units.contains(sizeUnit), s"$size is not a valid human readable size")
    (sizeNum * Math.pow(1024, units.indexOf(sizeUnit))).toLong
  }

  private def convertToHumanReadableSize(size: Long): String = {
    if(size < 0) return "0b"

    val units = Seq("b", "k", "m", "g", "t", "p", "e", "z", "y")
    val unitIndex = (Math.log10(size)/Math.log10(1024)).toInt
    assert(unitIndex < units.size, s"$size is too large to convert to human readable size")

    val sizeNum = size/Math.pow(1024, unitIndex)
    val sizeUnit = units(unitIndex)

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
  private def compareSparkVersion(version1: String, version2: String): Int = {
    val paddedVersions = version1.split("\\.").zipAll(version2.split("\\."), "0", "0")
    val difference = paddedVersions.find { case (a, b) => a != b }
    difference.fold(0) { case (a, b) => a.toInt - b.toInt }
  }

  private def recommendSparkProperties(bestConfig: Config, systemProps: SystemProps): Unit = {
    if (systemProps == null) {
      logWarning("System information is not available. Cannot recommend properties.")
      comments :+= "'spark.executor.memory' should be set to at least 2GB/core."
      comments :+= "'spark.executor.instances' should be set to 'num_gpus * num_workers'."
    } else {
      systemProps.num_workers match {
        case Some(numWorkers) =>
          val numInstances = if (systemProps.gpu_props != null) {
            numWorkers * systemProps.gpu_props.count
          } else numWorkers
          bestConfig.set_executor_instances(numInstances)
        case None =>
          val num_gpus_str = if (systemProps.gpu_props != null) {
            systemProps.gpu_props.count.toString
          } else "num_gpus"

          comments :+= s"'spark.executor.instances' should be set to $num_gpus_str * num_workers."
      }

      val numCores: Int = if (systemProps.gpu_props != null) {
        Math.min(systemProps.numCores * 1.0 / systemProps.gpu_props.count,
          MAX_PER_EXECUTOR_CORE_COUNT).toInt
      } else systemProps.numCores

      if(numCores < MIN_PER_EXECUTOR_CORE_COUNT) {
        comments :+= s"Number of cores per executor is very low. " +
          s"It is recommended to have at least $MIN_PER_EXECUTOR_CORE_COUNT cores per executor."
      }

      if (systemProps.num_workers.nonEmpty) {
        val numInstances = bestConfig.get_executor_instances
        if (numCores * numInstances < systemProps.numCores) {
          comments :+= "Not all cores in the machine are being used. " +
            "It is recommended to use different machine."
        }
      }

      bestConfig.set_executor_cores(numCores)

      val systemMemoryNum: Long = convertFromHumanReadableSize(systemProps.memory)
      val maxExecutorMemNum: Long = convertFromHumanReadableSize(MAX_EXECUTOR_MEMORY)
      val executorMemory: Long = if (systemProps.gpu_props != null) {
        Math.min(systemMemoryNum * 1.0 / systemProps.gpu_props.count, maxExecutorMemNum).toLong
      } else Math.min(systemMemoryNum * 1.0 / numCores, maxExecutorMemNum).toLong

      if(executorMemory < convertFromHumanReadableSize(MIN_EXECUTOR_MEMORY)) {
        comments :+= s"Executor memory is very low. " +
          s"It is recommended to have at least $MIN_EXECUTOR_MEMORY"
      }

      bestConfig.set_executor_memory(convertToHumanReadableSize(executorMemory))

      var shufflePartitions: Int = getSparkProperty("spark.sql.shuffle.partitions")
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
      bestConfig.set_shuffle_partitions(shufflePartitions)

      getSparkProperty("spark.sql.files.maxPartitionBytes") match {
        case None => bestConfig.set_max_partition_bytes(DEFAULT_MAX_PARTITION_BYTES)
        case Some(maxPartitionBytes) =>
          val taskInputSize =
            app.sqlTaskAggMetrics.map(_.inputBytesReadAvg).sum / app.sqlTaskAggMetrics.size
          val maxPartitionBytesNum = convertFromHumanReadableSize(maxPartitionBytes)
          val newMaxPartitionBytes =
            if (taskInputSize < convertFromHumanReadableSize(MIN_PARTITION_BYTES_RANGE)) {
              // Increase partition size
              convertToHumanReadableSize(
                maxPartitionBytesNum * DEFAULT_MAX_PARTITION_BYTES_MULTIPLIER)
            } else if (taskInputSize > convertFromHumanReadableSize(MAX_PARTITION_BYTES_RANGE)) {
              // Decrease partition size
              convertToHumanReadableSize(
                maxPartitionBytesNum / DEFAULT_MAX_PARTITION_BYTES_MULTIPLIER)
            } else {
              // Do not recommend maxPartitionBytes
              null
            }

          bestConfig.set_max_partition_bytes(newMaxPartitionBytes)
      }

      val aqeEnabled = getSparkProperty("spark.sql.adaptive.enabled").getOrElse("False")
      if (aqeEnabled == "False") {
        comments :+= "'spark.sql.adaptive.enabled' should be enabled for better performance."
      }

      // TODO: Check if the fraction calculator is correct.
      val jvmGCFraction = app.sqlTaskAggMetrics.map {
        taskMetrics => taskMetrics.jvmGCTimeSum * 1.0 / taskMetrics.executorCpuTime
      }
      if ((jvmGCFraction.sum / jvmGCFraction.size) > MAX_JVM_GCTIME_FRACTION) {
        comments :+= "Average JVM GC time is very high. " +
          "Other Garbage Collectors can be used for better performance"
      }
    }
  }

  private def recommendMemoryOverhead(pinnedPoolSize: String, executorMemory: String): Long = {
    val pinnedPoolSizeNum = convertFromHumanReadableSize(pinnedPoolSize)
    val executorMemoryNum = convertFromHumanReadableSize(executorMemory)
    val minMemoryOverhead = convertFromHumanReadableSize(MIN_MEMORY_OVERHEAD)
    (pinnedPoolSizeNum + Math.max(minMemoryOverhead, 0.1 * executorMemoryNum)).toLong
  }

  private def recommendGpuProperties(bestConfig: Config, systemProps: SystemProps): Unit = {
    if (systemProps.gpu_props == null) {
      logWarning("GPU information is not available. Cannot recommend properties.")
      comments :+= "'spark.task.resource.gpu.amount' should be set to 1/#cores."
    } else {
      val numGpus: Int = systemProps.gpu_props.count
      val numCores: Int = bestConfig.get_executor_cores

      val taskResourceGpu: Double = 1.0 / numCores
      val gpuMemoryNum: Long = convertFromHumanReadableSize(systemProps.gpu_props.memory)
      val concurrentGpuTasks: Int = Math.min(
        gpuMemoryNum * DEFAULT_CONCURRENT_GPU_TASKS_MULTIPLIER,
        MAX_CONCURRENT_GPU_TASKS).toInt

      bestConfig.set_task_resource_gpu(taskResourceGpu)
      bestConfig.set_concurrent_gpu_tasks(concurrentGpuTasks)

      if(numCores < concurrentGpuTasks) {
        comments :+= s"For the given GPU, number of CPU cores is very low. It should be" +
          s" at least equal to concurrent gpu tasks i.e. $concurrentGpuTasks."
      }

      getSparkProperty("spark.rapids.memory.pinnedPool.size") match {
        case Some(pinnedPoolSize) =>
          val sparkMaster = getSparkProperty("spark.master")
          if (sparkMaster.contains("k8s")) {
            if (compareSparkVersion(app.appInfo.head.sparkVersion, "3.3.0") > 0) {
              if (getSparkProperty("spark.executor.memoryOverheadFactor").isEmpty) {
                comments :+= "'spark.executor.memoryOverheadFactor' must be set " +
                  "if using 'spark.rapids.memory.pinnedPool.size'"
              }
            } else {
              if (getSparkProperty("spark.kubernetes.memoryOverheadFactor").isEmpty) {
                comments :+= "'spark.kubernetes.memoryOverheadFactor' must be set " +
                  "if using 'spark.rapids.memory.pinnedPool.size'"
              }
            }
          } else if (sparkMaster.contains("yarn")) {
            if (getSparkProperty("spark.executor.memoryOverhead").isEmpty) {
              val memoryOverhead = recommendMemoryOverhead(pinnedPoolSize,
                bestConfig.get_executor_memory)
              bestConfig.set_executor_memory_overhead(convertToHumanReadableSize(memoryOverhead))
            }
          }

        case None =>
          bestConfig.set_pinned_pool_size(DEFAULT_PINNED_POOL_SIZE)
          val memoryOverhead = recommendMemoryOverhead(DEFAULT_PINNED_POOL_SIZE,
            bestConfig.get_executor_memory)
          bestConfig.set_executor_memory_overhead(convertToHumanReadableSize(memoryOverhead))
      }
    }
  }

  private def toPropertyProfileResult(recommendedConfig:Config): Seq[RecommendedPropertyResult] = {
    val properties = recommendedConfig.get_all_properties
    properties.collect {
      case (property, value) if value != null => RecommendedPropertyResult(property, value)
    }.toSeq.sortBy(_.property)
  }

  private def toCommentProfileResult(comments:Seq[String]): Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult)
  }

  def getRecommendedProperties(): (Seq[RecommendedPropertyResult],
    Seq[RecommendedCommentResult])= {
    val systemProps = parseSystemInfo(workerInfo)
    val recommendedConfig = new Config()
    recommendSparkProperties(recommendedConfig, systemProps)
    recommendGpuProperties(recommendedConfig, systemProps)
    (toPropertyProfileResult(recommendedConfig), toCommentProfileResult(comments))
  }
}
