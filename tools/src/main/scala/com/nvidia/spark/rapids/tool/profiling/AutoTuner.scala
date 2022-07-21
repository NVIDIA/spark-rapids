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

  def get_executor_cores: Int = properties("spark.executor.cores").toInt

  def get_executor_memory: Long = properties("spark.executor.memory").toLong

  def get_concurrent_gpu_tasks: Int = properties("spark.rapids.sql.concurrentGpuTasks").toInt

  def get_task_resource_gpu: Double = properties("spark.task.resource.gpu.amount").toDouble

  def get_shuffle_partitions: Int = properties("spark.sql.shuffle.partitions").toInt

  def get_max_partition_bytes: String = properties("spark.sql.files.maxPartitionBytes")

  def get_pinned_pool_size: String = properties("spark.rapids.memory.pinnedPool.size")

  def get_executor_memory_overhead: String = properties("spark.executor.memoryOverhead")

  def get_all_properties: Map[String, String] = properties.toMap

  def set_executor_cores(executorCores: Int): Unit = {
    properties("spark.executor.cores") = executorCores.toString
  }

  def set_executor_memory(executorMemory: Long): Unit = {
    properties("spark.executor.memory") = executorMemory.toString
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
  val MAX_CONCURRENT_GPU_TASKS: Int = 4
  val DEFAULT_PINNED_POOL_SIZE: String = "2g"
  val DEFAULT_MEMORY_OVERHEAD_FACTOR: Double = 0.1

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
          rawGpuProps("memory_gb").toString.toLong,
          rawGpuProps("name").toString)
      } else null

      SystemProps(
        rawSystemProps.getOrElse("num_cores", 1).toString.toInt,
        rawSystemProps.getOrElse("cpu_arch", "").toString,
        rawSystemProps.getOrElse("memory_gb", 0).toString.toLong,
        rawSystemProps.getOrElse("free_disk_space_gb", 0).toString.toLong,
        rawSystemProps.getOrElse("time_zone", "").toString,
        gpuProps)
    } else null
  }

  private def getSparkProperty(property: String, default: String = null): String = {
    app.sparkProps.collectFirst {
      case propertyProfile if propertyProfile.key == property =>
        propertyProfile.rows(1)
    }.getOrElse(default)
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
      comments :+= "\"spark.executor.memory\" should be set to at least 2GB/core."
    } else {
      comments :+= "\"spark.executor.instances\" should be set to \"max(num_gpus, 1) * num_workers\"."

      val numCores: Int = if (systemProps.gpu_props != null) {
        systemProps.numCores / systemProps.gpu_props.count
      } else systemProps.numCores

      val totalMem: Long = numCores * DEFAULT_MEMORY_PER_CORE_MULTIPLIER
      bestConfig.set_executor_memory(totalMem)
      bestConfig.set_executor_cores(numCores)

      var shufflePartitions: Int = getSparkProperty("spark.sql.shuffle.partitions", "200").toInt

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

      val maxPartitionBytes: String = getSparkProperty("spark.sql.files.maxPartitionBytes", "2g")
      bestConfig.set_max_partition_bytes(maxPartitionBytes)
    }
  }

  private def recommendGpuProperties(bestConfig: Config, systemProps: SystemProps): Unit = {
    if (systemProps.gpu_props == null) {
      logWarning("GPU information is not available. Cannot recommend properties.")
      comments :+= "\"spark.task.resource.gpu.amount\" should be set to 1/#cores."
    } else {
      val numGpus: Int = systemProps.gpu_props.count
      val numCores: Int = bestConfig.get_executor_cores

      val taskResourceGpu: Double = 1.0 / numCores
      val concurrentGpuTasks: Int = Math.min(
        systemProps.gpu_props.memory * DEFAULT_CONCURRENT_GPU_TASKS_MULTIPLIER,
        MAX_CONCURRENT_GPU_TASKS).toInt

      bestConfig.set_task_resource_gpu(taskResourceGpu)
      bestConfig.set_concurrent_gpu_tasks(concurrentGpuTasks)

      val maxExecutorMemory = app.sqlTaskAggMetrics.map(_.peakExecutionMemoryMax).max
      val scanTimes = app.sqlMetrics.filter(_.name == "scan time").map(_.max_value)

      // TODO: Add heuristics for pinned pool size and overhead factor
      val pinnedPoolSize = DEFAULT_PINNED_POOL_SIZE
      bestConfig.set_pinned_pool_size(pinnedPoolSize)

      val sparkMaster = getSparkProperty("spark.master")
      if (sparkMaster.contains("yarn") || sparkMaster.contains("k8s")) {
        if (compareSparkVersion(app.appInfo.head.sparkVersion, "3.3.0") > 0) {
          bestConfig.set_executor_memory_overhead_factor(DEFAULT_MEMORY_OVERHEAD_FACTOR)
        } else {
          bestConfig.set_executor_memory_overhead(pinnedPoolSize)
        }
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

  // Spark version conf
  // Add an option for Recommender
  // I/O time - Scan time, Shuffle write time, Shuffle read time (in GPU)
  // numInstances - Needs to be cluster manager (YARN and K8s, will be ignored in standalone mode)
  // totalMem - Suggest a generic information (2g / core) or a script to get system information
  // maxPartitionBytes- Size processed by each task (may not be available in summary info)
  def getRecommendedProperties(): (Seq[RecommendedPropertyResult],
    Seq[RecommendedCommentResult])= {
    val systemProps = parseSystemInfo(workerInfo)
    val recommendedConfig = new Config()
    recommendSparkProperties(recommendedConfig, systemProps)
    recommendGpuProperties(recommendedConfig, systemProps)
    (toPropertyProfileResult(recommendedConfig), toCommentProfileResult(comments))
  }
}
