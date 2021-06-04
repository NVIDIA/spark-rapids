/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.concurrent.ThreadFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.sql.rapids.GpuShuffleEnv

sealed trait MemoryState
private case object Initialized extends MemoryState
private case object Uninitialized extends MemoryState
private case object Errored extends MemoryState

object GpuDeviceManager extends Logging {
  // This config controls whether RMM/Pinned memory are initialized from the task
  // or from the executor side plugin. The default is to initialize from the
  // executor plugin.
  // var for testing purposes only!
  var rmmTaskInitEnabled = {
    java.lang.Boolean.getBoolean("com.nvidia.spark.rapids.memory.gpu.rmm.init.task")
  }

  // for testing only
  def setRmmTaskInitEnabled(enabled: Boolean): Unit = {
    rmmTaskInitEnabled = enabled
  }

  private val threadGpuInitialized = new ThreadLocal[Boolean]()
  @volatile private var singletonMemoryInitialized: MemoryState = Uninitialized
  @volatile private var deviceId: Option[Int] = None

  /**
   * Exposes the device id used while initializing the RMM pool
   */
  def getDeviceId(): Option[Int] = deviceId

  // Attempt to set and acquire the gpu, return true if acquired, false otherwise
  def tryToSetGpuDeviceAndAcquire(addr: Int): Boolean = {
    try {
      GpuDeviceManager.setGpuDeviceAndAcquire(addr)
    } catch {
      case NonFatal(e) =>
        logInfo(s"Will not use GPU $addr because of $e")
        // we may have lost a race trying to acquire this addr or GPU is already busy
        return false
    }
    return true
  }

  /**
   * This is just being extra paranoid when we are running on GPUs in exclusive mode. It is not
   * clear exactly what cuda interactions cause us to acquire a device, so this will force us
   * to do an interaction we know will acquire the device.
   */
  private def findGpuAndAcquire(): Int = {
    val deviceCount: Int = Cuda.getDeviceCount()
    // loop multiple times to see if a GPU was released or something unexpected happened that
    // we couldn't acquire on first try
    var numRetries = 2
    val addrsToTry = ArrayBuffer.empty ++= (0 until deviceCount)
    while (numRetries > 0) {
      val addr = addrsToTry.find(tryToSetGpuDeviceAndAcquire)
      if (addr.isDefined) {
        return addr.get
      }
      numRetries -= 1
    }
    throw new IllegalStateException("Could not find a single GPU to use")
  }

  def setGpuDeviceAndAcquire(addr: Int): Int = {
    logDebug(s"Initializing GPU device ID to $addr")
    Cuda.setDevice(addr.toInt)
    // cudaFree(0) to actually allocate the set device - no process exclusive required
    // since we are relying on Spark to schedule it properly and not give it to multiple
    // executors
    Cuda.freeZero()
    addr
  }

  def getGPUAddrFromResources(resources: Map[String, ResourceInformation]): Option[Int] = {
    if (resources.contains("gpu")) {
      val addrs = resources("gpu").addresses
      if (addrs.length > 1) {
        // Throw an exception since we assume one GPU per executor.
        // If multiple GPUs are allocated by spark, then different tasks could get assigned
        // different GPUs but RMM would only be initialized for 1. We could also just get
        // weird results that are hard to debug.
        throw new IllegalArgumentException("Spark GPU Plugin only supports 1 gpu per executor")
      }
      Some(addrs.head.toInt)
    } else {
      None
    }
  }

  // Initializes the GPU if Spark assigned one.
  // Returns either the GPU addr Spark assigned or None if Spark didn't assign one.
  def initializeGpu(resources: Map[String, ResourceInformation]): Option[Int] = {
    getGPUAddrFromResources(resources).map(setGpuDeviceAndAcquire(_))
  }

  def initializeGpuAndMemory(resources: Map[String, ResourceInformation]): Unit = {
    // Set the GPU before RMM is initialized if spark provided the GPU address so that RMM
    // uses that GPU. We only need to initialize RMM once per Executor because we are relying on
    // only 1 GPU per executor.
    // If Spark didn't provide the address we just use the default GPU.
    val addr = initializeGpu(resources)
    initializeMemory(addr)
  }

  def shutdown(): Unit = synchronized {
    // assume error during shutdown until we complete it
    singletonMemoryInitialized = Errored
    RapidsBufferCatalog.close()
    Rmm.shutdown()
    singletonMemoryInitialized = Uninitialized
  }

  def getResourcesFromTaskContext: Map[String, ResourceInformation] = {
    val tc = TaskContext.get()
    if (tc == null) Map.empty[String, ResourceInformation] else tc.resources()
  }

  /**
   * Always set the GPU if it was assigned by Spark and initialize the RMM if its configured
   * to do so in the task.
   * We expect the plugin to be run with 1 task and 1 GPU per executor.
   */
  def initializeFromTask(): Unit = {
    if (threadGpuInitialized.get() == false) {
      val resources = getResourcesFromTaskContext
      if (rmmTaskInitEnabled) {
        initializeGpuAndMemory(resources)
      } else {
        // just set the device if provided so task thread uses right GPU
        initializeGpu(resources)
      }
      threadGpuInitialized.set(true)
    }
  }

  private def toKB(x: Long): Double = x / 1024.0

  private def toMB(x: Long): Double = x / 1024 / 1024.0

  private def computeRmmInitSizes(conf: RapidsConf, info: CudaMemInfo): (Long, Long) = {
    // Align workaround for https://github.com/rapidsai/rmm/issues/527
    def truncateToAlignment(x: Long): Long = x & ~511L

    var initialAllocation = truncateToAlignment((conf.rmmAllocFraction * info.free).toLong)
    val minAllocation = truncateToAlignment((conf.rmmAllocMinFraction * info.total).toLong)
    if (initialAllocation < minAllocation) {
      throw new IllegalArgumentException(s"The initial allocation of " +
        s"${toMB(initialAllocation)} MB (calculated from ${RapidsConf.RMM_ALLOC_FRACTION} " +
        s"(=${conf.rmmAllocFraction}) and ${toMB(info.free)} MB free memory) was less than " +
        s"the minimum allocation of ${toMB(minAllocation)} (calculated from " +
        s"${RapidsConf.RMM_ALLOC_MIN_FRACTION} (=${conf.rmmAllocMinFraction}) " +
        s"and ${toMB(info.total)} MB total memory)")
    }
    val maxAllocation = truncateToAlignment((conf.rmmAllocMaxFraction * info.total).toLong)
    if (maxAllocation < initialAllocation) {
      throw new IllegalArgumentException(s"The initial allocation of " +
        s"${toMB(initialAllocation)} MB (calculated from ${RapidsConf.RMM_ALLOC_FRACTION} " +
        s"(=${conf.rmmAllocFraction}) and ${toMB(info.free)} MB free memory) was more than " +
        s"the maximum allocation of ${toMB(maxAllocation)} (calculated from " +
        s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=${conf.rmmAllocMaxFraction}) " +
        s"and ${toMB(info.total)} MB total memory)")
    }
    val reserveAmount = conf.rmmAllocReserve
    if (reserveAmount >= maxAllocation) {
      throw new IllegalArgumentException(s"RMM reserve memory (${toMB(reserveAmount)} MB) " +
          s"larger than maximum pool size (${toMB(maxAllocation)} MB). Check the settings for " +
          s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=${conf.rmmAllocFraction}) and " +
          s"${RapidsConf.RMM_ALLOC_RESERVE} (=$reserveAmount)")
    }
    val adjustedMaxAllocation = truncateToAlignment(maxAllocation - reserveAmount)
    if (initialAllocation > adjustedMaxAllocation) {
      logWarning(s"Initial RMM allocation (${toMB(initialAllocation)} MB) is larger than " +
          s"the adjusted maximum allocation (${toMB(adjustedMaxAllocation)} MB), " +
          "lowering initial allocation to the adjusted maximum allocation.")
      initialAllocation = adjustedMaxAllocation
    }

    if (!conf.isPooledMemEnabled || "none".equalsIgnoreCase(conf.rmmPool)) {
      (initialAllocation, 0)
    } else {
      (initialAllocation, adjustedMaxAllocation)
    }
  }

  private def initializeRmm(gpuId: Int, rapidsConf: Option[RapidsConf] = None): Unit = {
    if (!Rmm.isInitialized) {
      val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
      val info = Cuda.memGetInfo()
      val (initialAllocation, maxAllocation) = computeRmmInitSizes(conf, info)
      var init = RmmAllocationMode.CUDA_DEFAULT
      val features = ArrayBuffer[String]()
      if (conf.isPooledMemEnabled) {
        init = conf.rmmPool match {
          case c if "default".equalsIgnoreCase(c) =>
            if (Cuda.isPtdsEnabled) {
              logWarning("Configuring the DEFAULT allocator with a CUDF built for " +
                  "Per-Thread Default Stream (PTDS). This is known to be unstable! " +
                  "We recommend you use the ARENA allocator when PTDS is enabled.")
            }
            features += "POOLED"
            init | RmmAllocationMode.POOL
          case c if "arena".equalsIgnoreCase(c) =>
            features += "ARENA"
            init | RmmAllocationMode.ARENA
          case c if "none".equalsIgnoreCase(c) =>
            // Pooling is disabled.
            init
          case c =>
            throw new IllegalArgumentException(s"RMM pool set to '$c' is not supported.")
        }
      } else if (!"none".equalsIgnoreCase(conf.rmmPool)) {
        logWarning("RMM pool is disabled since spark.rapids.memory.gpu.pooling.enabled is set " +
          "to false; however, this configuration is deprecated and the behavior may change in a " +
          "future release.")
      }

      if (conf.isUvmEnabled) {
        init = init | RmmAllocationMode.CUDA_MANAGED_MEMORY
        features += "UVM"
      }

      val logConf: Rmm.LogConf = conf.rmmDebugLocation match {
        case c if "none".equalsIgnoreCase(c) => null
        case c if "stdout".equalsIgnoreCase(c) =>
          features += "LOG: STDOUT"
          Rmm.logToStdout()
        case c if "stderr".equalsIgnoreCase(c) =>
          features += "LOG: STDERR"
          Rmm.logToStdout()
        case c =>
          logWarning(s"RMM logging set to '$c' is not supported and is being ignored.")
          null
      }

      deviceId = Some(gpuId)

      logInfo(s"Initializing RMM${features.mkString(" ", " ", "")} " +
          s"initial size = ${toMB(initialAllocation)} MB, " +
          s"max size = ${toMB(maxAllocation)} MB on gpuId $gpuId")

      if (Cuda.isPtdsEnabled()) {
        logInfo("Using per-thread default stream")
      } else {
        logInfo("Using legacy default stream")
      }

      val (allocationAlignment, alignmentThreshold) =
        if (conf.isGdsSpillEnabled && conf.isGdsSpillAlignedIO) {
          logInfo(s"Using allocation alignment = ${toKB(RapidsGdsStore.AllocationAlignment)} KB, " +
              s"alignment threshold = ${toKB(conf.gdsSpillAlignmentThreshold)} KB")
          (RapidsGdsStore.AllocationAlignment, conf.gdsSpillAlignmentThreshold)
        } else {
          (0L, 0L)
        }

      try {
        Cuda.setDevice(gpuId)
        Rmm.initialize(
          init, logConf, initialAllocation, maxAllocation, allocationAlignment, alignmentThreshold)
        RapidsBufferCatalog.init(conf)
        GpuShuffleEnv.init(conf)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }
    }
  }

  private def allocatePinnedMemory(gpuId: Int, rapidsConf: Option[RapidsConf] = None): Unit = {
    val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
    if (!PinnedMemoryPool.isInitialized && conf.pinnedPoolSize > 0) {
      logInfo(s"Initializing pinned memory pool (${conf.pinnedPoolSize / 1024 / 1024.0} MB)")
      PinnedMemoryPool.initialize(conf.pinnedPoolSize, gpuId)
    }
  }

  /**
   * Initialize the GPU memory for gpuId according to the settings in rapidsConf.  It is assumed
   * that if gpuId is set then that gpu is already the default device.  If gpuId is not set
   * this will search all available GPUs starting at 0 looking for the appropriate one.
   * @param gpuId the id of the gpu to use
   * @param rapidsConf the config to use.
   */
  def initializeMemory(gpuId: Option[Int], rapidsConf: Option[RapidsConf] = None): Unit = {
    if (singletonMemoryInitialized != Initialized) {
      // Memory or memory related components that only need to be initialized once per executor.
      // This synchronize prevents multiple tasks from trying to initialize these at the same time.
      GpuDeviceManager.synchronized {
        if (singletonMemoryInitialized == Errored) {
          throw new IllegalStateException(
            "Cannot initialize memory due to previous shutdown failing")
        } else if (singletonMemoryInitialized == Uninitialized) {
          val gpu = gpuId.getOrElse(findGpuAndAcquire())
          initializeRmm(gpu, rapidsConf)
          allocatePinnedMemory(gpu, rapidsConf)
          singletonMemoryInitialized = Initialized
        }
      }
    }
  }

  /** Wrap a thread factory with one that will set the GPU device on each thread created. */
  def wrapThreadFactory(factory: ThreadFactory): ThreadFactory = new ThreadFactory() {
    private[this] val devId = getDeviceId.getOrElse {
      throw new IllegalStateException("Device ID is not set")
    }

    override def newThread(runnable: Runnable): Thread = {
      factory.newThread(() => {
        Cuda.setDevice(devId)
        runnable.run()
      })
    }
  }
}
