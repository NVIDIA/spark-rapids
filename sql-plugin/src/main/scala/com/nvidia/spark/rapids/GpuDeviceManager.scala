/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import java.util.concurrent.{ThreadFactory, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.sql.internal.SQLConf
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

  private var numCores = 0

  /**
   * Get an approximate count on the number of cores this executor will use.
   */
  def getNumCores: Int = numCores

  // Memory resource used only for cudf::chunked_pack to allocate scratch space
  // during spill to host. This is done to set aside some memory for this operation
  // from the beginning of the job.
  var chunkedPackMemoryResource: Option[RmmPoolMemoryResource[RmmCudaMemoryResource]] = None

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

  @volatile private var poolSizeLimit = 0L

  // Never split below 100 MiB (but this is really just for testing)
  def getSplitUntilSize: Long = {
    val conf = new RapidsConf(SQLConf.get)
    conf.splitUntilSizeOverride
        .getOrElse(Math.max(poolSizeLimit / 8, 100 * 1024 * 1024))
  }

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

  def getGPUAddrFromResources(resources: Map[String, ResourceInformation],
      conf: RapidsConf): Option[Int] = {
    val sparkGpuResourceName = conf.getSparkGpuResourceName
    if (resources.contains(sparkGpuResourceName)) {
      logDebug(s"Spark resources contain: $sparkGpuResourceName")
      val addrs = resources(sparkGpuResourceName).addresses
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
  def initializeGpu(resources: Map[String, ResourceInformation], conf: RapidsConf): Option[Int] = {
    getGPUAddrFromResources(resources, conf).map(setGpuDeviceAndAcquire(_))
  }

  def initializeGpuAndMemory(resources: Map[String, ResourceInformation],
      conf: RapidsConf, numCores: Int): Unit = {
    this.numCores = numCores
    // as long in execute mode initialize everything because we could enable it after startup
    if (conf.isSqlExecuteOnGPU) {
      // Set the GPU before RMM is initialized if spark provided the GPU address so that RMM
      // uses that GPU. We only need to initialize RMM once per Executor because we are relying on
      // only 1 GPU per executor.
      // If Spark didn't provide the address we just use the default GPU.
      val addr = initializeGpu(resources, conf)
      initializeMemory(addr)
    }
  }

  def shutdown(): Unit = synchronized {
    // assume error during shutdown until we complete it
    singletonMemoryInitialized = Errored

    chunkedPackMemoryResource.foreach(_.close)
    chunkedPackMemoryResource = None
    poolSizeLimit = 0L

    RapidsBufferCatalog.close()
    GpuShuffleEnv.shutdown()
    // try to avoid segfault on RMM shutdown
    val timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    var isFirstTime = true
    while (Rmm.getTotalBytesAllocated > 0 && System.nanoTime() < timeout) {
      if (isFirstTime) {
        logWarning("Waiting for outstanding RMM allocations to be released...")
        isFirstTime = false
      }
      Thread.sleep(10)
    }
    if (System.nanoTime() >= timeout) {
      val remaining = Rmm.getTotalBytesAllocated
      if (remaining > 0) {
        logWarning(s"Shutting down RMM even though there are outstanding allocations $remaining")
      }
    }
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
   * We expect the plugin to be run with 1 GPU per executor.
   */
  def initializeFromTask(): Unit = {
    if (threadGpuInitialized.get() == false) {
      val resources = getResourcesFromTaskContext
      val conf = new RapidsConf(SparkEnv.get.conf)
      if (rmmTaskInitEnabled) {
        val numCores = RapidsPluginUtils.estimateCoresOnExec(SparkEnv.get.conf)
        initializeGpuAndMemory(resources, conf, numCores)
      } else {
        // just set the device if provided so task thread uses right GPU
        initializeGpu(resources, conf)
      }
      threadGpuInitialized.set(true)
    }
  }

  private def toMB(x: Long): Double = x / 1024 / 1024.0

  private def computeRmmPoolSize(conf: RapidsConf, info: CudaMemInfo): Long = {
    def truncateToAlignment(x: Long): Long = x & ~511L

    // No checks when rmmExactAlloc is given. We are just going to go with the amount requested
    // with the proper alignment (which is a requirement for some allocators). This is because
    // it is for testing and we assume that the tests know what they are doing. If the conf becomes
    // public, then we need to do some more work.
    conf.rmmExactAlloc.map(truncateToAlignment).getOrElse {
      val minAllocation = truncateToAlignment((conf.rmmAllocMinFraction * info.total).toLong)
      val maxAllocation = truncateToAlignment((conf.rmmAllocMaxFraction * info.total).toLong)
      val reserveAmount =
        if (conf.isUCXShuffleManagerMode && conf.rmmPool.equalsIgnoreCase("ASYNC")) {
          // When using the async allocator, UCX calls `cudaMalloc` directly to allocate the
          // bounce buffers.
          conf.rmmAllocReserve + conf.shuffleUcxBounceBuffersSize * 2
        } else {
          conf.rmmAllocReserve
        }
      var poolAllocation = truncateToAlignment(
        (conf.rmmAllocFraction * (info.free - reserveAmount)).toLong)
      if (poolAllocation < minAllocation) {
        throw new IllegalArgumentException(s"The pool allocation of " +
            s"${toMB(poolAllocation)} MB (calculated from ${RapidsConf.RMM_ALLOC_FRACTION} " +
            s"(=${conf.rmmAllocFraction}) and ${toMB(info.free)} MB free memory) was less than " +
            s"the minimum allocation of ${toMB(minAllocation)} (calculated from " +
            s"${RapidsConf.RMM_ALLOC_MIN_FRACTION} (=${conf.rmmAllocMinFraction}) " +
            s"and ${toMB(info.total)} MB total memory)")
      }
      if (maxAllocation < poolAllocation) {
        throw new IllegalArgumentException(s"The pool allocation of " +
            s"${toMB(poolAllocation)} MB (calculated from ${RapidsConf.RMM_ALLOC_FRACTION} " +
            s"(=${conf.rmmAllocFraction}) and ${toMB(info.free)} MB free memory) was more than " +
            s"the maximum allocation of ${toMB(maxAllocation)} (calculated from " +
            s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=${conf.rmmAllocMaxFraction}) " +
            s"and ${toMB(info.total)} MB total memory)")
      }
      if (reserveAmount >= maxAllocation) {
        throw new IllegalArgumentException(s"RMM reserve memory (${toMB(reserveAmount)} MB) " +
            s"larger than maximum pool size (${toMB(maxAllocation)} MB). Check the settings for " +
            s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=${conf.rmmAllocFraction}) and " +
            s"${RapidsConf.RMM_ALLOC_RESERVE} (=$reserveAmount)")
      }
      val adjustedMaxAllocation = truncateToAlignment(maxAllocation - reserveAmount)
      if (poolAllocation > adjustedMaxAllocation) {
        logWarning(s"RMM pool allocation (${toMB(poolAllocation)} MB) does not leave enough free " +
            s"memory for reserve memory (${toMB(reserveAmount)} MB), lowering the pool size to " +
            s"${toMB(adjustedMaxAllocation)} MB to accommodate the requested reserve amount.")
        poolAllocation = adjustedMaxAllocation
      }

      poolAllocation
    }
  }

  private def initializeRmm(gpuId: Int, rapidsConf: Option[RapidsConf]): Unit = {
    if (!Rmm.isInitialized) {
      val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))

      val poolSize = conf.chunkedPackPoolSize
      chunkedPackMemoryResource =
        if (poolSize > 0) {
          val chunkedPackPool =
            new RmmPoolMemoryResource(new RmmCudaMemoryResource(), poolSize, poolSize)
          logDebug(
            s"Initialized pool resource for spill operations " +
                s"of ${chunkedPackMemoryResource.map(_.getMaxSize)} Bytes")
          Some(chunkedPackPool)
        } else {
          None
        }

      val info = Cuda.memGetInfo()
      val poolAllocation = computeRmmPoolSize(conf, info)
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
          case c if "async".equalsIgnoreCase(c) =>
            features += "ASYNC"
            init | RmmAllocationMode.CUDA_ASYNC
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
        // Enable managed memory only if async allocator is not used.
        if ((init & RmmAllocationMode.CUDA_ASYNC) == 0) {
          features += "UVM"
          init = init | RmmAllocationMode.CUDA_MANAGED_MEMORY
        } else {
          throw new IllegalArgumentException(
            "CUDA Unified Memory is not supported in CUDA_ASYNC allocation mode");
        }
      }

      val logConf: Rmm.LogConf = conf.rmmDebugLocation match {
        case c if "none".equalsIgnoreCase(c) => null
        case c if "stdout".equalsIgnoreCase(c) =>
          features += "LOG: STDOUT"
          Rmm.logToStdout()
        case c if "stderr".equalsIgnoreCase(c) =>
          features += "LOG: STDERR"
          Rmm.logToStderr()
        case c =>
          logWarning(s"RMM logging set to '$c' is not supported and is being ignored.")
          null
      }

      deviceId = Some(gpuId)

      logInfo(s"Initializing RMM${features.mkString(" ", " ", "")} " +
          s"pool size = ${toMB(poolAllocation)} MB on gpuId $gpuId")

      if (Cuda.isPtdsEnabled()) {
        logInfo("Using per-thread default stream")
      } else {
        logInfo("Using legacy default stream")
      }

      Cuda.setDevice(gpuId)
      try {
        poolSizeLimit = poolAllocation
        Rmm.initialize(init, logConf, poolAllocation)
      } catch {
        case firstEx: CudfException if ((init & RmmAllocationMode.CUDA_ASYNC) != 0) => {
          logWarning("Failed to initialize RMM with ASYNC allocator. " +
            "Initializing with ARENA allocator as a fallback option.")
          init = init & (~RmmAllocationMode.CUDA_ASYNC) | RmmAllocationMode.ARENA
          try {
            Rmm.initialize(init, logConf, poolAllocation)
          } catch {
            case secondEx: Throwable => {
              logError(
                "Failed to initialize RMM with either ASYNC or ARENA allocators. Exiting...")
              secondEx.addSuppressed(firstEx)
              poolSizeLimit = 0L
              throw secondEx
            }
          }
        }
      }

      RapidsBufferCatalog.init(conf)
      GpuShuffleEnv.init(conf, RapidsBufferCatalog.getDiskBlockManager())
    }
  }

  private def initializeOffHeapLimits(gpuId: Int, rapidsConf: Option[RapidsConf]): Unit = {
    val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
    val setCuioDefaultResource = conf.pinnedPoolCuioDefault
    val (pinnedSize, nonPinnedLimit) = if (conf.offHeapLimitEnabled) {
      logWarning("OFF HEAP MEMORY LIMITS IS ENABLED. " +
          "THIS IS EXPERIMENTAL FOR NOW USE WITH CAUTION")
      val perTaskOverhead = conf.perTaskOverhead
      val totalOverhead = perTaskOverhead * GpuDeviceManager.numCores
      val confPinnedSize = conf.pinnedPoolSize
      val confLimit = conf.offHeapLimit
      // TODO the min limit size of overhead + 1 GiB is arbitrary and we should have some
      ///  better tests to see what an ideal value really should be.
      val minMemoryLimit = totalOverhead + (1024 * 1024 * 1024)

      val memoryLimit = if (confLimit.isDefined) {
        confLimit.get
      } else if (confPinnedSize > 0) {
        // TODO 1 GiB above the pinned size probably should change, we are using it to match the
        //  old behavior for the pool, but that is not great and we want to have hard evidence
        //  for a better value before just picking something else that is arbitrary
        val ret = confPinnedSize + (1024 * 1024 * 1024)
        logWarning(s"${RapidsConf.OFF_HEAP_LIMIT_SIZE} is not set using " +
            s"${RapidsConf.PINNED_POOL_SIZE} + 1 GiB instead for memory limit " +
            s"${ret / 1024 / 1024.0} MiB")
        ret
      } else {
        // We don't have pinned or a conf limit set, so go with the min
        logWarning(s"${RapidsConf.OFF_HEAP_LIMIT_SIZE} is not set and neither is " +
            s"${RapidsConf.PINNED_POOL_SIZE}. Using the minimum memory size for the limit " +
            s"which is ${RapidsConf.TASK_OVERHEAD_SIZE} * cores (${GpuDeviceManager.numCores}) " +
            s"+ 1 GiB => ${minMemoryLimit / 1024 / 1024.0} MiB")
        minMemoryLimit
      }

      val finalMemoryLimit = if (memoryLimit < minMemoryLimit) {
        logWarning(s"The memory limit ${memoryLimit / 1024 / 1024.0} MiB " +
            s"is smaller than the minimum limit size ${minMemoryLimit / 1024 / 1024.0} MiB " +
            s"using the minimum instead")
        minMemoryLimit
      } else {
        memoryLimit
      }

      // Now we need to know the pinned vs non-pinned limits
      val pinnedLimit = if (confPinnedSize + totalOverhead <= finalMemoryLimit) {
        confPinnedSize
      } else {
        val ret = finalMemoryLimit - totalOverhead
        logWarning(s"The configured pinned memory ${confPinnedSize / 1024 / 1024.0} MiB " +
            s"plus the overhead ${totalOverhead / 1024 / 1024.0} MiB " +
            s"is larger than the off heap limit ${finalMemoryLimit / 1024 / 1024.0} MiB " +
            s"dropping pinned memory to ${ret / 1024 / 1024.0} MiB")
        ret
      }
      val nonPinnedLimit = finalMemoryLimit - totalOverhead - pinnedLimit
      logWarning(s"Off Heap Host Memory configured to be " +
          s"${pinnedLimit / 1024 / 1024.0} MiB pinned, " +
          s"${nonPinnedLimit / 1024 / 1024.0} MiB non-pinned, and " +
          s"${totalOverhead / 1024 / 1024.0} MiB of untracked overhead.")
      (pinnedLimit, nonPinnedLimit)
    } else {
      (conf.pinnedPoolSize, -1L)
    }
    if (!PinnedMemoryPool.isInitialized && pinnedSize > 0) {
      logInfo(s"Initializing pinned memory pool (${pinnedSize / 1024 / 1024.0} MiB)")
      PinnedMemoryPool.initialize(pinnedSize, gpuId, setCuioDefaultResource)
    }
    // Host memory limits must be set after the pinned memory pool is initialized
    HostAlloc.initialize(nonPinnedLimit)
  }

  /**
   * Initialize the GPU memory for gpuId according to the settings in rapidsConf.  It is assumed
   * that if gpuId is set then that gpu is already the default device.  If gpuId is not set
   * this will search all available GPUs starting at 0 looking for the appropriate one.
   *
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
          initializeOffHeapLimits(gpu, rapidsConf)
          singletonMemoryInitialized = Initialized
        }
      }
    }
  }

  /** Wrap a thread factory with one that will set the GPU device on each thread created. */
  def wrapThreadFactory(factory: ThreadFactory,
      before: () => Unit = null,
      after: () => Unit = null): ThreadFactory = new ThreadFactory() {
    private[this] val devId = getDeviceId.getOrElse {
      throw new IllegalStateException("Device ID is not set")
    }

    override def newThread(runnable: Runnable): Thread = {
      factory.newThread(() => {
        Cuda.setDevice(devId)
        try {
          if (before != null) {
            before()
          }
          runnable.run()
        } finally {
          if (after != null) {
            after()
          }
        }
      })
    }
  }
}
