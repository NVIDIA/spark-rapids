/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf._

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceInformation

object GpuDeviceManager extends Logging {
  // This config controls whether RMM/Pinned memory are initialized from the task
  // or from the executor side plugin. The default is to initialize from the
  // executor plugin.
  // var for testing purposes only!
  var rmmTaskInitEnabled = {
    java.lang.Boolean.getBoolean("ai.rapids.spark.memory.gpu.rmm.init.task")
  }

  // for testing only
  def setRmmTaskInitEnabled(enabled: Boolean): Unit = {
    rmmTaskInitEnabled = enabled
  }

  private val threadGpuInitialized = new ThreadLocal[Boolean]()
  @volatile private var memoryListenerInitialized: Boolean = false
  @volatile private var singletonMemoryInitialized: Boolean = false

  // Attempt to set and acquire the gpu, return true if acquired, false otherwise
  def tryToSetGpuDeviceAndAcquire(addr: Int): Boolean = {
    try {
      GpuDeviceManager.setGpuDeviceAndAcquire(addr)
    } catch {
      case NonFatal(e) =>
        // we may have lost a race trying to acquire this addr or GPU is already busy
        return false
    }
    return true
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
      if (addrs.size > 1) {
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

  private def initializeRmm(rapidsConf: Option[RapidsConf] = None): Unit = {
    if (!Rmm.isInitialized) {
      val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
      val loggingEnabled = conf.isRmmDebugEnabled
      val info = Cuda.memGetInfo()
      val initialAllocation = (conf.rmmAllocFraction * info.total).toLong
      if (initialAllocation > info.free) {
        logWarning(s"Initial RMM allocation(${initialAllocation / 1024 / 1024.0} MB) is " +
          s"larger than free memory(${info.free / 1024 / 1024.0} MB)")
      }
      var init = RmmAllocationMode.CUDA_DEFAULT
      val features = ArrayBuffer[String]()
      if (conf.isPooledMemEnabled) {
        init = init | RmmAllocationMode.POOL
        features += "POOLED"
      }
      if (conf.isUvmEnabled) {
        init = init | RmmAllocationMode.CUDA_MANAGED_MEMORY
        features += "UVM"
      }

      logInfo(s"Initializing RMM${features.mkString(" ", " ", "")} ${initialAllocation / 1024 / 1024.0} MB")
      try {
        Rmm.initialize(init, loggingEnabled, initialAllocation)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }
    }
  }

  private def registerMemoryListener(rapidsConf: Option[RapidsConf] = None): Unit = {
    if (memoryListenerInitialized == false) {
      GpuDeviceManager.synchronized {
        if (memoryListenerInitialized == false) {
          val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
          MemoryListener.registerDeviceListener(GpuResourceManager)
          val info = Cuda.memGetInfo()
          val async = (conf.rmmAsyncSpillFraction * info.total).toLong
          val stop = (conf.rmmSpillFraction * info.total).toLong
          logInfo(s"MemoryListener setting cutoffs, async: $async stop: $stop")
          GpuResourceManager.setCutoffs(async, stop)
          memoryListenerInitialized = true
        }
      }
    }
  }

  private def allocatePinnedMemory(gpuId: Option[Int], rapidsConf: Option[RapidsConf] = None): Unit = {
    val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
    if (!PinnedMemoryPool.isInitialized && conf.pinnedPoolSize > 0) {
      logInfo(s"Initializing pinned memory pool (${conf.pinnedPoolSize / 1024 / 1024.0} MB)")
      PinnedMemoryPool.initialize(conf.pinnedPoolSize, gpuId.getOrElse(-1))
    }
  }

  def initializeMemory(gpuId: Option[Int], rapidsConf: Option[RapidsConf] = None): Unit = {
    if (singletonMemoryInitialized == false) {
      // Memory or memory related components that only need to be initialized once per executor.
      // This synchronize prevents multiple tasks from trying to initialize these at the same time.
      GpuDeviceManager.synchronized {
        if (singletonMemoryInitialized == false) {
          registerMemoryListener(rapidsConf)
          initializeRmm(rapidsConf)
          allocatePinnedMemory(gpuId, rapidsConf)
          singletonMemoryInitialized = true
        }
      }
    }
  }
}
