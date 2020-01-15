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

import ai.rapids.cudf._

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

object GpuDeviceManager extends Logging {
  // var for testing purposes only!
  var rmmTaskInitEnabled = {
    val propstr = System.getProperty("ai.rapids.spark.memory.gpu.rmm.init.task")
    if (propstr != null) {
      java.lang.Boolean.parseBoolean(propstr)
    } else {
      true
    }
  }

  // for testing only
  def setRmmTaskInitEnabled(enabled: Boolean): Unit = {
    rmmTaskInitEnabled = enabled
  }

  private val threadGpuInitialized = new ThreadLocal[Boolean]()
  @volatile private var memoryListenerInitialized: Boolean = false

  /**
   * Set the GPU assigned by Spark and initialize the RMM.
   * We expect the plugin to be run with 1 task and 1 GPU per executor.
   */
  def initialize(): Unit = {
    if (rmmTaskInitEnabled) {
      if (threadGpuInitialized.get() == false) {
        val tc = TaskContext.get()
        if (tc != null && tc.resources().contains("gpu")) {
          val addrs = tc.resources()("gpu").addresses
          if (addrs.size > 1) {
            throw new IllegalArgumentException("Spark GPU Plugin only supports 1 gpu per executor")
          }
          val addr = addrs.head
          logInfo(s"Initializing gpu device to id: $addr")
          Cuda.setDevice(addr.toInt)
          // cudaFree(0) to actually allocate the set device - no process exclusive required
          // since we are relying on Spark to schedule it properly and not give it to multiple
          // executors
          Cuda.freeZero()
          // Set gpu before RMM initialized so RMM uses that GPU.
          // We only need to initialize RMM once per Executor because are relying on
          // only 1 GPU per executor, we just need to set the GPU for each task.
          initializeMemory(addr.toInt)
        } else {
          // here we assume Spark GPU scheduling is not enabled and we rely on GPU being
          // in process exclusive mode
          logInfo("No GPU assigned by Spark, just initializing RMM and memory")
          initializeMemory(-1)
        }
        threadGpuInitialized.set(true)
      }
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

  private def allocatePinnedMemory(gpuId: Int, rapidsConf: Option[RapidsConf] = None): Unit = {
    val conf = rapidsConf.getOrElse(new RapidsConf(SparkEnv.get.conf))
    if (!PinnedMemoryPool.isInitialized && conf.pinnedPoolSize > 0) {
      logInfo(s"Initializing pinned memory pool (${conf.pinnedPoolSize / 1024 / 1024.0} MB)")
      PinnedMemoryPool.initialize(conf.pinnedPoolSize, gpuId)
    }
  }

  def initializeMemory(gpuId: Int, rapidsConf: Option[RapidsConf] = None): Unit = {
    registerMemoryListener(rapidsConf)
    initializeRmm(rapidsConf)
    allocatePinnedMemory(gpuId, rapidsConf)
  }
}