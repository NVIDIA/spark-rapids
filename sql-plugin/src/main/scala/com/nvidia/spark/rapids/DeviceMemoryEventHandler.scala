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

package com.nvidia.spark.rapids

import java.io.File
import java.lang.management.ManagementFactory

import ai.rapids.cudf.{NvtxColor, NvtxRange, Rmm, RmmEventHandler}
import com.sun.management.HotSpotDiagnosticMXBean

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * RMM event handler to trigger spilling from the device memory store.
 * @param store device memory store that will be triggered to spill
 * @param oomDumpDir local directory to create heap dumps on GPU OOM
 */
class DeviceMemoryEventHandler(
    store: RapidsDeviceMemoryStore,
    oomDumpDir: Option[String],
    isGdsSpillEnabled: Boolean) extends RmmEventHandler with Logging {

  /**
   * Handles RMM allocation failures by spilling buffers from device memory.
   * @param allocSize the byte amount that RMM failed to allocate
   * @return true if allocation should be reattempted or false if it should fail
   */
  override def onAllocFailure(allocSize: Long): Boolean = {
    try {
      val nvtx = new NvtxRange("onAllocFailure", NvtxColor.RED)
      try {
        val storeSize = store.currentSize
        logInfo(s"Device allocation of $allocSize bytes failed, device store has " +
            s"$storeSize bytes. Total RMM allocated is ${Rmm.getTotalBytesAllocated} bytes.")
        if (storeSize == 0) {
          logWarning(s"Device store exhausted, unable to allocate $allocSize bytes. " +
              s"Total RMM allocated is ${Rmm.getTotalBytesAllocated} bytes.")
          oomDumpDir.foreach(heapDump)
          return false
        }
        val targetSize = Math.max(storeSize - allocSize, 0)
        logDebug(s"Targeting device store size of $targetSize bytes")
        val amountSpilled = store.synchronousSpill(targetSize)
        logInfo(s"Spilled $amountSpilled bytes from the device store")
        if (isGdsSpillEnabled) {
          TrampolineUtil.incTaskMetricsDiskBytesSpilled(amountSpilled)
        } else {
          TrampolineUtil.incTaskMetricsMemoryBytesSpilled(amountSpilled)
        }
        true
      } finally {
        nvtx.close()
      }
    } catch {
      case t: Throwable =>
        logError(s"Error handling allocation failure", t)
        false
    }
  }

  override def getAllocThresholds: Array[Long] = null

  override def getDeallocThresholds: Array[Long] = null

  override def onAllocThreshold(totalAllocated: Long): Unit = {
  }

  override def onDeallocThreshold(totalAllocated: Long): Unit = {
  }

  private def heapDump(dumpDir: String): Unit = {
    val dumpPath = getDumpPath(dumpDir)
    logWarning(s"Dumping heap to $dumpPath")
    val server = ManagementFactory.getPlatformMBeanServer
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(server,
      "com.sun.management:type=HotSpotDiagnostic", classOf[HotSpotDiagnosticMXBean])
    mxBean.dumpHeap(dumpPath, false)
  }

  private def getDumpPath(dumpDir: String): String = {
    // pid is typically before the '@' character in the name
    val pid = ManagementFactory.getRuntimeMXBean.getName.split('@').head
    new File(dumpDir, s"gpu-oom-$pid.hprof").toString
  }
}
