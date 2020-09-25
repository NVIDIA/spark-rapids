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

import ai.rapids.cudf.{NvtxColor, NvtxRange, RmmEventHandler}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * RMM event handler to trigger spilling from the device memory store.
 * @param store device memory store that will be triggered to spill
 */
class DeviceMemoryEventHandler(store: RapidsDeviceMemoryStore)
    extends RmmEventHandler with Logging {

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
        logInfo(s"Device allocation of $allocSize bytes failed," +
            s" device store has $storeSize bytes")
        if (storeSize == 0) {
          logWarning("Device store exhausted, unable to satisfy "
              + s"allocation of $allocSize bytes")
          return false
        }
        val targetSize = Math.max(storeSize - allocSize, 0)
        logDebug(s"Targeting device store size of $targetSize bytes")
        val amountSpilled = store.synchronousSpill(targetSize)
        logInfo(s"Spilled $amountSpilled bytes from the device store")
        TrampolineUtil.incTaskMetricsMemoryBytesSpilled(amountSpilled)
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
}
