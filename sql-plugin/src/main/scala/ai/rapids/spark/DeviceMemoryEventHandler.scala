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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import ai.rapids.cudf.{Cuda, NvtxColor, NvtxRange, RmmEventHandler}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.bytesToString

object DeviceMemoryEventHandler {
  private val EXHAUSTED_POLL_INTERVAL_MSEC = 10
}

/**
 * RMM event handler to trigger spilling from the device memory store.
 * @param store device memory store that will be triggered to spill
 * @param startSpillThreshold memory allocation threshold that will trigger spill to start
 * @param endSpillThreshold memory allocation threshold that will stop spilling
 */
class DeviceMemoryEventHandler(
    store: RapidsDeviceMemoryStore,
    startSpillThreshold: Long,
    endSpillThreshold: Long) extends RmmEventHandler with Logging with AutoCloseable {

  private[this] val threadFactory: ThreadFactory = GpuDeviceManager.wrapThreadFactory(
    new ThreadFactory() {
      private[this] val factory = Executors.defaultThreadFactory()

      override def newThread(runnable: Runnable): Thread = {
        val t = factory.newThread(runnable)
        t.setName("device store spill")
        t.setDaemon(true)
        t
      }
    })

  private[this] val executor = Executors.newSingleThreadExecutor(threadFactory)

  private[this] val spillStream = new Cuda.Stream(true)

  private[this] val asyncSpillActive = new AtomicBoolean(false)

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
        logInfo(s"Targeting device store size of $targetSize bytes")
        store.synchronousSpill(targetSize)
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

  override def getAllocThresholds: Array[Long] = Array(startSpillThreshold)

  override def getDeallocThresholds: Array[Long] = Array(endSpillThreshold)

  override def onAllocThreshold(totalAllocated: Long): Unit = {
    try {
      if (asyncSpillActive.compareAndSet(false, true)) {
        logInfo(s"Asynchronous spill start triggered, total device memory " +
          s"allocated=${bytesToString(totalAllocated)}")

        // TODO: Because this is copying the buffer on a separate stream it needs to synchronize
        //       with the other stream that created the buffer, otherwise we could end up copying
        //       a buffer that is not done being computed.  Plan is to use events for this, but
        //       for now just turning this off until we have a fix.
        logWarning("ASYNCHRONOUS SPILL DISABLED DUE TO MISSING SYNCHRONIZATION")
//        executor.submit(new Runnable() {
//          override def run(): Unit = {
//            try {
//              asyncSpill()
//            } catch {
//              case t: Throwable => logError("Error during asynchronous spill", t)
//            }
//          }
//        })
      }
    } catch {
      case t: Throwable => logError("Error handling allocation threshold callback", t)
    }
  }

  override def onDeallocThreshold(totalAllocated: Long): Unit = {
    try {
      if (asyncSpillActive.compareAndSet(true, false)) {
        logInfo(s"Asynchronous spill stop triggered, total device memory " +
          s"allocated=${bytesToString(totalAllocated)}")
      }
    } catch {
      case t: Throwable => logError("Error handling deallocation threshold callback", t)
    }
  }

  /**
   * Spill device memory asynchronously using non-blocking CUDA streams
   * to avoid contending with activity on the default stream as much
   * as possible.
   */
  private def asyncSpill(): Unit = {
    val nvtx = new NvtxRange("spill", NvtxColor.ORANGE)
    try {
      var loggedSpillExhausted = false
      logInfo(s"Starting async device spill, device store " +
        s"size=${bytesToString(store.currentSize)}")
      var spillActive = asyncSpillActive.get

      while (spillActive) {
        if (!store.asyncSpillSingleBuffer(endSpillThreshold, spillStream)) {
          if (!loggedSpillExhausted) {
            logInfo(s"Device store has no more spillable buffers, resorting to polling")
            loggedSpillExhausted = true
          }
          val nvtx = new NvtxRange("sleeping", NvtxColor.PURPLE)
          try {
            Thread.sleep(DeviceMemoryEventHandler.EXHAUSTED_POLL_INTERVAL_MSEC)
          } catch {
            case _: InterruptedException =>
              logInfo(s"Async device spill polling interrupted")
              spillActive = false
          } finally {
            nvtx.close()
          }
        }
        spillActive = asyncSpillActive.get
      }

      logInfo(s"Async device spill complete, device store " +
        s"size=${bytesToString(store.currentSize)}")
    } finally {
      nvtx.close()
    }
  }

  override def close(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    spillStream.close()
  }
}
