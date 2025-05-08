/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, Future}

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar as GpuShuffleCoalesceIterator, but pulling in host batches asynchronously, to
 * overlap the host batch reading and the downstream GPU operations.
 */
class GpuShuffleAsyncCoalesceIterator(iter: Iterator[CoalescedHostResult],
    dataTypes: Array[DataType],
    outputBatchesMetric: GpuMetric = NoopMetric,
    outputRowsMetric: GpuMetric = NoopMetric,
    asyncReadTimeMetric: GpuMetric = NoopMetric,
    opTimeMetric: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {

  private val readExecutor =
    TrampolineUtil.newDaemonSingleThreadExecutor("async shuffle read")

  Option(TaskContext.get()).foreach( tc =>
    // Install a listener to to close the async read thread.
    tc.addTaskCompletionListener[Unit](_ => readExecutor.shutdown())
  )

  // don't try to call TaskContext.get().taskAttemptId() in the backend thread
  private val taskAttemptID = Option(TaskContext.get()).
    map(_.taskAttemptId().toString).getOrElse("unknown")

  private lazy val readCallable = new Callable[CoalescedHostResult]() {
    // The actual async read, including the host batches read and concatenation in
    // "HostCoalesceIteratorBase.next()".
    override def call(): CoalescedHostResult = {
      val nvRangeName = s"Task ${taskAttemptID}-Async Read Batch"
      withResource(new NvtxRange(nvRangeName, NvtxColor.BLUE)) { _ =>
        iter.next()
      }
    }
  }

  private var readFutureOpt: Option[Future[CoalescedHostResult]] = None

  override def hasNext(): Boolean = GpuMetric.ns(asyncReadTimeMetric, opTimeMetric) {
    readFutureOpt.isDefined || {
      // No async read is running when it comes here, so no need synchronization
      // when accessing the input iterator. "iter.hasNext" should be lightweight
      // enough, since it just read in a header which is very small.
      iter.hasNext
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches")
    }
    val nvRangeName = s"Task ${taskAttemptID}-Batch to GPU"
    withResource(new NvtxRange(nvRangeName, NvtxColor.BLUE)) { _ =>
      val hostConcatedRet = GpuMetric.ns(asyncReadTimeMetric, opTimeMetric) {
        readFutureOpt.map { readFuture =>
          // An async read is running, waiting for the result
          readFuture.get()
        }.getOrElse { // The first batch, just read it directly
          iter.next()
        }
      }
      val gpuCB = withResource(hostConcatedRet) { _ =>
        // We acquire the GPU regardless of whether the concatenated batch is an empty batch
        // or not, because the downstream tasks expect the `GpuShuffleCoalesceIterator`
        // to acquire the semaphore and may generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        GpuMetric.ns(opTimeMetric)(hostConcatedRet.toGpuBatch(dataTypes))
      }
      closeOnExcept(gpuCB) { _ =>
        val hasNextCB = GpuMetric.ns(asyncReadTimeMetric, opTimeMetric)(iter.hasNext)
        GpuMetric.ns(opTimeMetric) {
          // No need synchronization here since the async read is already done.
          if (hasNextCB) {
            // Prefetch and concatenate the next one asynchronously.
            readFutureOpt = Some(readExecutor.submit(readCallable))
          } else {
            readFutureOpt = None
          }
          outputBatchesMetric += 1
          outputRowsMetric += gpuCB.numRows()
          gpuCB
        }
      }
    }
  }

}
