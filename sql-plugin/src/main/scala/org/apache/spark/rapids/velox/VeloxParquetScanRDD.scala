/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.velox

import java.util.concurrent.locks.ReentrantLock

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric, NvtxWithMetrics}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.velox.RapidsHostColumn

import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class VeloxParquetScanRDD(scanRDD: RDD[ColumnarBatch],
                          outputAttr: Seq[Attribute],
                          outputSchema: StructType,
                          coalesceGoal: CoalesceSizeGoal,
                          metrics: Map[String, GpuMetric],
                          useNativeConverter: Boolean,
                          preloadedCapacity: Int
                         ) extends RDD[InternalRow](scanRDD.sparkContext, Nil) {

  private val veloxScanTime = GpuMetric.unwrap(metrics("VeloxScanTime"))

  override protected def getPartitions: Array[Partition] = scanRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // the wrapping Iterator for the underlying VeloxScan task
    val veloxIter = new VeloxScanMetricsIter(scanRDD.compute(split, context), veloxScanTime)

    val resIter = if (!useNativeConverter) {
      VeloxColumnarBatchConverter.roundTripConvert(
        veloxIter, outputAttr, coalesceGoal, metrics)
    } else {
      // Preloading only works for NativeConverter because using roundTripConverter we
      // can NOT split the building process of HostColumnVector and the host2device process,
      // since they are completed in one by GpuRowToColumnConverter.

      val schema = StructType(outputAttr.map { ar =>
        StructField(ar.name, ar.dataType, ar.nullable)
      })
      require(coalesceGoal.targetSizeBytes <= Int.MaxValue,
        s"targetSizeBytes should be smaller than 2GB, but got ${coalesceGoal.targetSizeBytes}"
      )
      val coalesceConverter = new CoalesceNativeConverter(
        veloxIter, coalesceGoal.targetSizeBytes.toInt, schema, metrics
      )

      val hostIter: RapidsHostBatchProducer = if (preloadedCapacity > 0) {
        val producerInitFn = () => {
          coalesceConverter.setRuntime()
        }
        PrefetchHostBatchProducer(context.taskAttemptId(),
          coalesceConverter,
          producerInitFn,
          preloadedCapacity,
          metrics("preloadWaitTime")
        )
      } else {
        coalesceConverter.setRuntime()
        SyncHostBatchProducer(coalesceConverter)
      }

      VeloxColumnarBatchConverter.hostToDevice(hostIter, outputAttr, metrics)
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, resIter.asInstanceOf[Iterator[InternalRow]])
  }

}

private class VeloxScanMetricsIter(iter: Iterator[ColumnarBatch],
                                   scanTime: SQLMetric
                                  ) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = {
    val start = System.nanoTime()
    try {
      iter.hasNext
    } finally {
      scanTime += System.nanoTime() - start
    }
  }

  override def next(): ColumnarBatch = {
    val start = System.nanoTime()
    try {
      iter.next()
    } finally {
      scanTime += System.nanoTime() - start
    }
  }
}

private case class PrefetchHostBatchProducer(taskAttId: Long,
                                             iterImpl: Iterator[Array[RapidsHostColumn]],
                                             producerInitFn: () => Unit,
                                             capacity: Int,
                                             waitTimeMetric: GpuMetric
                                            ) extends RapidsHostBatchProducer with Logging {

  @transient
  @volatile private var isInit: Boolean = false
  @transient
  @volatile private var isProducing: Boolean = false
  @transient @volatile private var readIndex: Int = 0
  @transient @volatile private var writeIndex: Int = 0
  // This lock guarantees anytime if ProducerStatus == running there must be a working batch
  // being produced or waiting to be put into the queue.
  @transient private lazy val hasNextLock = new ReentrantLock()

  @transient private lazy val emptyLock = new ReentrantLock()
  @transient private lazy val fullLock = new ReentrantLock()

  private var producer: Thread = _

  @transient private lazy val buffer: Array[Either[Throwable, Array[RapidsHostColumn]]] = {
    Array.ofDim[Either[Throwable, Array[RapidsHostColumn]]](capacity)
  }

  @transient private lazy val produceFn: Runnable = new Runnable {

    // This context will be got in the main Thread during the initialization of `produceFn`
    private val taskContext: TaskContext = TaskContext.get()

    override def run(): Unit = {
      TrampolineUtil.setTaskContext(taskContext)
      hasNextLock.lock()
      try {
        do {
          isProducing = true
          hasNextLock.unlock()

          do {
            fullLock.synchronized {
              if (writeIndex - readIndex == capacity) {
                fullLock.wait()
              }
            }
          } while (writeIndex - readIndex == capacity)

          buffer(writeIndex % capacity) = Right(iterImpl.next())
          emptyLock.synchronized {
            writeIndex += 1
            emptyLock.notify()
          }

          hasNextLock.lock()
          isProducing = false
          logInfo(s"[$taskAttId] PreloadedIterator produced $writeIndex batches, " +
            s"currently preloaded batchNum: ${writeIndex - readIndex}"
          )
        }
        while (iterImpl.hasNext)
        hasNextLock.unlock()
      } catch {
        case ex: Throwable =>
          // transfer the exception info to the main thread as an interrupted signal
          buffer(writeIndex % capacity) = Left(ex)
          writeIndex += 1
          isProducing = false
          if (hasNextLock.isHeldByCurrentThread) {
            hasNextLock.unlock()
          }
          throw new RuntimeException(ex)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def hasNext: Boolean = {
    if (!isInit) {
      withResource(new NvtxWithMetrics("waitForCPU", NvtxColor.RED, waitTimeMetric)) { _ =>
        if (!iterImpl.hasNext) {
          return false
        }
        isInit = true
        isProducing = true
        producerInitFn()
        producer = new Thread(produceFn)
        producer.start()
        return true
      }
    }

    writeIndex > readIndex || {
      hasNextLock.lock()
      val ret = writeIndex > readIndex || isProducing
      hasNextLock.unlock()
      ret
    }
  }

  override def waitForNext(): Unit = {
    // Return if buffer is not empty
    if (writeIndex > readIndex) {
      return
    }
    // Waiting for "emptyLock"
    withResource(new NvtxWithMetrics("waitForCPU", NvtxColor.RED, waitTimeMetric)) { _ =>
      do {
        emptyLock.synchronized {
          if (writeIndex == readIndex) {
            emptyLock.wait()
          }
        }
      } while (writeIndex == readIndex)
    }
  }

  override def takeNext: Array[RapidsHostColumn] = {
    require(writeIndex > readIndex, "The RingBuffer is EMPTY")

    buffer(readIndex % capacity) match {
      case Left(ex: Throwable) =>
        logError(s"[$taskAttId] PreloadedIterator: AsyncProducer failed with exceptions")
        throw new RuntimeException(s"[$taskAttId] PreloadedIterator", ex)
      case Right(ret: Array[RapidsHostColumn]) =>
        logInfo(s"[$taskAttId] PreloadedIterator consumed $readIndex batches, " +
          s"currently preloaded batchNum: ${writeIndex - readIndex}"
        )
        // Update the readIndex and activate "fullLock"
        fullLock.synchronized {
          readIndex += 1
          fullLock.notify()
        }
        ret
    }
  }
}
