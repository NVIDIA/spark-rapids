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

package org.apache.spark.rapids.hybrid

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{AcquireFailed, GpuColumnVector, GpuMetric, GpuSemaphore, NvtxWithMetrics}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.hybrid.{CoalesceBatchConverter => NativeConverter}
import com.nvidia.spark.rapids.hybrid.HybridJniWrapper
import com.nvidia.spark.rapids.hybrid.RapidsHostColumn

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class CoalesceConvertIterator(veloxIter: Iterator[ColumnarBatch],
                              targetBatchSizeInBytes: Int,
                              schema: StructType,
                              metrics: Map[String, GpuMetric])
  extends Iterator[Array[RapidsHostColumn]] with Logging {

  private var converterImpl: Option[NativeConverter] = None

  private var srcExhausted = false

  private val converterMetrics = Map(
    "C2COutputSize" -> GpuMetric.unwrap(metrics("C2COutputSize")))

  @transient private lazy val runtime: HybridJniWrapper = HybridJniWrapper.getOrCreate()

  override def hasNext(): Boolean = {
    // either converter holds data or upstreaming iterator holds data
    val ret = withResource(new NvtxWithMetrics("VeloxC2CHasNext", NvtxColor.WHITE,
      metrics("C2CStreamTime"))) { _ =>
      converterImpl.exists(c => c.isDeckFilled || c.hasProceedingBuilders) ||
        (!srcExhausted && veloxIter.hasNext)
    }
    if (!ret) {
      if (!srcExhausted) {
        srcExhausted = true
      }
      converterImpl.foreach { c =>
        // VeloxBatchConverter collects the eclipsedTime of C2C_Conversion by itself.
        // Here we fetch the final value before closing it.
        metrics("C2CTime") += c.eclipsedNanoSecond
        // release the native instance when upstreaming iterator has been exhausted
        val detailedMetrics = c.close()
        val tID = TaskContext.get().taskAttemptId()
        logError(s"task[$tID] CoalesceNativeConverter finished:\n$detailedMetrics")
        converterImpl = None
      }
    }
    ret
  }

  override def next(): Array[RapidsHostColumn] = {
    val ntvx = new NvtxWithMetrics("VeloxC2CNext", NvtxColor.YELLOW, metrics("C2CStreamTime"))
    withResource(ntvx) { _ =>
      while (true) {
        converterImpl.foreach { impl =>
          val needFlush = if (veloxIter.hasNext) {
            // The only condition leading to a nonEmpty deck is targetBuffers are unset after
            // the previous flushing
            if (impl.isDeckFilled) {
              impl.setupTargetVectors()
            }
            // tryAppendBatch, if failed, the batch will be placed on the deck
            metrics("CpuReaderBatches") += 1
            !impl.tryAppendBatch(veloxIter.next())
          } else {
            srcExhausted = true
            true
          }
          if (needFlush) {
            metrics("CoalescedBatches") += 1
            val rapidsHostBatch = impl.flush()
            // It is essential to check and tidy up the deck right after flushing. Because if
            // the next call of veloxIter.hasNext will release the batch which the deck holds
            // its reference.
            if (impl.isDeckFilled) {
              impl.setupTargetVectors()
            }
            return rapidsHostBatch
          }
        }
        if (converterImpl.isEmpty) {
          require(runtime != null, "Please setRuntime before fetching the iterator")
          val converter = NativeConverter(
            veloxIter.next(),
            targetBatchSizeInBytes, schema, converterMetrics
          )
          converterImpl = Some(converter)
        }
      }

      throw new RuntimeException("should NOT reach this line")
    }
  }

}

object CoalesceConvertIterator extends Logging {

  def hostToDevice(hostIter: Iterator[Array[RapidsHostColumn]],
                   outputAttr: Seq[Attribute],
                   metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    val dataTypes = outputAttr.map(_.dataType).toArray

    hostIter.map { hostVectors =>
      Option(TaskContext.get()).foreach { ctx =>
        GpuSemaphore.tryAcquire(ctx) match {
          case AcquireFailed(_) =>
            withResource(new NvtxWithMetrics("gpuAcquireC2C", NvtxColor.GREEN,
              metrics("GpuAcquireTime"))) { _ =>
              GpuSemaphore.acquireIfNecessary(ctx)
            }
          case _ =>
        }
      }

      val deviceVectors: Array[ColumnVector] = hostVectors.zip(dataTypes).safeMap {
        case (RapidsHostColumn(hcv, isPinned, totalBytes), dt) =>
          val nvtxMetric = if (isPinned) {
            metrics("PinnedH2DSize") += totalBytes
            new NvtxWithMetrics("pinnedH2D", NvtxColor.DARK_GREEN, metrics("PinnedH2DTime"))
          } else {
            metrics("PageableH2DSize") += totalBytes
            new NvtxWithMetrics("PageableH2D", NvtxColor.GREEN, metrics("PageableH2DTime"))
          }
          withResource(hcv) { _ =>
            withResource(nvtxMetric) { _ =>
              GpuColumnVector.from(hcv.copyToDevice(), dt)
            }
          }
      }

      new ColumnarBatch(deviceVectors, hostVectors.head.vector.getRowCount.toInt)
    }
  }

}
