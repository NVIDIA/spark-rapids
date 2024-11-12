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

import scala.collection.mutable

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{AcquireFailed, CoalesceSizeGoal, CudfRowTransitions, GeneratedInternalRowToCudfRowIterator, GpuColumnVector, GpuMetric, GpuRowToColumnConverter, GpuSemaphore, NvtxWithMetrics, RowToColumnarIterator}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.velox.{RapidsHostColumn, VeloxBatchConverter}
import org.apache.gluten.execution.VeloxColumnarToRowExec
import org.apache.gluten.rapids.GlutenJniWrapper

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class CoalesceNativeConverter(veloxIter: Iterator[ColumnarBatch],
                              targetBatchSizeInBytes: Int,
                              schema: StructType,
                              metrics: Map[String, GpuMetric])
  extends Iterator[Array[RapidsHostColumn]] with Logging {

  private var converterImpl: Option[VeloxBatchConverter] = None

  private var srcExhausted = false

  private val c2cMetrics = Map(
    "C2COutputSize" -> GpuMetric.unwrap(metrics("C2COutputSize")))

  @transient private var runtime: GlutenJniWrapper = _

  def setRuntime(): Unit = {
    runtime = GlutenJniWrapper.create()
  }

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
              impl.resetTargetBuffers()
            }
            // tryAppendBatch, if failed, the batch will be placed on the deck
            metrics("VeloxOutputBatches") += 1
            !impl.tryAppendBatch(veloxIter.next())
          } else {
            srcExhausted = true
            true
          }
          if (needFlush) {
            metrics("C2COutputBatches") += 1
            val rapidsHostBatch = impl.flushAndConvert()
            // It is essential to check and tidy up the deck right after flushing. Because if
            // the next call of veloxIter.hasNext will release the batch which the deck holds
            // its reference.
            if (impl.isDeckFilled) {
              impl.resetTargetBuffers()
            }
            return rapidsHostBatch
          }
        }
        if (converterImpl.isEmpty) {
          require(runtime != null, "Please setRuntime before fetching the iterator")
          val converter = VeloxBatchConverter(
            runtime,
            veloxIter.next(),
            targetBatchSizeInBytes, schema, c2cMetrics
          )
          converterImpl = Some(converter)
        }
      }

      throw new RuntimeException("should NOT reach this line")
    }
  }

}

trait RapidsHostBatchProducer {
  def hasNext: Boolean

  def waitForNext(): Unit

  def takeNext: Array[RapidsHostColumn]
}

case class SyncHostBatchProducer(base: Iterator[Array[RapidsHostColumn]])
  extends RapidsHostBatchProducer {

  @transient
  private lazy val deck = mutable.Queue.empty[Array[RapidsHostColumn]]

  override def hasNext: Boolean = base.hasNext

  override def waitForNext(): Unit = {
    if (deck.isEmpty) {
      deck.enqueue(base.next())
    }
  }

  override def takeNext: Array[RapidsHostColumn] = {
    require(deck.nonEmpty, "The deck is NOT filled")
    deck.dequeue()
  }
}

object VeloxColumnarBatchConverter extends Logging {

  def hostToDevice(hostProducer: RapidsHostBatchProducer,
                   outputAttr: Seq[Attribute],
                   metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {

    new Iterator[ColumnarBatch] {

      private val dataTypes = outputAttr.map(_.dataType).toArray

      override def hasNext: Boolean = hostProducer.hasNext

      override def next(): ColumnarBatch = {
        // 1. Preparing Stage
        // Firstly, waits for the next host batch being ready
        hostProducer.waitForNext()
        // Then, acquires GpuSemaphore
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
        // Finally, take the ownership of the next host batch, which might trigger the potential
        // asynchronous producer through creating an empty slot on the buffer.
        val hostColumns = hostProducer.takeNext

        // 2. Transferring Stage
        val deviceVectors: Array[ColumnVector] = hostColumns.zip(dataTypes).safeMap {
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
        new ColumnarBatch(deviceVectors, hostColumns.head.vector.getRowCount.toInt)
      }
    }
  }

  def roundTripConvert(iter: Iterator[ColumnarBatch],
                       outputAttr: Seq[Attribute],
                       coalesceGoal: CoalesceSizeGoal,
                       metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    val rowIter: Iterator[InternalRow] = VeloxColumnarToRowExec.toRowIterator(
      iter,
      outputAttr,
      GpuMetric.unwrap(metrics("C2ROutputRows")),
      GpuMetric.unwrap(metrics("C2ROutputBatches")),
      GpuMetric.unwrap(metrics("VeloxC2RTime"))
    )
    val useCudfRowTransition: Boolean = {
      outputAttr.nonEmpty && outputAttr.length < 100000000 &&
        CudfRowTransitions.areAllSupported(outputAttr)
    }

    if (useCudfRowTransition) {
      GeneratedInternalRowToCudfRowIterator(
        rowIter,
        outputAttr.toArray, coalesceGoal,
        metrics("R2CStreamTime"), metrics("R2CTime"),
        metrics("R2CInputRows"), metrics("R2COutputRows"), metrics("R2COutputBatches"))
    } else {
      val fullOutputSchema = StructType(outputAttr.map { ar =>
        StructField(ar.name, ar.dataType, ar.nullable)
      })
      val converters = new GpuRowToColumnConverter(fullOutputSchema)
      new RowToColumnarIterator(
        rowIter,
        fullOutputSchema, coalesceGoal, converters,
        metrics("R2CInputRows"), metrics("R2COutputRows"), metrics("R2COutputBatches"),
        metrics("R2CStreamTime"), metrics("R2CTime"), metrics("GpuAcquireTime"))
    }
  }

}
