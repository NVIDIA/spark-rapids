/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import scala.annotation.tailrec
import scala.collection.mutable.Queue

import ai.rapids.cudf.{Cuda, HostColumnVector, NvtxColor, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.RowConversion
import com.nvidia.spark.rapids.shims.{CudfUnsafeRow, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An iterator that uses the GPU for columnar to row conversion of fixed width types.
 */
class AcceleratedColumnarToRowIterator(
    schema: Seq[Attribute],
    batches: Iterator[ColumnarBatch],
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric,
    streamTime: GpuMetric) extends Iterator[InternalRow] with Serializable {
  @transient private var pendingCvs: Queue[HostColumnVector] = Queue.empty
  // GPU batches read in must be closed by the receiver (us)
  @transient private var currentCv: Option[HostColumnVector] = None
  // This only works on fixedWidth types for now...
  assert(schema.forall(attr => UnsafeRow.isFixedLength(attr.dataType)))
  // We want to remap the rows to improve packing.  This means that they should be sorted by
  // the largest alignment to the smallest.

  // for packMap the nth entry is the index of the original input column that we want at
  // the nth entry.
  private val packMap: Array[Int] = schema
    .zipWithIndex
    .sortWith {
      (x, y) =>
        DecimalUtil.getDataTypeSize(x._1.dataType) > DecimalUtil.getDataTypeSize(y._1.dataType)
    }.map(_._2)
    .toArray
  // For unpackMap the nth entry is the index in the row that came back for the original
  private val unpackMap: Array[Int] = packMap
      .zipWithIndex
      .sortWith(_._1 < _._1)
      .map(_._2)

  private val outputRow = new CudfUnsafeRow(packMap.map(schema(_)), unpackMap)
  private var baseDataAddress: Long = -1
  private var at: Int = 0
  private var total: Int = 0

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeAllPendingBatches()
    }
  }

  private def setCurrentBatch(wip: HostColumnVector): Unit = {
    currentCv = Some(wip)
    at = 0
    total = wip.getRowCount().toInt
    val byteBuffer = currentCv.get.getChildColumnView(0).getData
    baseDataAddress = byteBuffer.getAddress
  }

  private def closeCurrentBatch(): Unit = {
    currentCv.foreach(_.close())
    currentCv = None
  }

  private def closeAllPendingBatches(): Unit = {
    closeCurrentBatch()
    pendingCvs.foreach(_.close())
    pendingCvs = Queue.empty
  }

  private def rearrangeRows(cb: ColumnarBatch): Table = {
    val columns = GpuColumnVector.extractBases(cb)
    val rearrangedColumns = packMap.map(columns(_))
    new Table(rearrangedColumns : _*)
  }

  private[this] def setupBatchAndClose(scb: SpillableColumnarBatch): Boolean = {
    numInputBatches += 1
    // In order to match the numOutputRows metric in the generated code we update
    // numOutputRows for each batch. This is less accurate than doing it at output
    // because it will over count the number of rows output in the case of a limit,
    // but it is more efficient.
    numOutputRows += scb.numRows()
    if (scb.numRows() > 0) {
      withResource(new NvtxWithMetrics("ColumnarToRow: batch", NvtxColor.RED, opTime)) { _ =>
        val it = RmmRapidsRetryIterator.withRetry(scb, splitSpillableInHalfByRows) { attempt =>
          withResource(attempt.getColumnarBatch()) { attemptCb =>
            withResource(rearrangeRows(attemptCb)) { table =>
              // The fixed-width optimized cudf kernel only supports up to 1.5 KB per row which
              // means at most 184 double/long values. Spark by default limits codegen to 100
              // fields "spark.sql.codegen.maxFields". So, we are going to be cautious and
              // start with that until we have tested it more. We branching over the size of
              // the output to know which kernel to call. If schema.length < 100 we call the
              // fixed-width optimized version, otherwise the generic one
              if (schema.length < 100) {
                RowConversion.convertToRowsFixedWidthOptimized(table)
              } else {
                RowConversion.convertToRows(table)
              }
            }
          }
        }
        assert(it.hasNext, "Got an unexpected empty iterator after setting up batch with retry")
        it.foreach { rowsCvList =>
          withResource(rowsCvList) { _ =>
            rowsCvList.foreach { rowsCv =>
              pendingCvs += rowsCv.copyToHost()
            }
          }
        }
        setCurrentBatch(pendingCvs.dequeue())
        true
      }
    } else { // scb.numRows() <= 0
      scb.close()
      false
    }
  }

  private[this] def loadNextBatch(): Unit = {
    closeCurrentBatch()
    if (pendingCvs.nonEmpty) {
      setCurrentBatch(pendingCvs.dequeue())
    } else {
      populateBatch()
    }
    GpuSemaphore.releaseIfNecessary(TaskContext.get())
  }

  @tailrec
  private def populateBatch(): Unit = {
    // keep fetching input batches until we have a non-empty batch ready
    val nextBatch = fetchNextBatch()
    if (nextBatch.isDefined) {
      if (!setupBatchAndClose(nextBatch.get)) {
        populateBatch()
      }
    }
  }

  private def fetchNextBatch(): Option[SpillableColumnarBatch] = {
    withResource(new NvtxWithMetrics("ColumnarToRow: fetch", NvtxColor.BLUE, streamTime)) { _ =>
      if (batches.hasNext) {
        // Make it spillable once getting a columnar batch.
        val spillBatch = closeOnExcept(batches.next()) { cb =>
          SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        Some(spillBatch)
      } else {
        None
      }
    }
  }

  override def hasNext: Boolean = {
    val itHasNext = at < total
    if (!itHasNext) {
      loadNextBatch()
      at < total
    } else {
      itHasNext
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    // Here we should do some code generation, but for now
    val startReadOffset = currentCv.get.getStartListOffset(at)
    val endReadOffset = currentCv.get.getEndListOffset(at)
    outputRow.pointTo(baseDataAddress + startReadOffset, (endReadOffset - startReadOffset).toInt)
    at += 1
    outputRow
  }
}

/**
 * ColumnarToRowIterator converts GPU ColumnarBatches to CPU InternalRows.
 *
 * @note releaseSemaphore = true (default) should only be used in cases where
 *       we are sure that no GPU memory is left unaccounted for (not spillable).
 *       One notable case where releaseSemaphore is false is when used in
 *       `GpuUserDefinedFunction`, which is evaluated as part of a projection, that
 *       may or may not include other GPU columns.
 */
class ColumnarToRowIterator(batches: Iterator[ColumnarBatch],
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric,
    streamTime: GpuMetric,
    nullSafe: Boolean = false,
    releaseSemaphore: Boolean = true) extends Iterator[InternalRow] with AutoCloseable {
  // GPU batches read in must be closed by the receiver (us)
  @transient private var cb: ColumnarBatch = null
  private var it: java.util.Iterator[InternalRow] = null

  private[this] lazy val toHost = if (nullSafe) {
    (gpuCV: GpuColumnVector) => gpuCV.copyToNullSafeHost()
  } else{
    (gpuCV: GpuColumnVector) => gpuCV.copyToHost()
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeCurrentBatch()
    }
  }

  override def close(): Unit = closeCurrentBatch()

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  def loadNextBatch(): Unit = {
    closeCurrentBatch()
    it = null
    // devCb will be None if the parent iterator is empty
    val devCb = fetchNextBatch()
    // perform conversion
    try {
      devCb.foreach { devCb =>
        val sDevCb = SpillableColumnarBatch(devCb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        cb = withRetryNoSplit(sDevCb) { _ =>
          withResource(sDevCb.getColumnarBatch()) { devCb =>
            withResource(new NvtxWithMetrics("ColumnarToRow: batch", NvtxColor.RED, opTime)) { _ =>
              new ColumnarBatch(GpuColumnVector.extractColumns(devCb).safeMap(toHost),
                devCb.numRows())
            }
          }
        }
        it = cb.rowIterator()
        // In order to match the numOutputRows metric in the generated code we update
        // numOutputRows for each batch. This is less accurate than doing it at output
        // because it will over count the number of rows output in the case of a limit,
        // but it is more efficient.
        numOutputRows += cb.numRows()
      }
    } finally {
      // Leaving the GPU for a while: if this iterator is configured to release
      // the semaphore, do it now.
      if (releaseSemaphore) {
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      }
    }
  }

  private def fetchNextBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("ColumnarToRow: fetch", NvtxColor.BLUE, streamTime)) { _ =>
      while (batches.hasNext) {
        numInputBatches += 1
        val devCb = batches.next()
        if (devCb.numRows() > 0) {
          return Some(devCb)
        } else {
          devCb.close()
        }
      }
      None
    }
  }

  override def hasNext: Boolean = {
    val itHasNext = it != null && it.hasNext
    if (!itHasNext) {
      loadNextBatch()
      it != null && it.hasNext
    } else {
      itHasNext
    }
  }

  override def next(): InternalRow = {
    if (it == null || !it.hasNext) {
      loadNextBatch()
    }
    if (it == null) {
      throw new NoSuchElementException()
    }
    it.next()
  }
}

object CudfRowTransitions {
  def isSupportedType(dataType: DataType): Boolean = dataType match {
    // Only fixed width for now...
    case ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BooleanType | DateType | TimestampType => true
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => true
    case _ => false
  }

  def areAllSupported(schema: Seq[Attribute]): Boolean =
    schema.forall(att => isSupportedType(att.dataType))
}

case class GpuColumnarToRowExec(
    child: SparkPlan,
    exportColumnarRdd: Boolean = false)
    extends ShimUnaryExecNode with ColumnarToRowTransition with GpuExec {
  import GpuMetric._
  // We need to do this so the assertions don't fail
  override def supportsColumnar = false

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Override the original metrics to remove NUM_OUTPUT_BATCHES, which makes no sense.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)

    val f = GpuColumnarToRowExec.makeIteratorFunc(child.output, numOutputRows, numInputBatches,
      opTime, streamTime)

    val cdata = child.executeColumnar()
    if (exportColumnarRdd) {
      // If we are exporting columnar rdd we need an easy way for the code that walks the
      // RDDs to know where the columnar to row transition is happening.
      GpuColumnToRowMapPartitionsRDD.mapPartitions(cdata, f)
    } else {
      cdata.mapPartitions(f)
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

object GpuColumnarToRowExec {
  /**
   * Helper to check if GPU accelerated row-column transpose is supported.
   * This is a workaround for [[https://github.com/rapidsai/cudf/issues/10569]],
   * where CUDF JNI column->row transposition works incorrectly on certain
   * GPU architectures.
   */
  private lazy val isAcceleratedTransposeSupported: Boolean = {
    // Check if the current CUDA device architecture exceeds Pascal.
    // i.e. CUDA compute capability > 6.x.
    // Reference:  https://developer.nvidia.com/cuda-gpus
    Cuda.getComputeCapabilityMajor > 6
  }

  def makeIteratorFunc(
      output: Seq[Attribute],
      numOutputRows: GpuMetric,
      numInputBatches: GpuMetric,
      opTime: GpuMetric,
      streamTime: GpuMetric): Iterator[ColumnarBatch] => Iterator[InternalRow] = {
    if (CudfRowTransitions.areAllSupported(output) &&
        // For a small number of columns it is still best to do it the original way
        output.length > 4 &&
        // We can support upto 2^31 bytes per row. That is ~250M columns of 64-bit fixed-width data.
        // This number includes the 1-bit validity per column, but doesn't include padding.
        // We are being conservative by only allowing 100M columns until we feel the need to
        // increase this number
        output.length <= 100000000) {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        // Work around {@link https://github.com/rapidsai/cudf/issues/10569}, where CUDF JNI
        // acceleration of column->row transposition produces incorrect results on certain
        // GPU architectures.
        // Check that the accelerated transpose works correctly on the current CUDA device.
        if (isAcceleratedTransposeSupported) {
          new AcceleratedColumnarToRowIterator(output, batches, numInputBatches, numOutputRows,
            opTime, streamTime).map(toUnsafe)
        } else {
          new ColumnarToRowIterator(batches,
            numInputBatches, numOutputRows, opTime, streamTime).map(toUnsafe)
        }
      }
    } else {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        new ColumnarToRowIterator(batches,
          numInputBatches, numOutputRows, opTime, streamTime).map(toUnsafe)
      }
    }
  }
}
