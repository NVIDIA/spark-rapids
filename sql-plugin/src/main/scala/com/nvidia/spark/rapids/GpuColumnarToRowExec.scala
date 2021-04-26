/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{HostColumnVector, NvtxColor, Table}
import com.nvidia.spark.rapids.GpuColumnarToRowExecParent.makeIteratorFunc

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CudfUnsafeRow, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
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
    gpuOpTime: GpuMetric,
    fetchTime: GpuMetric) extends Iterator[InternalRow] with Arm with Serializable {
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

  TaskContext.get().addTaskCompletionListener[Unit](_ => closeAllPendingBatches())

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

  private[this] def setupBatch(cb: ColumnarBatch): Boolean = {
    numInputBatches += 1
    // In order to match the numOutputRows metric in the generated code we update
    // numOutputRows for each batch. This is less accurate than doing it at output
    // because it will over count the number of rows output in the case of a limit,
    // but it is more efficient.
    numOutputRows += cb.numRows()
    if (cb.numRows() > 0) {
      withResource(new NvtxWithMetrics("ColumnarToRow: batch", NvtxColor.RED, gpuOpTime)) { _ =>
        withResource(rearrangeRows(cb)) { table =>
          withResource(table.convertToRows()) { rowsCvList =>
            rowsCvList.foreach { rowsCv =>
              pendingCvs += rowsCv.copyToHost()
            }
            setCurrentBatch(pendingCvs.dequeue())
            return true
          }
        }
      }
    }
    false
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
      if (!withResource(nextBatch.get)(setupBatch)) {
        populateBatch()
      }
    }
  }

  private def fetchNextBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("ColumnarToRow: fetch", NvtxColor.BLUE, fetchTime)) { _ =>
      if (batches.hasNext) {
        Some(batches.next())
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

class ColumnarToRowIterator(batches: Iterator[ColumnarBatch],
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    gpuOpTime: GpuMetric,
    fetchTime: GpuMetric) extends Iterator[InternalRow] with Arm {
  // GPU batches read in must be closed by the receiver (us)
  @transient var cb: ColumnarBatch = null
  var it: java.util.Iterator[InternalRow] = null

  TaskContext.get().addTaskCompletionListener[Unit](_ => closeCurrentBatch())

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  def loadNextBatch(): Unit = {
    closeCurrentBatch()
    if (it != null) {
      it = null
    }

    // fetch next batch
    val devCb = withResource(new NvtxWithMetrics("ColumnarToRow: fetch", NvtxColor.BLUE,
        fetchTime)) { _ =>
      if (batches.hasNext) {
        Some(batches.next())
      } else {
        None
      }
    }

    // perform conversion
    devCb.foreach { devCb =>
      withResource(new NvtxWithMetrics("ColumnarToRow: batch", NvtxColor.RED, gpuOpTime)) { _ =>
        try {
          cb = new ColumnarBatch(GpuColumnVector.extractColumns(devCb).map(_.copyToHost()),
            devCb.numRows())
          it = cb.rowIterator()
          numInputBatches += 1
          // In order to match the numOutputRows metric in the generated code we update
          // numOutputRows for each batch. This is less accurate than doing it at output
          // because it will over count the number of rows output in the case of a limit,
          // but it is more efficient.
          numOutputRows += cb.numRows()
        } finally {
          devCb.close()
          // Leaving the GPU for a while
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        }
      }
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
         FloatType | DoubleType | BooleanType | DateType | TimestampType | _: DecimalType => true
    case _ => false
  }

  def areAllSupported(schema: Seq[Attribute]): Boolean =
    schema.forall(att => isSupportedType(att.dataType))
}

abstract class GpuColumnarToRowExecParent(child: SparkPlan, val exportColumnarRdd: Boolean)
    extends UnaryExecNode with GpuExec {
  import GpuMetric._
  // We need to do this so the assertions don't fail
  override def supportsColumnar = false

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Override the original metrics to remove NUM_OUTPUT_BATCHES, which makes no sense.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    GPU_OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_OP_TIME),
    FETCH_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_FETCH_TIME),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val gpuOpTime = gpuLongMetric(GPU_OP_TIME)
    val fetchTime = gpuLongMetric(FETCH_TIME)

    val f = makeIteratorFunc(child.output, numOutputRows, numInputBatches, gpuOpTime, fetchTime)

    val cdata = child.executeColumnar()
    if (exportColumnarRdd) {
      // If we are exporting columnar rdd we need an easy way for the code that walks the
      // RDDs to know where the columnar to row transition is happening.
      GpuColumnToRowMapPartitionsRDD.mapPartitions(cdata, f)
    } else {
      cdata.mapPartitions(f)
    }
  }
}

object GpuColumnarToRowExecParent {
  def unapply(arg: GpuColumnarToRowExecParent): Option[(SparkPlan, Boolean)] = {
    Option(Tuple2(arg.child, arg.exportColumnarRdd))
  }

  def makeIteratorFunc(
      output: Seq[Attribute],
      numOutputRows: GpuMetric,
      numInputBatches: GpuMetric,
      gpuOpTime: GpuMetric,
      fetchTime: GpuMetric): Iterator[ColumnarBatch] => Iterator[InternalRow] = {
    if (CudfRowTransitions.areAllSupported(output) &&
        // For a small number of columns it is still best to do it the original way
        output.length > 4 &&
        // The cudf kernel only supports up to 1.5 KB per row which means at most 184 double/long
        // values. Spark by default limits codegen to 100 fields "spark.sql.codegen.maxFields".
        // So, we are going to be cautious and start with that until we have tested it more.
        output.length < 100) {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        new AcceleratedColumnarToRowIterator(output,
          batches, numInputBatches, numOutputRows, gpuOpTime, fetchTime).map(toUnsafe)
      }
    } else {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        new ColumnarToRowIterator(batches,
          numInputBatches, numOutputRows, gpuOpTime, fetchTime).map(toUnsafe)
      }
    }
  }
}

case class GpuColumnarToRowExec(child: SparkPlan, override val exportColumnarRdd: Boolean = false)
   extends GpuColumnarToRowExecParent(child, exportColumnarRdd)
