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

import java.util.{Comparator, LinkedList, PriorityQueue}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ContiguousTable, NvtxColor, Table}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.GpuMetric._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait SortExecType extends Serializable

object OutOfCoreSort extends SortExecType
object FullSortSingleBatch extends SortExecType
object SortEachBatch extends SortExecType

class GpuSortMeta(
    sort: SortExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SortExec](sort, conf, parent, rule) {
  override def convertToGpu(): GpuExec = {
    GpuSortExec(childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]],
      sort.global,
      childPlans.head.convertIfNeeded(),
      if (conf.outOfCoreSort) OutOfCoreSort else FullSortSingleBatch)
  }
}

case class GpuSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    sortType: SortExecType)
  extends UnaryExecNode with GpuExec {

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = sortType match {
    case FullSortSingleBatch => Seq(RequireSingleBatch)
    case OutOfCoreSort | SortEachBatch => Seq(null)
    case t => throw new IllegalArgumentException(s"Unexpected Sort Type $t")
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  // Eventually this might change, but for now we will produce a single batch, which is the same
  // as what we require from our input.
  override def outputBatching: CoalesceGoal = sortType match {
    case FullSortSingleBatch => RequireSingleBatch
    case _ => null
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics = Map(
    SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY))

  private [this] lazy val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sorter = new GpuSorter(sortOrder, output)

    val sortTime = gpuLongMetric(SORT_TIME)
    val peakDevMemory = gpuLongMetric(PEAK_DEVICE_MEMORY)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val outputBatch = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val outOfCore = sortType == OutOfCoreSort
    child.executeColumnar().mapPartitions { cbIter =>
      if (outOfCore) {
        val cpuOrd = new LazilyGeneratedOrdering(sorter.cpuOrdering)
        val iter = GpuOutOfCoreSortIterator(cbIter, sorter, cpuOrd,
          // bad things can happen if the target size is really small
          math.max(16 * 1024, targetSize), totalTime, sortTime, outputBatch, outputRows)
        TaskContext.get().addTaskCompletionListener(_ -> iter.close())
        iter
      } else {
        GpuSortEachBatchIterator(cbIter, sorter, totalTime, sortTime, outputBatch, outputRows,
          peakDevMemory)
      }
    }
  }
}

case class GpuSortEachBatchIterator(
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    totalTime: GpuMetric = NoopMetric,
    sortTime: GpuMetric = NoopMetric,
    outputBatches: GpuMetric = NoopMetric,
    outputRows: GpuMetric = NoopMetric,
    peakDevMemory: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] with Arm {
  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    withResource(iter.next()) { cb =>
        withResource(new NvtxWithMetrics("sort total", NvtxColor.WHITE, totalTime)) { _ =>
          val ret = sorter.fullySortBatch(cb, sortTime, peakDevMemory)
          outputBatches += 1
          outputRows += ret.numRows()
          ret
        }
    }
  }
}

case class OutOfCoreBatch(buffer: SpillableColumnarBatch, row: UnsafeRow) extends AutoCloseable {
  override def close(): Unit = buffer.close()
}

class Pending(cpuOrd: LazilyGeneratedOrdering) extends AutoCloseable {
  private val pending = new PriorityQueue[OutOfCoreBatch](new Comparator[OutOfCoreBatch]() {
    override def compare(a: OutOfCoreBatch, b: OutOfCoreBatch): Int =
      cpuOrd.compare(a.row, b.row)
  })
  private var pendingSize = 0L
  def add(buffer: SpillableColumnarBatch, row: UnsafeRow): Unit = {
    pending.add(OutOfCoreBatch(buffer, row))
    pendingSize += buffer.sizeInBytes
  }

  def storedSize: Long = pendingSize

  def size(): Int = pending.size()

  def poll(): OutOfCoreBatch = {
    val ret = pending.poll()
    if (ret != null) {
      pendingSize -= ret.buffer.sizeInBytes
    }
    ret
  }

  def peek(): OutOfCoreBatch = pending.peek()

  def isEmpty: Boolean = pending.isEmpty

  override def close(): Unit = pending.forEach(_.close())
}

case class GpuOutOfCoreSortIterator(
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    cpuOrd: LazilyGeneratedOrdering,
    targetSize: Long,
    totalTime: GpuMetric = NoopMetric,
    sortTime: GpuMetric = NoopMetric,
    outputBatches: GpuMetric = NoopMetric,
    outputRows: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch]
    with Arm with AutoCloseable {


  private val pending = new Pending(cpuOrd)

  private val sorted = new LinkedList[SpillableColumnarBatch]()
  private var sortedSize = 0L

  override def hasNext: Boolean = !sorted.isEmpty || !pending.isEmpty || iter.hasNext

  // Use types for the UnsafeProjection otherwise we need to have CPU BoundAttributeReferences
  private lazy val unsafeProjection = UnsafeProjection.create(sorter.projectedBatchTypes)
  private lazy val converters = new GpuRowToColumnConverter(
    TrampolineUtil.fromAttributes(sorter.projectedBatchSchema))

  private def convertBoundaries(tab: Table): Array[UnsafeRow] = {
    import scala.collection.JavaConverters._
    val cb = new ColumnarBatch(
      GpuColumnVector.extractColumns(tab, sorter.projectedBatchTypes).map(_.copyToHost()),
      tab.getRowCount.toInt)
    withResource(cb) { cb =>
      cb.rowIterator().asScala.map(unsafeProjection).map(_.copy().asInstanceOf[UnsafeRow]).toArray
    }
  }

  private final def splitAfterSortAndSave(sortedTbl: Table, sortedOffset: Int = -1): Unit = {
    // We need to figure out how to split up the data into reasonable batches. We could try and do
    // something really complicated and figure out how much data get per batch, but in practice
    // we really only expect to see one or two batches worth of data come in, so lets optimize
    // for that case and set the targetBatchSize to always be 1/8th the targetSize.
    val targetBatchSize = targetSize / 8
    val rows = sortedTbl.getRowCount.toInt
    val memSize = GpuColumnVector.getTotalDeviceMemoryUsed(sortedTbl)
    val averageRowSize = memSize.toDouble/rows
    // Protect ourselves from large rows when there are small targetSizes
    val targetRowCount = Math.max((targetBatchSize/averageRowSize).toInt, 1024)

    if (sortedOffset == rows - 1) {
      // The entire thing is sorted
      withResource(sortedTbl.contiguousSplit()) { split =>
        assert(split.length == 1)
        closeOnExcept(
          GpuColumnVectorFromBuffer.from(split.head, sorter.projectedBatchTypes)) { cb =>
          val sp = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          sortedSize += sp.sizeInBytes
          sorted.add(sp)
        }
      }
    } else {
      val splitIndexes = if (sortedOffset >= 0) {
        sortedOffset until rows by targetRowCount
      } else {
        targetRowCount until rows by targetRowCount
      }
      // Get back the first row so we can sort the batches
      val gatherIndexes = if (sortedOffset >= 0) {
        // The first batch is sorted so don't gather a row for it
        splitIndexes
      } else {
        Seq(0) ++ splitIndexes
      }

      val boundaries = withResource(ColumnVector.fromInts(gatherIndexes: _*)) { gatherMap =>
        withResource(sortedTbl.gather(gatherMap)) { boundariesTab =>
          convertBoundaries(boundariesTab)
        }
      }

      withResource(sortedTbl.contiguousSplit(splitIndexes: _*)) { split =>
        val stillPending = if (sortedOffset >= 0) {
          closeOnExcept(
            GpuColumnVectorFromBuffer.from(split.head, sorter.projectedBatchTypes)) { cb =>
            val sp = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            sortedSize += sp.sizeInBytes
            sorted.add(sp)
          }
          split.slice(1, split.length)
        } else {
          split
        }

        assert(boundaries.length == stillPending.length)
        stillPending.zip(boundaries).foreach {
          case (ct: ContiguousTable, lower: UnsafeRow) =>
            if (ct.getRowCount > 0) {
              closeOnExcept(
                GpuColumnVectorFromBuffer.from(ct, sorter.projectedBatchTypes)) { cb =>
                pending.add(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
                  lower)
              }
            } else {
              ct.close()
            }
        }
      }
    }
  }

  private final def firstPassReadBatches(): Unit = {
    while(iter.hasNext) {
      val sortedTbl = withResource(iter.next()) { batch =>
        withResource(new NvtxWithMetrics("initial sort", NvtxColor.CYAN, totalTime)) { _ =>
          sorter.appendProjectedAndSort(batch, sortTime)
        }
      }
      withResource(new NvtxWithMetrics("split input batch", NvtxColor.CYAN, totalTime)) { _ =>
        withResource(sortedTbl) { sortedTbl =>
          val rows = sortedTbl.getRowCount.toInt
          // filter out empty batches
          if (rows > 0) {
            splitAfterSortAndSave(sortedTbl)
          }
        }
      }
    }
  }

  private final def mergeSortEnoughToOutput(): Unit = {
    // Now get enough sorted data to return
    while (!pending.isEmpty && sortedSize < targetSize) {
      // Keep going until we have enough data to return
      var bytesLeftToFetch = targetSize
      val mergedBatch = withResource(ArrayBuffer[SpillableColumnarBatch]()) { pendingSort =>
        while (!pending.isEmpty &&
            (bytesLeftToFetch - pending.peek().buffer.sizeInBytes >= 0 || pendingSort.isEmpty)) {
          val buffer = pending.poll().buffer
          pendingSort += buffer
          bytesLeftToFetch -= buffer.sizeInBytes
        }
        withResource(ArrayBuffer[ColumnarBatch]()) { batches =>
          pendingSort.foreach { tmp =>
            batches += tmp.getColumnarBatch()
          }
          if (batches.size == 1) {
            // Single batch no need for a merge sort
            GpuColumnVector.incRefCounts(batches.head)
          } else {
            sorter.mergeSort(batches.toArray, sortTime)
          }
        }
      }
      withResource(mergedBatch) { mergedBatch =>
        // First we want figure out what is fully sorted from what is not
        val sortSplitOffset = if (pending.isEmpty) {
          // No need to split it
          mergedBatch.numRows() - 1
        } else {
          // The data is only fully sorted if there is nothing pending that is smaller than it
          // so get the nest smallest row that is pending
          val cutoff = pending.peek().row
          val builders = new GpuColumnarBatchBuilder(
            TrampolineUtil.fromAttributes(sorter.projectedBatchSchema), 1, null)
          converters.convert(cutoff, builders)
          withResource(builders.build(1)) { cutoffCb =>
            withResource(sorter.upperBound(mergedBatch, cutoffCb)) { result =>
              withResource(result.copyToHost()) { hostResult =>
                assert(hostResult.getRowCount == 1)
                hostResult.getInt(0)
              }
            }
          }
        }
        withResource(GpuColumnVector.from(mergedBatch)) { mergedTbl =>
          splitAfterSortAndSave(mergedTbl, sortSplitOffset)
        }
      }
    }
  }

  private final def concatOutput(): ColumnarBatch = {
    // combine all the sorted data into a single batch
    withResource(ArrayBuffer[Table]()) { tables =>
      var totalBytes = 0L
      while(!sorted.isEmpty &&
          (totalBytes + sorted.peek().sizeInBytes) < targetSize) {
        withResource(sorted.pop()) { tmp =>
          sortedSize -= tmp.sizeInBytes
          totalBytes += tmp.sizeInBytes
          withResource(tmp.getColumnarBatch()) { batch =>
            tables += GpuColumnVector.from(batch)
          }
        }
      }
      val ret = if (tables.length == 1) {
        // We cannot concat a single table
        sorter.removeProjectedColumns(tables.head)
      } else {
        withResource(Table.concatenate(tables: _*)) { combined =>
          sorter.removeProjectedColumns(combined)
        }
      }
      outputBatches += 1
      outputRows += ret.numRows()
      ret
    }
  }

  override def next(): ColumnarBatch = {
    if (sorter.projectedBatchSchema.isEmpty) {
      // special case, no columns just rows
      iter.next()
    }
    if (pending.isEmpty && sorted.isEmpty) {
      firstPassReadBatches()
    }
    withResource(new NvtxWithMetrics("Sort next output batch", NvtxColor.CYAN, totalTime)) { _ =>
      mergeSortEnoughToOutput()
      concatOutput()
    }
  }

  override def close(): Unit = {
    sorted.forEach(_.close())
    pending.close()
  }
}
