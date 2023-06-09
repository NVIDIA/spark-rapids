/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer, ArrayStack}

import ai.rapids.cudf.{ColumnVector, ContiguousTable, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
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

  // Uses output attributes of child plan because SortExec will not change the attributes,
  // and we need to propagate possible type conversions on the output attributes of
  // GpuSortAggregateExec.
  override protected val useOutputAttributesOfChild: Boolean = true

  // For transparent plan like ShuffleExchange, the accessibility of runtime data transition is
  // depended on the next non-transparent plan. So, we need to trace back.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

  override def convertToGpu(): GpuExec = {
    GpuSortExec(childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]],
      sort.global,
      childPlans.head.convertIfNeeded(),
      if (conf.stableSort) FullSortSingleBatch else OutOfCoreSort
    )(sort.sortOrder)
  }
}

object GpuSortExec {
  def targetSize(sqlConf: SQLConf): Long = {
    val batchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(sqlConf)
    targetSize(batchSize)
  }

  def targetSize(batchSize: Long): Long = {
    // To avoid divide by zero errors, underflow and overflow issues in tests
    // that want the targetSize to be 0, we set it to something more reasonable
    math.max(16 * 1024, batchSize)
  }
}

case class GpuSortExec(
    gpuSortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    sortType: SortExecType)(cpuSortOrder: Seq[SortOrder])
  extends ShimUnaryExecNode with GpuExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuSortOrder :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = sortType match {
    case FullSortSingleBatch => Seq(RequireSingleBatch)
    case OutOfCoreSort | SortEachBatch => Seq(null)
    case t => throw new IllegalArgumentException(s"Unexpected Sort Type $t")
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = cpuSortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(cpuSortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def outputBatching: CoalesceGoal = sortType match {
    // We produce a single batch if we know that our input will be a single batch
    case FullSortSingleBatch => RequireSingleBatch
    case _ => null
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics: Map[String, GpuMetric] =
    Map(
      OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
      SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME))

  private lazy val targetSize = GpuSortExec.targetSize(conf)

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val sorter = new GpuSorter(gpuSortOrder, output)

    val sortTime = gpuLongMetric(SORT_TIME)
    val opTime = gpuLongMetric(OP_TIME)
    val outputBatch = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val outOfCore = sortType == OutOfCoreSort
    val singleBatch = sortType == FullSortSingleBatch
    child.executeColumnar().mapPartitions { cbIter =>
      if (outOfCore) {
        val cpuOrd = new LazilyGeneratedOrdering(sorter.cpuOrdering)
        val iter = GpuOutOfCoreSortIterator(cbIter, sorter, cpuOrd,
          targetSize, opTime, sortTime, outputBatch, outputRows)
        TaskContext.get().addTaskCompletionListener(_ -> iter.close())
        iter
      } else {
        GpuSortEachBatchIterator(cbIter, sorter, singleBatch,
          opTime, sortTime, outputBatch, outputRows)
      }
    }
  }
}

case class GpuSortEachBatchIterator(
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    singleBatch: Boolean,
    opTime: GpuMetric = NoopMetric,
    sortTime: GpuMetric = NoopMetric,
    outputBatches: GpuMetric = NoopMetric,
    outputRows: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    withResource(iter.next()) { cb =>
      withResource(new NvtxWithMetrics("sort op", NvtxColor.WHITE, opTime)) { _ =>
        val ret = sorter.fullySortBatch(cb, sortTime)
        outputBatches += 1
        outputRows += ret.numRows()
        if (singleBatch) {
          GpuColumnVector.tagAsFinalBatch(ret)
        } else {
          ret
        }
      }
    }
  }
}

/**
 * Holds data for the out of core sort. It includes the batch of data and the first row in that
 * batch so we can sort the batches.
 */
case class OutOfCoreBatch(buffer: SpillableColumnarBatch,
    firstRow: UnsafeRow) extends AutoCloseable {
  override def close(): Unit = buffer.close()
}

/**
 * Data that the out of core sort algorithm has not finished sorting. This acts as a priority
 * queue with each batch sorted by the first row in that batch.
 */
class Pending(cpuOrd: LazilyGeneratedOrdering) extends AutoCloseable {
  private val pending = new PriorityQueue[OutOfCoreBatch](new Comparator[OutOfCoreBatch]() {
    override def compare(a: OutOfCoreBatch, b: OutOfCoreBatch): Int =
      cpuOrd.compare(a.firstRow, b.firstRow)
  })
  private var pendingSize = 0L

  def add(batch: OutOfCoreBatch): Unit = {
    pendingSize += batch.buffer.sizeInBytes
    pending.add(batch)
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

/**
 * Sorts incoming batches of data spilling if needed.
 * <br/>
 * The algorithm for this is a modified version of an external merge sort with multiple passes for
 * large data.
 * https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
 * <br/>
 * The main difference is that we cannot stream the data when doing a merge sort. So, we instead
 * divide the data into batches that are small enough that we can do a merge sort on N batches
 * and still fit the output within the target batch size. When merging batches instead of
 * individual rows we cannot assume that all of the resulting data is globally sorted. Hopefully,
 * most of it is globally sorted but we have to use the first row from the next pending batch to
 * determine the cutoff point between globally sorted data and data that still needs to be merged
 * with other batches. The globally sorted portion is put into a sorted queue while the rest of
 * the merged data is split and put back into a pending queue.  The process repeats until we have
 * enough data to output.
 */
case class GpuOutOfCoreSortIterator(
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    cpuOrd: LazilyGeneratedOrdering,
    targetSize: Long,
    opTime: GpuMetric,
    sortTime: GpuMetric,
    outputBatches: GpuMetric,
    outputRows: GpuMetric) extends Iterator[ColumnarBatch]
    with AutoCloseable {

  // A priority queue of data that is not merged yet.
  private val pending = new Pending(cpuOrd)

  // data that has been determined to be globally sorted and is waiting to be output.
  private val sorted = new LinkedList[SpillableColumnarBatch]()
  // how much data, in bytes, that is stored in `sorted`
  private var sortedSize = 0L

  override def hasNext: Boolean = !sorted.isEmpty || !pending.isEmpty || iter.hasNext

  // Use types for the UnsafeProjection otherwise we need to have CPU BoundAttributeReferences
  // used for converting between columnar data and rows (to get the first row in each batch).
  private lazy val unsafeProjection = UnsafeProjection.create(sorter.projectedBatchTypes)
  // Used for converting between rows and columns when we have to put a cuttoff on the GPU
  // to know how much of the data after a merge sort is fully sorted.
  private lazy val converters = new GpuRowToColumnConverter(
    TrampolineUtil.fromAttributes(sorter.projectedBatchSchema))

  /**
   * Convert the boundaries (first rows for each batch) into unsafe rows for use later on.
   */
  private def convertBoundaries(tab: Table): Array[UnsafeRow] = {
    import scala.collection.JavaConverters._
    val cb = withResource(new NvtxRange("COPY BOUNDARIES", NvtxColor.PURPLE)) { _ =>
      new ColumnarBatch(
        GpuColumnVector.extractColumns(tab, sorter.projectedBatchTypes).map(_.copyToHost()),
        tab.getRowCount.toInt)
    }
    withResource(cb) { cb =>
      withResource(new NvtxRange("TO UNSAFE ROW", NvtxColor.RED)) { _ =>
        cb.rowIterator().asScala.map(unsafeProjection).map(_.copy().asInstanceOf[UnsafeRow]).toArray
      }
    }
  }

  /**
   * A rather complex function. It will take a sorted table (either the output of a regular sort or
   * a merge sort), split it up, and place the split portions into the proper queues. If
   * sortedOffset >= 0 then everything below that offset is considered to be fully sorted and is
   * returned as an option of "SpillableColumnarBatch". Everything else is spilt into smaller
   * batches as determined by this function and returned as a seq of "OutOfCoreBatch".
   * Call `saveSplitResult` to place them into the cache correspondingly.
   */
  private final def splitAfterSort(sortedSpill: SpillableColumnarBatch,
      sortedOffset: Int = -1): (Option[SpillableColumnarBatch], Seq[OutOfCoreBatch]) = {
    withResource(sortedSpill.getColumnarBatch()) { cb =>
      withResource(GpuColumnVector.from(cb)) { table =>
        splitTableAfterSort(table, sortedOffset)
      }
    }
  }

  private final def splitTableAfterSort(
      sortedTbl: Table,
      sortedOffset: Int): (Option[SpillableColumnarBatch], Seq[OutOfCoreBatch]) = {
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

    var sortedCb: Option[SpillableColumnarBatch] = None
    val pendingObs: ArrayBuffer[OutOfCoreBatch] = ArrayBuffer.empty
    if (sortedOffset == rows) {
      // The entire thing is sorted
      val batch = GpuColumnVector.from(sortedTbl, sorter.projectedBatchTypes)
      val sp = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      sortedCb = Some(sp)
    } else {
      val hasFullySortedData = sortedOffset > 0
      val splitIndexes = if (hasFullySortedData) {
        sortedOffset until rows by targetRowCount
      } else {
        targetRowCount until rows by targetRowCount
      }
      // Get back the first row so we can sort the batches
      val gatherIndexes = if (hasFullySortedData) {
        // The first batch is sorted so don't gather a row for it
        splitIndexes
      } else {
        Seq(0) ++ splitIndexes
      }

      val boundaries =
        withResource(new NvtxRange("boundaries", NvtxColor.ORANGE)) { _ =>
          withResource(ColumnVector.fromInts(gatherIndexes: _*)) { gatherMap =>
            withResource(sortedTbl.gather(gatherMap)) { boundariesTab =>
              convertBoundaries(boundariesTab)
            }
          }
        }

      withResource(sortedTbl.contiguousSplit(splitIndexes: _*)) { splits =>
        val stillPending = if (hasFullySortedData) {
          val sp = SpillableColumnarBatch(splits.head, sorter.projectedBatchTypes,
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          sortedCb = Some(sp)
          splits.slice(1, splits.length)
        } else {
          splits
        }

        closeOnExcept(sortedCb) { _ =>
          assert(boundaries.length == stillPending.length)
          closeOnExcept(pendingObs) { _ =>
            stillPending.zip(boundaries).foreach {
              case (ct: ContiguousTable, lower: UnsafeRow) =>
                if (ct.getRowCount > 0) {
                  val sp = SpillableColumnarBatch(ct, sorter.projectedBatchTypes,
                    SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
                  pendingObs += OutOfCoreBatch(sp, lower)
                } else {
                  ct.close()
                }
            }
          }
        }
      }
    }
    (sortedCb, pendingObs)
  }

  /** Save the splitting result returned from `splitAfterSort` into the cache */
  private final def saveSplitResult(
      result: (Option[SpillableColumnarBatch], Seq[OutOfCoreBatch])): Unit = {
    val (sortedSp, pendingObs) = result
    closeOnExcept(pendingObs) { _ =>
      sortedSp.foreach { sp =>
        sortedSize += sp.sizeInBytes
        sorted.add(sp)
      }
    }
    pendingObs.foreach(pending.add)
  }

  /**
   * First pass through the data. Read in all of the batches, sort each batch and split them up into
   * smaller chunks for later merge sorting.
   */
  private final def firstPassReadBatches(): Unit = {
    while(iter.hasNext) {
      val spillBatch = closeOnExcept(iter.next()) { batch =>
        SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      }
      val sortedIt =
        withResource(new NvtxWithMetrics("initial sort", NvtxColor.CYAN, opTime)){ _ =>
          withRetry(spillBatch, splitSpillableInHalfByRows) { attemptScb =>
            onFirstPassSort()
            withResource(attemptScb.getColumnarBatch()) { attemptCb =>
              sorter.appendProjectedAndSort(attemptCb, sortTime)
            }
          }
        }

      withResource(new NvtxWithMetrics("split input batch", NvtxColor.CYAN, opTime)) { _ =>
        while(sortedIt.hasNext) {
          val sortedTbl = sortedIt.next()
          val rows = sortedTbl.getRowCount.toInt
          // filter out empty batches
          if (rows > 0) {
            val sp = withResource(sortedTbl) { _ =>
              closeOnExcept(GpuColumnVector.from(sortedTbl, sorter.projectedBatchTypes)) { cb =>
                SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
              }
            }
            val ret = withRetryNoSplit(sp) { attempt =>
              onFirstPassSplit()
              splitAfterSort(attempt)
            }
            saveSplitResult(ret)
          } else {
            sortedTbl.close()
          }
        }
      }
    }
  }

  /**
   * Merge sort enough data that we can output a batch and put it in the sorted queue.
   * @return if we can determine that we can return a final batch without putting it in the sorted
   *         queue then optionally return it.
   */
  private final def mergeSortEnoughToOutput(): Option[ColumnarBatch] = {
    // Now get enough sorted data to return
    while (!pending.isEmpty && sortedSize < targetSize) {
      // Keep going until we have enough data to return
      var bytesLeftToFetch = targetSize
      val pendingSort = new ArrayStack[SpillableColumnarBatch]()
      closeOnExcept(pendingSort) { _ =>
        while (!pending.isEmpty &&
            (bytesLeftToFetch - pending.peek().buffer.sizeInBytes >= 0 || pendingSort.isEmpty)) {
          val buffer = pending.poll().buffer
          pendingSort.push(buffer)
          bytesLeftToFetch -= buffer.sizeInBytes
        }
      }
      // Here may need to retry work, however `mergeSortAndClose` already has some
      // optimization to reduce the GPU memory pressure for some cases.
      val mergedBatch = sorter.mergeSortAndClose(pendingSort, sortTime)

      val (retBatch, sortedOffset) = closeOnExcept(mergedBatch) { _ =>
        // First we want figure out what is fully sorted from what is not
        val sortSplitOffset = if (pending.isEmpty) {
          // No need to split it
          mergedBatch.numRows()
        } else {
          // The data is only fully sorted if there is nothing pending that is smaller than it
          // so get the next "smallest" row that is pending.
          val cutoff = pending.peek().firstRow
          withResource(converters.convertBatch(Array(cutoff),
            TrampolineUtil.fromAttributes(sorter.projectedBatchSchema))) { cutoffCb =>
            withResource(sorter.upperBound(mergedBatch, cutoffCb)) { result =>
              withResource(result.copyToHost()) { hostResult =>
                assert(hostResult.getRowCount == 1)
                hostResult.getInt(0)
              }
            }
          }
        }
        if (sortSplitOffset == mergedBatch.numRows() && sorted.isEmpty &&
          (GpuColumnVector.getTotalDeviceMemoryUsed(mergedBatch) >= targetSize ||
            pending.isEmpty)) {
          // This is a special case where we have everything we need to output already so why
          // bother with another contig split just to put it into the queue
          withResource(GpuColumnVector.from(mergedBatch)) { mergedTbl =>
            (Some(sorter.removeProjectedColumns(mergedTbl)), sortSplitOffset)
          }
        } else {
          (None, sortSplitOffset)
        }
      }

      if (retBatch.nonEmpty) {
        withResource(mergedBatch) { _ =>
          return retBatch
        }
      } else {
        val mergedSp = closeOnExcept(mergedBatch) { _ =>
          SpillableColumnarBatch(mergedBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        val splitResult = withRetryNoSplit(mergedSp) { attempt =>
          onMergeSortSplit()
          splitAfterSort(attempt, sortedOffset)
        }
        saveSplitResult(splitResult)
      }
    }
    None
  }

  /**
   * Take data from the sorted queue and return a final batch that can be returned
   * @return a batch that can be returned.
   */
  private final def concatOutput(): ColumnarBatch = {
    // combine all the sorted data into a single batch
    val spillCbs = ArrayBuffer[SpillableColumnarBatch]()
    var totalBytes = 0L
    closeOnExcept(spillCbs) { _ =>
      while (!sorted.isEmpty && (spillCbs.isEmpty ||
          (totalBytes + sorted.peek().sizeInBytes) < targetSize)) {
        val tmp = sorted.pop()
        sortedSize -= tmp.sizeInBytes
        totalBytes += tmp.sizeInBytes
        spillCbs += tmp
      }
    }
    if (spillCbs.length == 1) {
      // We cannot concat a single table
      withRetryNoSplit(spillCbs.head) { attemptSp =>
        onConcatOutput()
        withResource(attemptSp.getColumnarBatch()) { attemptCb =>
          withResource(GpuColumnVector.from(attemptCb)) { attemptTbl =>
            sorter.removeProjectedColumns(attemptTbl)
          }
        }
      }
    } else {
      // withRetryNoSplit will take over the batches.
      withRetryNoSplit(spillCbs) { attempt =>
        onConcatOutput()
        val tables = attempt.safeMap { sp =>
          withResource(sp.getColumnarBatch())(GpuColumnVector.from)
        }
        withResource(tables) { _ =>
          withResource(Table.concatenate(tables: _*)) { combined =>
            // ignore the output of removing the columns because it is just dropping columns
            // so it will be smaller than this with not added memory
            sorter.removeProjectedColumns(combined)
          }
        }
      }
    }
  }

  override def next(): ColumnarBatch = {
    if (sorter.projectedBatchSchema.isEmpty) {
      // special case, no columns just rows
      iter.next()
    } else {
      if (pending.isEmpty && sorted.isEmpty) {
        firstPassReadBatches()
      }
      withResource(new NvtxWithMetrics("Sort next output batch", NvtxColor.CYAN, opTime)) { _ =>
        val ret = mergeSortEnoughToOutput().getOrElse(concatOutput())

        outputBatches += 1
        outputRows += ret.numRows()
        // We already read in all of the data so calling hasNext is cheap
        if (hasNext) {
          ret
        } else {
          GpuColumnVector.tagAsFinalBatch(ret)
        }
      }
    }
  }

  override def close(): Unit = {
    sorted.forEach(_.close())
    pending.close()
  }

  /** Callbacks designed for unit tests only. Don't do any heavy things inside. */
  protected def onFirstPassSort(): Unit = {}
  protected def onFirstPassSplit(): Unit = {}
  protected def onMergeSortSplit(): Unit = {}
  protected def onConcatOutput(): Unit = {}
}
