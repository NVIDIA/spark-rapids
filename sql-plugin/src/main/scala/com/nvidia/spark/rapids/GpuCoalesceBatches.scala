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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, NvtxColor, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, NullType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Consumes an Iterator of ColumnarBatches and concatenates them into a single ColumnarBatch.
 * The batches will be closed when this operation is done.
 */
object ConcatAndConsumeAll {
  /**
   * Build a single batch from the batches collected so far. If array is empty this will likely
   * blow up.
   * @param arrayOfBatches the batches to concat. This will be consumed and you do not need to
   *                       close any of the batches after this is called.
   * @param schema the schema of the output types.
   * @return a single batch with all of them concated together.
   */
  def buildNonEmptyBatch(arrayOfBatches: Array[ColumnarBatch],
      schema: StructType): ColumnarBatch =
    buildNonEmptyBatchFromTypes(
      arrayOfBatches, GpuColumnVector.extractTypes(schema))

  /**
   * Build a single batch from the batches collected so far. If array is empty this will likely
   * blow up.
   * @param arrayOfBatches the batches to concat. This will be consumed and you do not need to
   *                       close any of the batches after this is called.
   * @param dataTypes the output types.
   * @return a single batch with all of them concated together.
   */
  def buildNonEmptyBatchFromTypes(arrayOfBatches: Array[ColumnarBatch],
                                  dataTypes: Array[DataType]): ColumnarBatch = {
    if (arrayOfBatches.length == 1) {
      arrayOfBatches(0)
    } else {
      val tables = arrayOfBatches.map(GpuColumnVector.from)
      try {
        val combined = Table.concatenate(tables: _*)
        try {
          GpuColumnVector.from(combined, dataTypes)
        } finally {
          combined.close()
        }
      } finally {
        tables.foreach(_.close())
        arrayOfBatches.foreach(_.close())
      }
    }
  }

  /**
   * Verify that a single batch was returned from the iterator, or if it is empty return an empty
   * batch.
   * @param batches batches to be consumed.
   * @param format the format of the batches in case we need to return an empty batch.  Typically
   *               this is the output of your exec.
   * @return the single batch or an empty batch if needed.  Please be careful that your exec
   *         does not return empty batches as part of an RDD.
   */
  def getSingleBatchWithVerification(batches: Iterator[ColumnarBatch],
      format: Seq[Attribute]): ColumnarBatch = {
    import collection.JavaConverters._
    if (!batches.hasNext) {
      GpuColumnVector.emptyBatch(format.asJava)
    } else {
      val batch = batches.next()
      if (batches.hasNext) {
        batch.close()
        throw new IllegalStateException("Expected to only receive a single batch")
      }
      batch
    }
  }
}

object CoalesceGoal {
  def maxRequirement(a: CoalesceGoal, b: CoalesceGoal): CoalesceGoal = (a, b) match {
    case (_: RequireSingleBatchLike, _) => a
    case (_, _: RequireSingleBatchLike) => b
    case (_: BatchedByKey, _: TargetSize) => a
    case (_: TargetSize, _: BatchedByKey) => b
    case (a: BatchedByKey, b: BatchedByKey) =>
      if (satisfies(a, b)) {
        a // They are equal so it does not matter
      } else {
        // Nothing is the same so there is no guarantee
        BatchedByKey(Seq.empty)(Seq.empty)
      }
    case (TargetSize(aSize), TargetSize(bSize)) if aSize > bSize => a
    case _ => b
  }

  def minProvided(a: CoalesceGoal, b:CoalesceGoal): CoalesceGoal = (a, b) match {
    case (_: RequireSingleBatchLike, _) => b
    case (_, _: RequireSingleBatchLike) => a
    case (_: BatchedByKey, _: TargetSize) => b
    case (_: TargetSize, _: BatchedByKey) => a
    case (a: BatchedByKey, b: BatchedByKey) =>
      if (satisfies(a, b)) {
        a // They are equal so it does not matter
      } else {
        null
      }
    case (TargetSize(aSize), TargetSize(bSize)) if aSize < bSize => a
    case _ => b
  }

  def satisfies(found: CoalesceGoal, required: CoalesceGoal): Boolean = (found, required) match {
    case (_: RequireSingleBatchLike, _) => true
    case (_, _: RequireSingleBatchLike) => false
    case (_: BatchedByKey, _: TargetSize) => true
    case (_: TargetSize, _: BatchedByKey) => false
    case (BatchedByKey(aOrder), BatchedByKey(bOrder)) =>
      aOrder.length == bOrder.length &&
          aOrder.zip(bOrder).forall {
            case (a, b) => a.satisfies(b)
          }
    case (TargetSize(foundSize), TargetSize(requiredSize)) => foundSize >= requiredSize
    case _ => false // found is null so it is not satisfied
  }
}

/**
 * Provides a goal for batching of data.
 */
sealed abstract class CoalesceGoal extends GpuUnevaluable with ShimExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = NullType

  override def children: Seq[Expression] = Seq.empty
}

sealed abstract class CoalesceSizeGoal extends CoalesceGoal {

  val targetSizeBytes: Long = Integer.MAX_VALUE
}

/**
 * Trait used for pattern matching for single batch coalesce goals.
 */
trait RequireSingleBatchLike

/**
 * Trait used for pattern matching for goals that could be split, as they
 * only specify that batches won't be too much bigger than a maximum target
 * size in bytes.
 */
trait SplittableGoal

/**
 * A single batch is required as the input to a node in the SparkPlan. This means
 * all of the data for a given task is in a single batch. This should be avoided
 * as much as possible because it can result in running out of memory or run into
 * limitations of the batch size by both Spark and cudf.
 */
case object RequireSingleBatch extends CoalesceSizeGoal with RequireSingleBatchLike {

  override val targetSizeBytes: Long = Long.MaxValue

  /** Override toString to improve readability of Spark explain output */
  override def toString: String = "RequireSingleBatch"
}

/**
 * This is exactly the same as `RequireSingleBatch` except that if the
 * batch would fail to coalesce because it reaches cuDF row-count limits, the
 * coalesce code is free to null filter given the filter expression in `filterExpression`.
 * @note This is an ugly hack because ideally these rows are never read from the input source
 *       given that we normally push down IsNotNull in Spark. This should be removed when
 *       we can handle this in a proper way, likely at the logical plan optimization level.
 *       More details here: https://issues.apache.org/jira/browse/SPARK-39131
 */
case class RequireSingleBatchWithFilter(filterExpression: GpuExpression)
    extends CoalesceSizeGoal with RequireSingleBatchLike {

  override val targetSizeBytes: Long = Long.MaxValue

  /** Override toString to improve readability of Spark explain output */
  override def toString: String = "RequireSingleBatchWithFilter"
}
/**
 * Produce a stream of batches that are at most the given size in bytes. The size
 * is estimated in some cases so it may go over a little, but it should generally be
 * very close to the target size. Generally you should not go over 2 GiB to avoid
 * limitations in cudf for nested type columns.
 * @param targetSizeBytes the size of each batch in bytes.
 */
case class TargetSize(override val targetSizeBytes: Long)
    extends CoalesceSizeGoal
        with SplittableGoal {
  require(targetSizeBytes <= Integer.MAX_VALUE,
    "Target cannot exceed 2GB without checks for cudf row count limit")
}

/**
 * Split the data into batches where a set of keys are all within a single batch. This is
 * generally used for things like a window operation or a sort based aggregation where you
 * want all of the keys for a given operation to be available so the GPU can produce a
 * correct answer. There is no limit on the target size so if there is a lot of data skew
 * for a key, the batch may still run into limits on set by Spark or cudf. It should be noted
 * that it is required that a node in the Spark plan that requires this should also require
 * an input ordering that satisfies this ordering as well.
 * @param gpuOrder the GPU keys that should be used for batching.
 * @param cpuOrder the CPU keys that should be used for batching.
 */
case class BatchedByKey(gpuOrder: Seq[SortOrder])(val cpuOrder: Seq[SortOrder])
    extends CoalesceGoal {
  require(gpuOrder.size == cpuOrder.size)

  override def otherCopyArgs: Seq[AnyRef] = cpuOrder :: Nil

  override def children: Seq[Expression] = gpuOrder
}

abstract class AbstractGpuCoalesceIterator(
    inputIter: Iterator[ColumnarBatch],
    goal: CoalesceSizeGoal,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    streamTime: GpuMetric,
    concatTime: GpuMetric,
    opTime: GpuMetric,
    opName: String) extends Iterator[ColumnarBatch] with Logging {

  private val iter = new CollectTimeIterator(s"$opName: collect", inputIter, streamTime)

  private var batchInitialized: Boolean = false

  /**
   * This iterator is redefined if this coalesce iterator is under a `SplittableGoal`
   * and so might retry and split given OOMs
   */
  private var coalesceBatchIterator: Iterator[ColumnarBatch] = Iterator.empty

  /**
   * This is defined iff `goal` is `RequireSingleBatchWithFilter` and we have
   * reached the cuDF row-count limit.
   */
  private var inputFilterExpression: Option[Expression] = None

  /**
   * Return true if there is something saved on deck for later processing.
   */
  protected def hasOnDeck: Boolean

  /**
   * Save a batch for later processing. In case of an exception raised while
   * saving the batch, saveOnDeck guarantees it closes batch.
   */
  protected def saveOnDeck(batch: ColumnarBatch): Unit

  /**
   * If there is anything saved on deck close it.
   */
  protected def clearOnDeck(): Unit

  /**
   * Remove whatever is on deck and return it.
   */
  protected def popOnDeck(): ColumnarBatch

  /** Perform the necessary cleanup for an input batch */
  protected def cleanupInputBatch(batch: ColumnarBatch): Unit = batch.close()

  /** Optional row limit */
  var batchRowLimit: Int = 0

  // note that TaskContext.get() can return null during unit testing so we wrap it in an
  // option here
  Option(TaskContext.get())
      .foreach(_.addTaskCompletionListener[Unit](_ => clearOnDeck()))

  private def getHasOnDeck: Boolean = {
    while (!hasOnDeck && iter.hasNext) {
      val cb = iter.next()
      withResource(new MetricRange(opTime)) { _ =>
        val numRows = cb.numRows()
        numInputBatches += 1
        numInputRows += numRows
        if (numRows > 0) {
          saveOnDeck(cb)
        } else {
          cleanupInputBatch(cb)
        }
      }
    }
    hasOnDeck
  }

  override def hasNext: Boolean = {
    coalesceBatchIterator.hasNext || getHasOnDeck
  }

  /**
   * Called first to initialize any state needed for a new batch to be created.
   */
  def initNewBatch(batch: ColumnarBatch): Unit

  /**
   * Called to add a new batch to the final output batch. The batch passed in will
   * not be closed.  If it needs to be closed it is the responsibility of the child class
   * to do it.
   *
   * @param batch the batch to add in.
   */
  def addBatchToConcat(batch: ColumnarBatch): Unit

  /**
   * Called after all of the batches have been added in.
   *
   * @return the concated batches on the GPU.
   */
  def concatAllAndPutOnGPU(): ColumnarBatch

  /**
   * True for coalesce iterators that support an iterator that can retry
   * and produce smaller batches on OOMs.
   */
  protected val supportsRetryIterator: Boolean = true

  /**
   * Function that returns a retry iterator that returns coalesced batches, as much
   * as possible.
   *
   * Note this throws if the subclass does not support splitting its input.
   * (supportsRetryIterator = false)
   *
   * @return an iterator that should be used to obtain coalesced batches
   */
  def getCoalesceRetryIterator: Iterator[ColumnarBatch]

  /**
   * Called to cleanup any state when a batch is done (even if there was a failure)
   */
  def cleanupConcatIsDone(): Unit

  /**
   * Gets the size in bytes of the data buffer for a given column
   */
  def getBatchDataSize(cb: ColumnarBatch): Long = {
    if (cb.numCols() > 0) {
      cb.column(0) match {
        case g: GpuColumnVectorFromBuffer =>
          g.getBuffer.getLength
        case _: GpuColumnVector =>
          (0 until cb.numCols()).map {
            i => cb.column(i).asInstanceOf[GpuColumnVector].getBase.getDeviceMemorySize
          }.sum
        case g: GpuCompressedColumnVector =>
          g.getTableBuffer.getLength
        case g =>
          throw new IllegalStateException(s"Unexpected column type: $g")
      }
    } else {
      0
    }
  }

  /**
   * Add input batches to the `batches` collection up to the limit specified
   * by the goal. Note: for a size goal, if any incoming batch is greater than this size
   * it will be passed through unmodified.
   *
   * If the coalesce goal is `RequireSingleBatch` then an exception will be thrown if there
   * is remaining data after the first batch is added.
   *
   * @note protected for testing
   * @return boolean that is true if this call reached the last input batch.
   */
  protected def populateCandidateBatches(): Boolean = {
    var numRows: Long = 0 // to avoid overflows
    var numBytes: Long = 0

    // check if there is a batch "on deck" from a previous call to next()
    if (hasOnDeck) {
      val batch = popOnDeck()
      numRows += batch.numRows()
      numBytes += getBatchDataSize(batch)
      addBatch(batch)
    }

    // there is a hard limit of 2^31 rows
    while (numRows < Int.MaxValue && !hasOnDeck && iter.hasNext) {
      val cbFromIter = iter.next()

      var cb = if (inputFilterExpression.isDefined) {
        // If we have reached the cuDF limit once, proactively filter batches
        // after that first limit is reached.
        GpuFilter(cbFromIter, inputFilterExpression.get)
      } else {
        cbFromIter
      }

      closeOnExcept(cb) { _ =>
        val nextRows = cb.numRows()
        numInputBatches += 1

        // filter out empty batches
        if (nextRows > 0) {
          numInputRows += nextRows
          val nextBytes = getBatchDataSize(cb)

          // calculate the new sizes based on this input batch being added to the current
          // output batch
          val wouldBeRows = numRows + nextRows
          val wouldBeBytes = numBytes + nextBytes

          if (wouldBeRows > Int.MaxValue) {
            goal match {
              case RequireSingleBatch =>
                throw new IllegalStateException("A single batch is required for this operation," +
                    s" but cuDF only supports ${Int.MaxValue} rows. At least $wouldBeRows" +
                    s" are in this partition. Please try increasing your partition count.")
              case RequireSingleBatchWithFilter(filterExpression) =>
                // filter what we had already stored
                val filteredDown = GpuFilter(concatAllAndPutOnGPU(), filterExpression)
                closeOnExcept(filteredDown) { _ =>
                  // filter the incoming batch as well
                  closeOnExcept(GpuFilter(cb, filterExpression)) { filteredCb =>
                    cb = null // null out `cb` to prevent multiple close calls
                    val filteredWouldBeRows = filteredDown.numRows() + filteredCb.numRows()
                    if (filteredWouldBeRows > Int.MaxValue) {
                      throw new IllegalStateException(
                        "A single batch is required for this operation, but cuDF only supports " +
                            s"${Int.MaxValue} rows. At least $filteredWouldBeRows are in this " +
                            "partition, even after filtering nulls. " +
                            "Please try increasing your partition count.")
                    }
                    if (inputFilterExpression.isEmpty) {
                      inputFilterExpression = Some(filterExpression)
                      logWarning("Switched to null-filtering mode. This coalesce iterator " +
                          "succeeded to fit rows under the cuDF limit only after null filtering. " +
                          "Please try increasing your partition count.")
                    }
                    numRows = filteredWouldBeRows
                    numBytes = getBatchDataSize(filteredDown) + getBatchDataSize(filteredCb)
                    addBatch(filteredDown)
                    addBatch(filteredCb)
                  }
                }
              case _ => saveOnDeck(cb) // not a single batch requirement
            }
          } else if (batchRowLimit > 0 && wouldBeRows > batchRowLimit) {
            saveOnDeck(cb)
          } else if (wouldBeBytes > goal.targetSizeBytes && numBytes > 0) {
            // There are no explicit checks for the concatenate result exceeding the cudf 2^31
            // row count limit for any column. We are relying on cudf's concatenate to throw
            // an exception if this occurs and limiting performance-oriented goals to under
            // 2GB data total to avoid hitting that error.
            saveOnDeck(cb)
          } else {
            addBatch(cb)
            numRows = wouldBeRows
            numBytes = wouldBeBytes
          }
        } else {
          cleanupInputBatch(cb)
        }
      }
    }

    val isLastBatch = !(hasOnDeck || iter.hasNext)

    // enforce single batch limit when appropriate
    if (!isLastBatch) {
      goal match {
        case _: RequireSingleBatchLike =>
          throw new IllegalStateException("A single batch is required for this operation," +
              " Please try increasing your partition count.")
        case _ =>
      }
    }
    isLastBatch
  }

  var wasLastBatch: Boolean = false

  /**
   * Each call to next() will combine batches according to the goal specified.
   * However, if any incoming batch is greater than this size it will be passed
   * through unmodified.
   *
   * If the coalesce goal is `RequireSingleBatch` then an exception will be thrown if there
   * is remaining data after the first batch is produced.
   *
   * If OOMs occur while coalescing (which may include decompression depending on the
   * instance), this may be retried, and as a result `ColumnarBatch` may be smaller than
   * desired, since we follow a "coalesce half of the batches" strategy, which should
   * half the number of batches that are candidates for coalesce at each OOM, leaving the rest
   * for a subsequent call to `next`.
   *
   * @return The coalesced batch
   */
  override def next(): ColumnarBatch = withResource(new MetricRange(opTime)) { _ =>
    if (coalesceBatchIterator.hasNext) {
      val batch = coalesceBatchIterator.next()
      if (wasLastBatch) {
        // if the coalesce iterator is empty, and nothing is left on deck
        if (!hasNext) {
          GpuColumnVector.tagAsFinalBatch(batch)
        } // else, we already marked `wasLastBatch`, will check it again
          // next time.
      }
      numOutputRows += batch.numRows()
      numOutputBatches += 1
      batch
    } else {
      // reset batch state
      batchInitialized = false
      batchRowLimit = 0

      try {
        val isLastBatch = if (!coalesceBatchIterator.hasNext) {
          populateCandidateBatches()
        } else {
          wasLastBatch
        }

        withResource(new NvtxWithMetrics(s"$opName concat", NvtxColor.CYAN, concatTime)) { _ =>
          goal match {
            case _: SplittableGoal if supportsRetryIterator =>
              coalesceBatchIterator = getCoalesceRetryIterator
              val batch = coalesceBatchIterator.next()
              if (isLastBatch) {
                if (!hasNext) {
                  GpuColumnVector.tagAsFinalBatch(batch)
                } else {
                  wasLastBatch = true // but couldn't mark this one because there are leftovers
                }
              }
              numOutputRows += batch.numRows()
              numOutputBatches += 1
              batch
            case _ =>
              val singleBatch = concatAllAndPutOnGPU()
              if (isLastBatch) {
                GpuColumnVector.tagAsFinalBatch(singleBatch)
              }
              numOutputRows += singleBatch.numRows()
              numOutputBatches += 1
              singleBatch
          }
        }
      } finally {
        cleanupConcatIsDone()
      }
    }
  }

  private def addBatch(batch: ColumnarBatch): Unit = {
    if (!batchInitialized) {
      initNewBatch(batch)
      batchInitialized = true
    }
    addBatchToConcat(batch)
  }

  /**
   * Splits a `BatchesToCoalesce` instance into two.
   * @return Seq[BatchesToCoalesce] with 2 items.
   */
  protected def splitBatchesToCoalesceFn: BatchesToCoalesce => Seq[BatchesToCoalesce] = {
    (batchesToCoalesce: BatchesToCoalesce) => {
      closeOnExcept(batchesToCoalesce) { _ =>
        val it = batchesToCoalesce.batches
        val numBatches = it.length
        if (numBatches <= 1) {
          throw new OutOfMemoryError(s"Cannot split a sequence of $numBatches batches")
        }
        val res = it.splitAt(numBatches / 2)
        Seq(BatchesToCoalesce(res._1), BatchesToCoalesce(res._2))
      }
    }
  }
}

/**
 * A helper class that contains a sequence of SpillableColumnarBatch and that
 * can be used to split the sequence into two. This class is auto closeable,
 * as it is sent to code that will close it, and in turn close the SpillableColumnarBatch
 * instances in `batches`
 * @param batches a sequence of `SpillableColumnarBatch` to manage.
 */
case class BatchesToCoalesce(batches: Array[SpillableColumnarBatch])
    extends AutoCloseable {
  override def close(): Unit = {
    batches.safeClose()
  }
}

class GpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    sparkTypes: Array[DataType],
    goal: CoalesceSizeGoal,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    opTime: GpuMetric,
    opName: String)
  extends AbstractGpuCoalesceIterator(iter,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    opTime,
    opName) {

  protected val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty

  override def initNewBatch(batch: ColumnarBatch): Unit = {
    batches.safeClose()
    batches.clear()
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit =
    batches.append(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY))

  private def concatBatches(batches: Array[SpillableColumnarBatch]): ColumnarBatch = {
    val wip = batches.safeMap(_.getColumnarBatch())
    ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(wip, sparkTypes)
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    val candidates = batches.clone()
    batches.clear()
    withRetryNoSplit(candidates) { attempt =>
      concatBatches(attempt.toArray)
    }
  }

  override def getCoalesceRetryIterator: Iterator[ColumnarBatch] = {
    val candidates = BatchesToCoalesce(batches.clone().toArray)
    batches.clear()
    withRetry(candidates, splitBatchesToCoalesceFn) { attempt: BatchesToCoalesce =>
      concatBatches(attempt.batches)
    }
  }

  override def cleanupConcatIsDone(): Unit = {
    batches.clear()
  }

  private var onDeck: Option[SpillableColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined

  override protected def saveOnDeck(batch: ColumnarBatch): Unit = {
    // wrap batch on a closeOnExcept, in case assert throws
    closeOnExcept(batch) { _ =>
      assert(onDeck.isEmpty)
    }
    onDeck = Some(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
  }

  override protected def clearOnDeck(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }

  override protected def popOnDeck(): ColumnarBatch = {
    val ret = onDeck.get.getColumnarBatch()
    clearOnDeck()
    ret
  }
}

/**
 * Compression codec-aware `GpuCoalesceIterator` subclass which should be used in cases
 * where the RAPIDS Shuffle Manager could be configured, as batches to be coalesced
 * may be compressed.
 */
class GpuCompressionAwareCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    sparkTypes: Array[DataType],
    goal: CoalesceSizeGoal,
    maxDecompressBatchMemory: Long,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    opTime: GpuMetric,
    opName: String,
    codecConfigs: TableCompressionCodecConfig)
  extends GpuCoalesceIterator(
    iter, sparkTypes, goal,
    numInputRows = numInputRows,
    numInputBatches = numInputBatches,
    numOutputRows = numOutputRows,
    numOutputBatches = numOutputBatches,
    collectTime = collectTime,
    concatTime = concatTime,
    opTime = opTime,
    opName) {

  private[this] var codec: TableCompressionCodec = _

  private def concatBatches(batches: Array[SpillableColumnarBatch]): ColumnarBatch = {
    val toConcat = closeOnExcept(batches.safeMap(_.getColumnarBatch())) { wip =>
      val compressedBatchIndices = wip.zipWithIndex.filter { pair =>
        GpuCompressedColumnVector.isBatchCompressed(pair._1)
      }.map(_._2)
      if (compressedBatchIndices.nonEmpty) {
        val compressedVecs = compressedBatchIndices.map { batchIndex =>
          wip(batchIndex).column(0).asInstanceOf[GpuCompressedColumnVector]
        }
        if (codec == null) {
          val descr = compressedVecs.head.getTableMeta.bufferMeta.codecBufferDescrs(0)
          codec = TableCompressionCodec.getCodec(descr.codec, codecConfigs)
        }
        withResource(codec.createBatchDecompressor(maxDecompressBatchMemory,
          Cuda.DEFAULT_STREAM)) { decompressor =>
          compressedVecs.foreach { cv =>
            val buffer = cv.getTableBuffer
            val bufferMeta = cv.getTableMeta.bufferMeta
            // don't currently support switching codecs when partitioning
            buffer.incRefCount()
            decompressor.addBufferToDecompress(buffer, bufferMeta)
          }
          withResource(decompressor.finishAsync()) { outputBuffers =>
            outputBuffers.zipWithIndex.foreach { case (outputBuffer, outputIndex) =>
              val cv = compressedVecs(outputIndex)
              val batchIndex = compressedBatchIndices(outputIndex)
              val compressedBatch = wip(batchIndex)
              withResource(compressedBatch) { _ =>
                // the decompressed batch should get a new meta without codec information
                // so that future materializations of the batch don't get confused and attempt
                // to use GpuCompressedColumnVector instead of GpuPackedTableColumn
                wip(batchIndex) =
                  MetaUtils.getBatchFromMeta(
                    outputBuffer, MetaUtils.dropCodecs(cv.getTableMeta), sparkTypes)
              }
            }
          }
        }
      }
      wip
    }
    ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(toConcat, sparkTypes)
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    val candidates = batches.clone()
    batches.clear()
    withRetryNoSplit(candidates) { attempt =>
      concatBatches(attempt.toArray)
    }
  }

  override def getCoalesceRetryIterator(): Iterator[ColumnarBatch] = {
    val candidates = BatchesToCoalesce(batches.clone().toArray)
    batches.clear()
    withRetry(candidates, splitBatchesToCoalesceFn) { attempt: BatchesToCoalesce =>
      concatBatches(attempt.batches)
    }
  }
}

case class GpuCoalesceBatches(child: SparkPlan, goal: CoalesceGoal)
  extends ShimUnaryExecNode with GpuExec {
  import GpuMetric._

  private[this] val (codecConfigs, maxDecompressBatchMemory) = {
    val rapidsConf = new RapidsConf(child.conf)
    (TableCompressionCodec.makeCodecConfig(rapidsConf),
     rapidsConf.shuffleCompressionMaxBatchMemory)
  }

  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputBatching: CoalesceGoal = goal

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = goal match {
    case batchingGoal: BatchedByKey =>
      Seq(batchingGoal.cpuOrder)
    case _ =>
      super.requiredChildOrdering
  }

  override def outputOrdering: Seq[SortOrder] = goal match {
    case batchingGoal: BatchedByKey =>
      batchingGoal.cpuOrder
    case _ =>
      child.outputOrdering
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val opTime = gpuLongMetric(OP_TIME)

    // cache in local vars to avoid serializing the plan
    val outputSchema = schema
    val dataTypes = GpuColumnVector.extractTypes(outputSchema)
    val decompressMemoryTarget = maxDecompressBatchMemory

    val batches = child.executeColumnar()
    val localCodecConfigs = codecConfigs
    if (outputSchema.isEmpty) {
      batches.mapPartitions { iter =>
        val numRows = iter.map(_.numRows).sum
        val combinedCb = new ColumnarBatch(Array.empty, numRows)
        Iterator.single(combinedCb)
      }
    } else {
      goal match {
        case sizeGoal: CoalesceSizeGoal =>
          batches.mapPartitions { iter =>
            new GpuCompressionAwareCoalesceIterator(
              iter, dataTypes, sizeGoal, decompressMemoryTarget,
              numInputRows, numInputBatches, numOutputRows, numOutputBatches, NoopMetric,
              concatTime, opTime, "GpuCoalesceBatches",
              localCodecConfigs)
          }
        case batchingGoal: BatchedByKey =>
          val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
          val f = GpuKeyBatchingIterator.makeFunc(batchingGoal.gpuOrder, output.toArray, targetSize,
            numInputRows, numInputBatches, numOutputRows, numOutputBatches,
            concatTime, opTime)
          batches.mapPartitions { iter =>
            f(iter)
          }
      }
    }
  }
}
