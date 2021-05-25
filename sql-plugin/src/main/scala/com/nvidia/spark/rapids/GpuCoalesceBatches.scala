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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, NvtxColor, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructType}
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
      schema: StructType): ColumnarBatch = {
    if (arrayOfBatches.length == 1) {
      arrayOfBatches(0)
    } else {
      val tables = arrayOfBatches.map(GpuColumnVector.from)
      try {
        val combined = Table.concatenate(tables: _*)
        try {
          GpuColumnVector.from(combined, GpuColumnVector.extractTypes(schema))
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
  def max(a: CoalesceGoal, b: CoalesceGoal): CoalesceGoal = (a, b) match {
    case (RequireSingleBatch, _) => a
    case (_, RequireSingleBatch) => b
    case (TargetSize(aSize), TargetSize(bSize)) if aSize > bSize => a
    case _ => b
  }

  def min(a: CoalesceGoal, b:CoalesceGoal): CoalesceGoal = (a, b) match {
    case (RequireSingleBatch, _) => b
    case (_, RequireSingleBatch) => a
    case (TargetSize(aSize), TargetSize(bSize)) if aSize < bSize => a
    case _ => b
  }

  def satisfies(found: CoalesceGoal, required: CoalesceGoal): Boolean = (found, required) match {
    case (RequireSingleBatch, _) => true
    case (_, RequireSingleBatch) => false
    case (TargetSize(foundSize), TargetSize(requiredSize)) => foundSize >= requiredSize
    case _ => false // found is null so it is not satisfied
  }
}

sealed abstract class CoalesceGoal extends Serializable {

  val targetSizeBytes: Long = Integer.MAX_VALUE
}

object RequireSingleBatch extends CoalesceGoal {

  override val targetSizeBytes: Long = Long.MaxValue

  /** Override toString to improve readability of Spark explain output */
  override def toString: String = "RequireSingleBatch"
}

case class TargetSize(override val targetSizeBytes: Long) extends CoalesceGoal {
  require(targetSizeBytes <= Integer.MAX_VALUE,
    "Target cannot exceed 2GB without checks for cudf row count limit")
}

abstract class AbstractGpuCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    totalTime: GpuMetric,
    opName: String) extends Iterator[ColumnarBatch] with Arm with Logging {
  private var batchInitialized: Boolean = false

  /**
   * Return true if there is something saved on deck for later processing.
   */
  protected def hasOnDeck: Boolean

  /**
   * Save a batch for later processing.
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

  /** Optional row limit */
  var batchRowLimit: Int = 0

  // note that TaskContext.get() can return null during unit testing so we wrap it in an
  // option here
  Option(TaskContext.get())
      .foreach(_.addTaskCompletionListener[Unit](_ => clearOnDeck()))

  private def iterHasNext: Boolean = withResource(new MetricRange(collectTime)) { _ =>
    iter.hasNext
  }

  private def iterNext(): ColumnarBatch = withResource(new MetricRange(collectTime)) { _ =>
    iter.next()
  }

  override def hasNext: Boolean = withResource(new MetricRange(totalTime)) { _ =>
    while (!hasOnDeck && iterHasNext) {
      closeOnExcept(iterNext()) { cb =>
        val numRows = cb.numRows()
        numInputBatches += 1
        numInputRows += numRows
        if (numRows > 0) {
          saveOnDeck(cb)
        } else {
          cb.close()
        }
      }
    }
    hasOnDeck
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
   * Each call to next() will combine incoming batches up to the limit specified
   * by [[RapidsConf.GPU_BATCH_SIZE_BYTES]]. However, if any incoming batch is greater
   * than this size it will be passed through unmodified.
   *
   * If the coalesce goal is `RequireSingleBatch` then an exception will be thrown if there
   * is remaining data after the first batch is produced.
   *
   * @return The coalesced batch
   */
  override def next(): ColumnarBatch = withResource(new MetricRange(totalTime)) { _ =>
    // reset batch state
    batchInitialized = false
    batchRowLimit = 0

    try {
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
      while (numRows < Int.MaxValue && !hasOnDeck && iterHasNext) {
        closeOnExcept(iterNext()) { cb =>
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
              if (goal == RequireSingleBatch) {
                throw new IllegalStateException("A single batch is required for this operation," +
                    s" but cuDF only supports ${Int.MaxValue} rows. At least $wouldBeRows" +
                    s" are in this partition. Please try increasing your partition count.")
              }
              saveOnDeck(cb)
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
            cb.close()
          }
        }
      }

      // enforce single batch limit when appropriate
      if (goal == RequireSingleBatch && (hasOnDeck || iterHasNext)) {
        throw new IllegalStateException("A single batch is required for this operation." +
            " Please try increasing your partition count.")
      }

      numOutputRows += numRows
      numOutputBatches += 1
      withResource(new NvtxWithMetrics(s"$opName concat", NvtxColor.CYAN, concatTime)) { _ =>
        concatAllAndPutOnGPU()
      }
    } finally {
      cleanupConcatIsDone()
    }
  }

  private def addBatch(batch: ColumnarBatch): Unit = {
    if (!batchInitialized) {
      initNewBatch(batch)
      batchInitialized = true
    }
    addBatchToConcat(batch)
  }
}

class GpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    schema: StructType,
    goal: CoalesceGoal,
    maxDecompressBatchMemory: Long,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    totalTime: GpuMetric,
    peakDevMemory: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback,
    opName: String)
  extends AbstractGpuCoalesceIterator(iter,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    totalTime,
    opName) with Arm {

  private val sparkTypes: Array[DataType] = GpuColumnVector.extractTypes(schema)
  private val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
  private var maxDeviceMemory: Long = 0

  override def initNewBatch(batch: ColumnarBatch): Unit = {
    batches.safeClose()
    batches.clear()
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit =
    batches.append(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY,
      spillCallback))

  private[this] var codec: TableCompressionCodec = _

  private[this] def popAllDecompressed(): Array[ColumnarBatch] = {
    closeOnExcept(batches.map(_.getColumnarBatch())) { wip =>
      batches.safeClose()
      batches.clear()

      val compressedBatchIndices = wip.zipWithIndex.filter { pair =>
        GpuCompressedColumnVector.isBatchCompressed(pair._1)
      }.map(_._2)
      if (compressedBatchIndices.nonEmpty) {
        val compressedVecs = compressedBatchIndices.map { batchIndex =>
          wip(batchIndex).column(0).asInstanceOf[GpuCompressedColumnVector]
        }
        if (codec == null) {
          val descr = compressedVecs.head.getTableMeta.bufferMeta.codecBufferDescrs(0)
          codec = TableCompressionCodec.getCodec(descr.codec)
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
              wip(batchIndex) =
                  MetaUtils.getBatchFromMeta(outputBuffer, cv.getTableMeta, sparkTypes)
              compressedBatch.close()
            }
          }
        }
      }
      wip.toArray
    }
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    val ret = ConcatAndConsumeAll.buildNonEmptyBatch(popAllDecompressed(), schema)
    // sum of current batches and concatenating batches. Approximately sizeof(ret * 2).
    maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret) * 2
    ret
  }

  override def cleanupConcatIsDone(): Unit = {
    peakDevMemory.set(maxDeviceMemory)
    batches.clear()
  }

  private var onDeck: Option[SpillableColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined

  override protected def saveOnDeck(batch: ColumnarBatch): Unit = {
    assert(onDeck.isEmpty)
    onDeck = Some(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
      spillCallback))
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

case class GpuCoalesceBatches(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode with GpuExec {
  import GpuMetric._

  private[this] val maxDecompressBatchMemory =
    new RapidsConf(child.conf).shuffleCompressionMaxBatchMemory

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    COLLECT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_COLLECT_TIME),
    CONCAT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY)
  ) ++ spillMetrics

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputBatching: CoalesceGoal = goal

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val peakDevMemory = gpuLongMetric(PEAK_DEVICE_MEMORY)

    // cache in local vars to avoid serializing the plan
    val outputSchema = schema
    val decompressMemoryTarget = maxDecompressBatchMemory

    val batches = child.executeColumnar()
    batches.mapPartitions { iter =>
      if (outputSchema.isEmpty) {
        val numRows = iter.map(_.numRows).sum
        val combinedCb = new ColumnarBatch(Array.empty, numRows)
        Iterator.single(combinedCb)
      } else {
        val callback = GpuMetric.makeSpillCallback(allMetrics)
        new GpuCoalesceIterator(iter, outputSchema, goal, decompressMemoryTarget,
          numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime,
          concatTime, totalTime, peakDevMemory, callback, "GpuCoalesceBatches")
      }
    }
  }
}
