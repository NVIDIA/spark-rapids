/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BufferType, NvtxColor, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataTypes, StructType}
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
   * @return a single batch with all of them concated together.
   */
  def buildNonEmptyBatch(arrayOfBatches: Array[ColumnarBatch]): ColumnarBatch = {
    if (arrayOfBatches.length == 1) {
      arrayOfBatches(0)
    } else {
      val tables = arrayOfBatches.map(GpuColumnVector.from)
      try {
        val combined = Table.concatenate(tables: _*)
        try {
          GpuColumnVector.from(combined)
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
}

sealed abstract class CoalesceGoal extends Serializable {

  val targetSizeBytes: Long = Integer.MAX_VALUE
}

object RequireSingleBatch extends CoalesceGoal {

  override val targetSizeBytes: Long = Long.MaxValue

  /** Override toString to improve readability of Spark explain output */
  override def toString: String = "RequireSingleBatch"
}

case class TargetSize(override val targetSizeBytes: Long) extends CoalesceGoal

abstract class AbstractGpuCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    schema: StructType,
    goal: CoalesceGoal,
    numInputRows: SQLMetric,
    numInputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    collectTime: SQLMetric,
    concatTime: SQLMetric,
    totalTime: SQLMetric,
    opName: String) extends Iterator[ColumnarBatch] with Logging {
  private var batchInitialized: Boolean = false
  private var collectMetric: Option[MetricRange] = None
  private var totalMetric: Option[MetricRange] = None

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

  /** We need to track the sizes of string columns to make sure we don't exceed 2GB */
  private val stringFieldIndices: Array[Int] = schema.fields.zipWithIndex
    .filter(_._1.dataType == DataTypes.StringType)
    .map(_._2)

  /** Optional row limit */
  var batchRowLimit: Int = 0

  // note that TaskContext.get() can return null during unit testing so we wrap it in an
  // option here
  Option(TaskContext.get())
      .foreach(_.addTaskCompletionListener[Unit]( _ => clearOnDeck()))

  override def hasNext: Boolean = {
    if (collectMetric.isEmpty) {
      // use one being not set as indicator that neither are intialized to avoid
      // 2 checks or extra initialized variable
      collectMetric = Some(new MetricRange(collectTime))
      totalMetric = Some(new MetricRange(totalTime))
    }
    while (!hasOnDeck && iter.hasNext) {
      val cb = iter.next()
      val numRows = cb.numRows()
      numInputBatches += 1
      numInputRows += numRows
      if (numRows > 0) {
        saveOnDeck(cb)
      } else {
        cb.close()
      }
    }
    val res = hasOnDeck
    if (!res) {
      totalMetric.foreach(_.close())
      totalMetric = None
      collectMetric.foreach(_.close())
      collectMetric = None
    }
    res
  }

  /**
   * Called first to initialize any state needed for a new batch to be created.
   */
  def initNewBatch(): Unit

  /**
   * Called to add a new batch to the final output batch. The batch passed in will
   * not be closed.  If it needs to be closed it is the responsibility of the child class
   * to do it.
   * @param batch the batch to add in.
   */
  def addBatchToConcat(batch: ColumnarBatch): Unit

  /**
   * Calculate (or estimate) the size of each column in a batch in bytes.
   * @return Array of column sizes in bytes
   */
  def getColumnSizes(batch: ColumnarBatch): Array[Long]

  /**
   * Called after all of the batches have been added in.
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
  def getColumnDataSize(cb: ColumnarBatch, index: Int, defaultSize: Long): Long = {
    cb.column(index) match {
      case g: GpuColumnVector =>
        val buff = g.getBase.getDeviceBufferFor(BufferType.DATA)
        if (buff == null) 0 else buff.getLength
      case h: RapidsHostColumnVector =>
        val buff = h.getBase.getHostBufferFor(BufferType.DATA)
        if (buff == null) 0 else buff.getLength
      case g: GpuCompressedColumnVector =>
        val columnMeta = g.getTableMeta.columnMetas(index)
        columnMeta.data().length()
      case _ =>
        defaultSize
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
  override def next(): ColumnarBatch = {
    // reset batch state
    batchInitialized = false
    batchRowLimit = 0

    try {
      var numRows: Long = 0 // to avoid overflows
      var numBytes: Long = 0
      var columnSizes: Array[Long] = schema.fields.indices.map(_ => 0L).toArray
      var stringColumnSizes: Array[Long] = stringFieldIndices.map(_ => 0L)

      // check if there is a batch "on deck" from a previous call to next()
      if (hasOnDeck) {
        val batch = popOnDeck()
        numRows += batch.numRows()
        columnSizes = getColumnSizes(batch)
        numBytes += columnSizes.sum
        stringColumnSizes = stringFieldIndices.map(i => getColumnDataSize(batch, i, columnSizes(i)))
            .zip(stringColumnSizes)
            .map(pair => pair._1 + pair._2)
        addBatch(batch)
      }

      try {

        // there is a hard limit of 2^31 rows
        while (numRows < Int.MaxValue && !hasOnDeck && iter.hasNext) {

          val cb = iter.next()
          val nextRows = cb.numRows()
          numInputBatches += 1

          // filter out empty batches
          if (nextRows > 0) {
            numInputRows += nextRows
            val nextColumnSizes = getColumnSizes(cb)
            val nextBytes = nextColumnSizes.sum

            // calculate the new sizes based on this input batch being added to the current
            // output batch
            val wouldBeRows = numRows + nextRows
            val wouldBeBytes = numBytes + nextBytes
            val wouldBeColumnSizes = columnSizes.zip(nextColumnSizes).map(pair => pair._1 + pair._2)

            // CuDF has a hard limit on the size of string data in a column so we check to make
            // sure that the string columns each use no more than Int.MaxValue bytes. This check is
            // overly cautious because the calculated size includes the offset bytes. When nested
            // types are supported, this logic will need to be enhanced to take offset and validity
            // buffers into account since they could account for a larger percentage of overall
            // memory usage.
            val wouldBeStringColumnSizes =
            stringFieldIndices.map(i => getColumnDataSize(cb, i, wouldBeColumnSizes(i)))
              .zip(stringColumnSizes)
              .map(pair => pair._1 + pair._2)

            if (wouldBeRows > Int.MaxValue) {
              if (goal == RequireSingleBatch) {
                throw new IllegalStateException("A single batch is required for this operation," +
                  s" but cuDF only supports ${Int.MaxValue} rows. At least $wouldBeRows are in" +
                  s" this partition. Please try increasing your partition count.")
              }
              saveOnDeck(cb)
            } else if (batchRowLimit > 0 && wouldBeRows > batchRowLimit) {
              saveOnDeck(cb)
            } else if (wouldBeBytes > goal.targetSizeBytes && numBytes > 0) {
              saveOnDeck(cb)
            } else if (wouldBeStringColumnSizes.exists(size => size > Int.MaxValue)) {
              if (goal == RequireSingleBatch) {
                throw new IllegalStateException("A single batch is required for this operation," +
                  s" but cuDF only supports ${Int.MaxValue} bytes in a single string column." +
                  s" At least ${wouldBeStringColumnSizes.max} are in a single column in this" +
                  s" partition. Please try increasing your partition count.")
              }
              saveOnDeck(cb)
            } else {
              addBatch(cb)
              numRows = wouldBeRows
              numBytes = wouldBeBytes
              columnSizes = wouldBeColumnSizes
              stringColumnSizes = wouldBeStringColumnSizes
            }
          } else {
            cb.close()
          }
        }

        // enforce single batch limit when appropriate
        if (goal == RequireSingleBatch && (hasOnDeck || iter.hasNext)) {
          throw new IllegalStateException("A single batch is required for this operation." +
            " Please try increasing your partition count.")
        }

        numOutputRows += numRows
        numOutputBatches += 1

      } finally {
        collectMetric.foreach(_.close())
        collectMetric = None
      }

      val concatRange = new NvtxWithMetrics(s"$opName concat", NvtxColor.CYAN, concatTime)
      val ret = try {
        concatAllAndPutOnGPU()
      } finally {
        concatRange.close()
      }
      ret
    } finally {
      cleanupConcatIsDone()
      totalMetric.foreach(_.close())
      totalMetric = None
    }
  }

  private def addBatch(batch: ColumnarBatch): Unit = {
    if (!batchInitialized) {
      initNewBatch()
      batchInitialized = true
    }
    addBatchToConcat(batch)
  }
}

class GpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    schema: StructType,
    goal: CoalesceGoal,
    maxDecompressBatchMemory: Long,
    numInputRows: SQLMetric,
    numInputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    collectTime: SQLMetric,
    concatTime: SQLMetric,
    totalTime: SQLMetric,
    peakDevMemory: SQLMetric,
    opName: String)
  extends AbstractGpuCoalesceIterator(iter,
    schema,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    totalTime,
    opName) with Arm {

  private val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
  private var maxDeviceMemory: Long = 0

  override def initNewBatch(): Unit = {
    batches.safeClose()
    batches.clear()
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit =
    batches.append(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY))

  override def getColumnSizes(cb: ColumnarBatch): Array[Long] = {
    if (!GpuCompressedColumnVector.isBatchCompressed(cb)) {
      GpuColumnVector.extractBases(cb).map(_.getDeviceMemorySize)
    } else {
      GpuCompressedColumnVector.getUncompressedColumnSizes(cb)
    }
  }

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
        withResource(codec.createBatchDecompressor(maxDecompressBatchMemory)) { decompressor =>
          compressedVecs.foreach { cv =>
            val bufferMeta = cv.getTableMeta.bufferMeta
            // don't currently support switching codecs when partitioning
            val buffer = cv.getBuffer.slice(0, cv.getBuffer.getLength)
            decompressor.addBufferToDecompress(buffer, bufferMeta)
          }
          withResource(decompressor.finish()) { outputBuffers =>
            outputBuffers.zipWithIndex.foreach { case (outputBuffer, outputIndex) =>
              val cv = compressedVecs(outputIndex)
              val batchIndex = compressedBatchIndices(outputIndex)
              val compressedBatch = wip(batchIndex)
              wip(batchIndex) = MetaUtils.getBatchFromMeta(outputBuffer, cv.getTableMeta)
              compressedBatch.close()
            }
          }
        }
      }
      wip.toArray
    }
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    val ret = ConcatAndConsumeAll.buildNonEmptyBatch(popAllDecompressed())
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

case class GpuCoalesceBatches(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode with GpuExec {

  private[this] val maxDecompressBatchMemory =
    new RapidsConf(child.conf).shuffleCompressionMaxBatchMemory

  import GpuMetricNames._
  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time"),
    PEAK_DEVICE_MEMORY -> SQLMetrics.createSizeMetric(sparkContext, DESCRIPTION_PEAK_DEVICE_MEMORY)
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val totalTime = longMetric(TOTAL_TIME)
    val peakDevMemory = longMetric("peakDevMemory")

    val batches = child.executeColumnar()
    batches.mapPartitions { iter =>
      if (child.schema.nonEmpty) {
        new GpuCoalesceIterator(iter, schema, goal, maxDecompressBatchMemory,
          numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime,
          concatTime, totalTime, peakDevMemory, "GpuCoalesceBatches")
      } else {
        val numRows = iter.map(_.numRows).sum
        val combinedCb = new ColumnarBatch(Array.empty, numRows)
        Iterator.single(combinedCb)
      }
    }
  }
}
