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

package ai.rapids.spark

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, Table}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

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
  def getSingleBatchWithVerification(batches: Iterator[ColumnarBatch], format: Seq[Attribute]): ColumnarBatch = {
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
    case (PreferSingleBatch, _) => a
    case (_, PreferSingleBatch) => b
    case (TargetSize(aSize, _), TargetSize(bSize, _)) if aSize > bSize => a
    case _ => b
  }
}

sealed abstract class CoalesceGoal extends Serializable {

  @deprecated(message = "This will be removed once all operators respect byte limits on batch sizes")
  val targetSizeRows: Long

  val targetSizeBytes: Long

  def whenTargetExceeded(actualSize: Long): Unit = {}
}

object RequireSingleBatch extends CoalesceGoal {

  @deprecated(message = "This will be removed once all operators respect byte limits on batch sizes")
  override val targetSizeRows: Long = Integer.MAX_VALUE

  override val targetSizeBytes: Long = Integer.MAX_VALUE

  override def whenTargetExceeded(actualSize: Long): Unit = {
    throw new IllegalStateException("A single batch is required for this operation." +
      " Please try increasing your partition count.")
  }
}

object PreferSingleBatch extends CoalesceGoal {

  @deprecated(message = "This will be removed once all operators respect byte limits on batch sizes")
  override val targetSizeRows: Long = Integer.MAX_VALUE

  override val targetSizeBytes: Long = Integer.MAX_VALUE
}

case class TargetSize(override val targetSizeRows: Long, override val targetSizeBytes: Long) extends CoalesceGoal {
  assert(targetSizeRows <= Integer.MAX_VALUE)
}

class RemoveEmptyBatchIterator(iter: Iterator[ColumnarBatch],
    numFiltered: SQLMetric) extends Iterator[ColumnarBatch] {
  private var onDeck: Option[ColumnarBatch] = None

  TaskContext.get().addTaskCompletionListener[Unit](_ => onDeck.foreach(_.close()))

  override def hasNext: Boolean = {
    while (onDeck.isEmpty && iter.hasNext) {
      val cb = iter.next()
      val rows = cb.numRows()
      if (rows > 0) {
        onDeck = Some(cb)
      } else {
        numFiltered += 1
        cb.close()
      }
    }
    onDeck.isDefined
  }

  override def next(): ColumnarBatch =
    if (onDeck.isDefined || hasNext) {
      val ret = onDeck.get
      onDeck = None
      ret
    } else {
      throw new NoSuchElementException()
    }
}

abstract class AbstractGpuCoalesceIterator(origIter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    numInputRows: SQLMetric,
    numInputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    collectTime: SQLMetric,
    concatTime: SQLMetric,
    totalTime: SQLMetric,
    peakDevMemory: SQLMetric,
    opName: String) extends Iterator[ColumnarBatch] {
  private val iter = new RemoveEmptyBatchIterator(origIter, numInputBatches)
  private var onDeck: Option[ColumnarBatch] = None
  private var maxDeviceMemory: Long = 0

  TaskContext.get().addTaskCompletionListener[Unit](_ => onDeck.foreach(_.close()))

  override def hasNext: Boolean = onDeck.isDefined || iter.hasNext

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
   * Called after all of the batches have been added in.
   * @return the concated batches on the GPU.
   */
  def concatAllAndPutOnGPU(): ColumnarBatch

  /**
   * Called to cleanup any state when a batch is done (even if there was a failure)
   */
  def cleanupConcatIsDone(): Unit

  override def next(): ColumnarBatch = {
    val total = new MetricRange(totalTime)
    try {
      initNewBatch()
      var numRows: Long = 0 // to avoid overflows
      if (onDeck.isDefined) {
        val cb = onDeck.get
        val rows = cb.numRows()
        if (rows > goal.targetSizeRows) {
          goal.whenTargetExceeded(rows)
        }
        addBatchToConcat(cb)
        onDeck = None
        numRows += rows
      }

      val collect = new MetricRange(collectTime)
      try {
        while (numRows < goal.targetSizeRows && onDeck.isEmpty && iter.hasNext) {
          val cb = iter.next()
          val nextRows = cb.numRows()
          numInputBatches += 1
          numInputRows += nextRows
          val wouldBeRows = nextRows + numRows
          if (wouldBeRows > goal.targetSizeRows) {
            goal.whenTargetExceeded(wouldBeRows)
            // If numRows == 0, this is the first batch so we really should just do it.
            if (numRows == 0) {
              addBatchToConcat(cb)
              numRows = wouldBeRows
            } else {
              onDeck = Some(cb)
            }
          } else {
            addBatchToConcat(cb)
            numRows = wouldBeRows
          }
        }
        numOutputRows += numRows
        numOutputBatches += 1
      } finally {
        collect.close()
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
      total.close()
    }
  }
}

class GpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
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
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    totalTime,
    peakDevMemory,
    opName) {

  private var batches: ArrayBuffer[ColumnarBatch] = ArrayBuffer.empty
  private var maxDeviceMemory: Long = 0

  override def initNewBatch(): Unit =
    batches = ArrayBuffer[ColumnarBatch]()

  override def addBatchToConcat(batch: ColumnarBatch): Unit =
    batches += batch

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    val tmp = batches.toArray
    // Clear the buffer so we don't close it again (buildNonEmptyBatch closed it for us).
    batches = ArrayBuffer.empty
    val ret = ConcatAndConsumeAll.buildNonEmptyBatch(tmp)
    // sum of current batches and concatenating batches. Approximately sizeof(ret * 2).
    maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret) * 2
    ret
  }

  override def cleanupConcatIsDone(): Unit = {
    peakDevMemory.set(maxDeviceMemory)
    batches.foreach(_.close())
  }
}

case class GpuCoalesceBatches(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode with GpuExec {
  import GpuMetricNames._

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "input rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input batches"),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time"),
    "peakDevMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak device memory")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def output: Seq[Attribute] = child.output

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val totalTime = longMetric(TOTAL_TIME)
    val peakDevMemory = longMetric("peakDevMemory")

    val batches = child.executeColumnar()
    batches.mapPartitions { iter =>
      if (child.schema.nonEmpty) {
        new GpuCoalesceIterator(iter, goal,
          numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime, totalTime,
          peakDevMemory, "GpuCoalesceBatches")
      } else {
        val numRows = iter.map(_.numRows).sum
        val combinedCb = new ColumnarBatch(Array.empty, numRows)
        Iterator.single(combinedCb)
      }
    }
  }
}
