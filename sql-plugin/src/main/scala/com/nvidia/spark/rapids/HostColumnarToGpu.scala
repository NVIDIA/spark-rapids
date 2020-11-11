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

import ai.rapids.cudf.DType

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object HostColumnarToGpu {
  def columnarCopy(cv: ColumnVector, b: ai.rapids.cudf.HostColumnVector.ColumnBuilder,
      nullable: Boolean, rows: Int): Unit = {
    (GpuColumnVector.getRapidsType(cv.dataType()), nullable) match {
      case (DType.INT8 | DType.BOOL8, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getByte(i))
          }
        }
      case (DType.INT8 | DType.BOOL8, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getByte(i))
        }
      case (DType.INT16, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getShort(i))
          }
        }
      case (DType.INT16, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getShort(i))
        }
      case (DType.INT32 | DType.TIMESTAMP_DAYS, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getInt(i))
          }
        }
      case (DType.INT32 | DType.TIMESTAMP_DAYS, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getInt(i))
        }
      case (DType.INT64 | DType.TIMESTAMP_SECONDS | DType.TIMESTAMP_MILLISECONDS
            | DType.TIMESTAMP_MICROSECONDS | DType.TIMESTAMP_NANOSECONDS, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getLong(i))
          }
        }
      case (DType.INT64 | DType.TIMESTAMP_SECONDS | DType.TIMESTAMP_MILLISECONDS
            | DType.TIMESTAMP_MICROSECONDS | DType.TIMESTAMP_NANOSECONDS, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getLong(i))
        }
      case (DType.FLOAT32, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getFloat(i))
          }
        }
      case (DType.FLOAT32, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getFloat(i))
        }
      case (DType.FLOAT64, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getDouble(i))
          }
        }
      case (DType.FLOAT64, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getDouble(i))
        }
      case (DType.STRING, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.appendUTF8String(cv.getUTF8String(i).getBytes)
          }
        }
      case (DType.STRING, false) =>
        for (i <- 0 until rows) {
          b.appendUTF8String(cv.getUTF8String(i).getBytes)
        }
      case (t, n) =>
        throw new UnsupportedOperationException(s"Converting to GPU for ${t} is not currently " +
          s"supported")
    }
  }
}

/**
 * This iterator builds GPU batches from host batches. The host batches potentially use Spark's
 * UnsafeRow so it is not safe to cache these batches. Rows must be read and immediately written
 * to CuDF builders.
 */
class HostToGpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    schema: StructType,
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
    opName) {

  // RequireSingleBatch goal is intentionally not supported in this iterator
  assert(goal != RequireSingleBatch)

  var batchBuilder: GpuColumnVector.GpuColumnarBatchBuilder = _
  var totalRows = 0
  var maxDeviceMemory: Long = 0

  /**
   * Initialize the builders using an estimated row count based on the schema and the desired
   * batch size defined by [[RapidsConf.GPU_BATCH_SIZE_BYTES]].
   */
  override def initNewBatch(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    // when reading host batches it is essential to read the data immediately and pass to a
    // builder and we need to determine how many rows to allocate in the builder based on the
    // schema and desired batch size
    batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes,
      GpuBatchUtils.estimateGpuMemory(schema, 512), 512)
    batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, batchRowLimit, null)
    totalRows = 0
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit = {
    val rows = batch.numRows()
    for (i <- 0 until batch.numCols()) {
      HostColumnarToGpu.columnarCopy(batch.column(i), batchBuilder.builder(i),
        schema.fields(i).nullable, rows)
    }
    totalRows += rows
  }

  override def getColumnSizes(batch: ColumnarBatch): Array[Long] = {
    schema.fields.indices.map(GpuBatchUtils.estimateGpuMemory(schema, _, batchRowLimit)).toArray
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    // About to place data back on the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val ret = batchBuilder.build(totalRows)
    maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret)

    // refine the estimate for number of rows based on this batch
    batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes, maxDeviceMemory,
      ret.numRows())

    ret
  }

  override def cleanupConcatIsDone(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    totalRows = 0
    peakDevMemory.set(maxDeviceMemory)
  }

  private var onDeck: Option[ColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined
  override protected def saveOnDeck(batch: ColumnarBatch): Unit = onDeck = Some(batch)
  override protected def clearOnDeck(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
  override protected def popOnDeck(): ColumnarBatch = {
    val ret = onDeck.get
    onDeck = None
    ret
  }
}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode
  with GpuExec {
  import GpuMetricNames._

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_NUM_INPUT_BATCHES),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time"),
    "peakDevMemory" ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_PEAK_DEVICE_MEMORY)
  )

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def outputBatching: CoalesceGoal = goal

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
   * Returns an RDD[ColumnarBatch] that when mapped over will produce GPU-side column vectors
   * that are expected to be closed by its caller, not [[HostColumnarToGpu]].
   *
   * The expectation is that the only valid instantiation of this node is
   * as a child of a GPU exec node.
   *
   * @return an RDD of `ColumnarBatch`
   */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val totalTime = longMetric(TOTAL_TIME)
    val peakDevMemory = longMetric("peakDevMemory")

    // cache in a local to avoid serializing the plan
    val outputSchema = schema

    val batches = child.executeColumnar()
    batches.mapPartitions { iter =>
      new HostToGpuCoalesceIterator(iter, goal, outputSchema,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime,
        totalTime, peakDevMemory, "HostColumnarToGpu")
    }
  }
}
