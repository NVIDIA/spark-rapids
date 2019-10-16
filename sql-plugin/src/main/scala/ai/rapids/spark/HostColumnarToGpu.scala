/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import ai.rapids.cudf.DType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object HostColumnarToGpu {
  def columnarCopy(cv: ColumnVector, b: ai.rapids.cudf.ColumnVector.Builder,
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
      case (DType.INT32 | DType.DATE32, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getInt(i))
          }
        }
      case (DType.INT32 | DType.DATE32, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getInt(i))
        }
      case (DType.INT64 | DType.DATE64 | DType.TIMESTAMP, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getLong(i))
          }
        }
      case (DType.INT64 | DType.DATE64 | DType.TIMESTAMP, false) =>
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
        throw new UnsupportedOperationException(s"Converting to GPU for ${t} is not currently supported")
    }
  }
}

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

  var batchBuilder: GpuColumnVector.GpuColumnarBatchBuilder = null
  var totalRows = 0

  override def initNewBatch(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, goal.targetSize.toInt, null)
    totalRows = 0
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit = {
    val rows = batch.numRows()
    for (i <- 0 until batch.numCols()) {
      HostColumnarToGpu.columnarCopy(batch.column(i), batchBuilder.builder(i), schema.fields(i).nullable, rows)
    }
    totalRows += rows
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = batchBuilder.build(totalRows)

  override def cleanupConcatIsDone(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    totalRows = 0
  }
}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan, goal: CoalesceGoal) extends UnaryExecNode with GpuExec {
  import GpuMetricNames._

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "input rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input batches"),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time"),
    "peakDevMemory" ->SQLMetrics.createMetric(sparkContext, "peak dev memory")
  )

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
    * Returns an RDD[ColumnarBatch] that when mapped over will produce GPU-side column vectors
    * that are expected to be closed by its caller, not [[HostcolumnarToGpu]].
    *
    * The expectation is that the only valid instantiation of this node is
    * as a child of a GPU exec node.
    *
    * @return an RDD of [[ColumnarBatch]]
    */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

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
      new HostToGpuCoalesceIterator(iter, goal, schema,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime,
        totalTime, peakDevMemory, "HostColumnarToGpu")
    }
  }
}
