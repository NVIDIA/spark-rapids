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
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan) extends UnaryExecNode with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

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
      case (t, n) =>
        throw new IllegalArgumentException(s"Converting to GPU for ${t} is not currently supported")
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    AutoCloseColumnBatchIterator.map[ColumnarBatch](child.executeColumnar(), b => {
      val rows = b.numRows()
      val batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, rows)
      try {
        for (i <- 0 until b.numCols()) {
          columnarCopy(b.column(i), batchBuilder.builder(i), schema.fields(i).nullable, rows)
        }
        batchBuilder.build(rows)
      } finally {
        batchBuilder.close()
      }
    })
  }
}
