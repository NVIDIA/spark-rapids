/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class LimitRetrySuite extends RmmSparkRetrySuiteBase {

  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val attrs = AttributeReference(ref.name, ref.dataType, ref.nullable)()
  private val gpuSorter = new GpuSorter(Seq(SortOrder(ref, Ascending)), Array(attrs))
  private val NUM_ROWS = 100

  private def buildBatch1: ColumnarBatch = {
    val ints = 0 until NUM_ROWS by 2
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), ints.length)
  }

  private def buildBatch2: ColumnarBatch = {
    val ints = 1 until NUM_ROWS by 2
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), ints.length)
  }

  test("GPU topn with split and retry OOM") {
    val limit = 20
    Seq(0, 5).foreach { offset =>
      val topNIter = GpuTopN(limit, gpuSorter, Seq(buildBatch1, buildBatch2).toIterator,
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
        offset)
      val numRows = limit - offset
      var curValue = offset
      var pos = 0
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
      assert(topNIter.hasNext)
      withResource(topNIter.next()) { scb =>
        withResource(scb.getColumnarBatch()) { cb =>
          withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
            while (pos < hCol.getRowCount.toInt) {
              assertResult(curValue)(hCol.getInt(pos))
              pos += 1
              curValue += 1
            }
          }
        }
      }
      assertResult(numRows)(pos)
      // only one batch
      assert(!topNIter.hasNext)
    }
  }
}
