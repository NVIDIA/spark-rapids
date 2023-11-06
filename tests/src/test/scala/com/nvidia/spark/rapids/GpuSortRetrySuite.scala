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
import com.nvidia.spark.rapids.jni.{RetryOOM, RmmSpark, SplitAndRetryOOM}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSortRetrySuite extends RmmSparkRetrySuiteBase with MockitoSugar {

  private val ref = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")
  private val sortOrder = SortOrder(ref, Ascending)
  private val attrs = AttributeReference(ref.name, ref.dataType, ref.nullable)()
  private val gpuSorter = new GpuSorter(Seq(sortOrder), Array(attrs))
  private val NUM_ROWS = 100

  private def buildBatch: ColumnarBatch = {
    val ints = (NUM_ROWS / 2 until NUM_ROWS) ++ (0 until NUM_ROWS / 2)
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), NUM_ROWS)
  }

  test("GPU out-of-core sort without OOM failures") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch),
      gpuSorter,
      targetSize = 1024)
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort with retry when first-pass-sort RetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch),
      gpuSorter,
      targetSize = 1024,
      firstPassSortExp = new RetryOOM())
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort with retry when first-pass-sort SplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch),
      gpuSorter,
      targetSize = 1024,
      firstPassSortExp = new SplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort with retry when first-pass-split RetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch),
      gpuSorter,
      targetSize = 1024,
      firstPassSplitExp = new RetryOOM())
    withResource(outCoreIter) { _ =>
      withResource(outCoreIter.next()) { cb =>
        // only one batch
        assertResult(NUM_ROWS)(cb.numRows())
        assertResult(true)(GpuColumnVector.isTaggedAsFinalBatch(cb))
      }
    }
  }

  test("GPU out-of-core sort throws when first-pass-split SplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch),
      gpuSorter,
      targetSize = 1024,
      firstPassSplitExp = new SplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[SplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  test("GPU out-of-core sort with retry when merge-sort-split RetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch, buildBatch),
      gpuSorter,
      targetSize = 400,
      mergeSortExp = new RetryOOM())
    withResource(outCoreIter) { _ =>
      var numRows = 0
      while(outCoreIter.hasNext) {
        withResource(outCoreIter.next()) { cb =>
          numRows += cb.numRows()
        }
      }
      assertResult(NUM_ROWS * 2)(numRows)
    }
  }

  test("GPU out-of-core sort throws when merge-sort-split SplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch, buildBatch),
      gpuSorter,
      targetSize = 400,
      mergeSortExp = new SplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[SplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  test("GPU out-of-core sort with retry when concat-output RetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch, buildBatch),
      gpuSorter,
      targetSize = 400,
      concatOutExp = new RetryOOM())
    withResource(outCoreIter) { _ =>
      var numRows = 0
      while (outCoreIter.hasNext) {
        withResource(outCoreIter.next()) { cb =>
          numRows += cb.numRows()
        }
      }
      assertResult(NUM_ROWS * 2)(numRows)
    }
  }

  test("GPU out-of-core sort throws when concat-output SplitAndRetryOOM") {
    val outCoreIter = new GpuOutOfCoreSortIteratorThatThrows(
      Iterator(buildBatch, buildBatch),
      gpuSorter,
      targetSize = 400,
      concatOutExp = new SplitAndRetryOOM())
    withResource(outCoreIter) { _ =>
      assertThrows[SplitAndRetryOOM] {
        outCoreIter.next()
      }
    }
  }

  private class GpuOutOfCoreSortIteratorThatThrows(
      iter: Iterator[ColumnarBatch],
      sorter: GpuSorter,
      targetSize: Long,
      firstPassSortExp: Throwable = null,
      firstPassSplitExp: Throwable = null,
      mergeSortExp: Throwable = null,
      concatOutExp: Throwable = null,
      expMaxCount: Int = 1)
    extends GpuOutOfCoreSortIterator(iter, sorter, targetSize,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric){

    private var expCnt = expMaxCount

    override def onFirstPassSort(): Unit = if (firstPassSortExp != null && expCnt > 0) {
      expCnt -= 1
      throw firstPassSortExp
    }

    override def onFirstPassSplit(): Unit = if (firstPassSplitExp != null && expCnt > 0) {
      expCnt -= 1
      throw firstPassSplitExp
    }

    override def onMergeSortSplit(): Unit = if (mergeSortExp != null && expCnt > 0) {
      expCnt -= 1
      throw mergeSortExp
    }

    override def onConcatOutput(): Unit = if (concatOutExp != null && expCnt > 0) {
      expCnt -= 1
      throw concatOutExp
    }
  }

  test("GPU each batch sort with RetryOOM") {
    val eachBatchIter = new GpuSortEachBatchIterator(
      Iterator(buildBatch, buildBatch),
      gpuSorter,
      singleBatch = false)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2)
    while (eachBatchIter.hasNext) {
      var pos = 0
      var curValue = 0
      withResource(eachBatchIter.next()) { cb =>
        assertResult(NUM_ROWS)(cb.numRows())
        withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
          while (pos < hCol.getRowCount.toInt) {
            assertResult(curValue)(hCol.getInt(pos))
            pos += 1
            curValue += 1
          }
        }
      }
    }
  }

  test("GPU each batch sort throws SplitAndRetryOOM") {
    val inputIter = Iterator(buildBatch, buildBatch)
    val eachBatchIter = new GpuSortEachBatchIterator(
      inputIter,
      gpuSorter,
      singleBatch = false)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
    assertThrows[SplitAndRetryOOM] {
      eachBatchIter.next()
    }
    inputIter.foreach(_.close())
  }
}
