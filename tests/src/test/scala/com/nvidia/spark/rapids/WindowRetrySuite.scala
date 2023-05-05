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

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.{RowFrame, SortOrder, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.rapids.GpuCount
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, LongType}

class WindowRetrySuite
    extends RmmSparkRetrySuiteBase
        with MockitoSugar {
  private def buildInputBatch() = {
    val windowTable = new Table.TestBuilder()
      .column(1.asInstanceOf[java.lang.Integer], 1, 1, 1)
      .column(5L, null.asInstanceOf[java.lang.Long], 3L, 3L)
      .build()
    withResource(windowTable) { tbl =>
      GpuColumnVector.from(tbl, Seq(IntegerType, LongType).toArray[DataType])
    }
  }

  def setupWindowIterator(
      frame: GpuSpecifiedWindowFrame,
      orderSpec: Seq[SortOrder] = Seq.empty): GpuWindowIterator = {
    val spec = GpuWindowSpecDefinition(Seq.empty, orderSpec, frame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral.create(1, IntegerType))), spec)
    new GpuWindowIterator(
      input = Seq(buildInputBatch()).iterator,
      boundWindowOps = Seq(GpuAlias(count, "count")()),
      boundPartitionSpec = Seq.empty,
      boundOrderSpec = orderSpec,
      outputTypes = Array(DataTypes.LongType),
      numOutputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      opTime = NoopMetric
    )
  }

  test("row based window handles RetryOOM") {
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(UnboundedFollowing))
    val it = setupWindowIterator(frame)
    // pre-load a spillable batch before injecting the OOM
    val spillableBatch = spy(it.getNext())
    it.onDeck = Some(spillableBatch)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1)
    val batch = it.next()
    assertResult(4)(batch.numRows())
    withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { col =>
      (0 until batch.numRows().toInt).foreach { row =>
        assertResult(4)(col.getLong(row))
      }
    }
    verify(spillableBatch, times(2)).getColumnarBatch()
    verify(spillableBatch, times(1)).close()
  }

  /*
  test("optimized-row based window handles RetryOOM") {
    val inputBatch = buildInputBatch()
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val (groupAggs, outputColumns) = setupCountAgg(frame)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1)
    groupAggs.doAggsAndClose(
      false,
      Seq.empty[SortOrder],
      Array.empty,
      Array.empty,
      inputBatch.getColumnarBatch(),
      outputColumns)
    withResource(outputColumns) { _ =>
      var rowsLeftToCheck = 4
      withResource(outputColumns(2).copyToHost()) { hostCol =>
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          assertResult(row + 1)(hostCol.getLong(row))
          rowsLeftToCheck -= 1
        }
      }
      assertResult(0)(rowsLeftToCheck)
    }
    verify(inputBatch, times(2)).getColumnarBatch()
    verify(inputBatch, times(1)).close()
  }

  test("ranged based window handles RetryOOM") {
    val inputBatch = buildInputBatch()
    val frame = GpuSpecifiedWindowFrame(
      RangeFrame,
      GpuLiteral.create(-1, IntegerType),
      GpuSpecialFrameBoundary(CurrentRow))
    val child = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "test")
    val orderSpec = SortOrder(child, Ascending) :: Nil
    val (groupAggs, outputColumns) = setupCountAgg(frame, orderSpec = orderSpec)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1)
    groupAggs.doAggsAndClose(
      false,
      orderSpec,
      Array(0),
      Array.empty,
      inputBatch.getColumnarBatch(),
      outputColumns)
    withResource(outputColumns) { _ =>
      var rowsLeftToCheck = 4
      withResource(outputColumns(2).copyToHost()) { hostCol =>
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          assertResult(4)(hostCol.getLong(row))
          rowsLeftToCheck -= 1
        }
      }
      assertResult(0)(rowsLeftToCheck)
    }
    verify(inputBatch, times(2)).getColumnarBatch()
    verify(inputBatch, times(1)).close()
  }

  test("SplitAndRetryOOM is not handled in doAggs") {
    val inputBatch = buildInputBatch()

    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val (groupAggs, outputColumns) = setupCountAgg(frame)
    // simulate a successful window operation
    val theMock = mock[ColumnVector]
    outputColumns(0) = theMock
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1)
    assertThrows[SplitAndRetryOOM] {
      groupAggs.doAggsAndClose(
        false,
        Seq.empty[SortOrder],
        Array.empty,
        Array.empty,
        inputBatch.getColumnarBatch(),
        outputColumns)
    }
    // when we throw we must have closed any columns in `outputColumns` that are not null
    // and we would have marked them null
    assertResult(null)(outputColumns(0))
    verify(theMock, times(1)).close()
    verify(inputBatch, times(1)).getColumnarBatch()
    verify(inputBatch, times(1)).close()
  }

  test("row based group by window handles RetryOOM") {
    val inputBatch = buildInputBatch()
    val frame = GpuSpecifiedWindowFrame(
      RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(CurrentRow))
    val (groupAggs, outputColumns) = setupCountAgg(frame)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1)
    groupAggs.doAggsAndClose(
      false,
      Seq.empty[SortOrder],
      Array.empty,
      Array(1),
      inputBatch.getColumnarBatch(),
      outputColumns)
    withResource(outputColumns) { _ =>
      var rowsLeftToCheck = 4
      withResource(outputColumns(2).copyToHost()) { hostCol =>
        (0 until hostCol.getRowCount.toInt).foreach { row =>
          if (row == 0) { // 5
            assertResult(1)(hostCol.getLong(row))
            rowsLeftToCheck -= 1
          } else if (row == 1) { // null
            assertResult(1)(hostCol.getLong(row))
            rowsLeftToCheck -= 1
          } else if (row == 2) { // 3
            assertResult(1)(hostCol.getLong(row))
            rowsLeftToCheck -= 1
          } else if (row == 3) { // 3
            assertResult(2)(hostCol.getLong(row))
            rowsLeftToCheck -= 1
          }
        }
      }
      assertResult(0)(rowsLeftToCheck)
    }
    verify(inputBatch, times(2)).getColumnarBatch()
    verify(inputBatch, times(1)).close()
  }
   */
}
