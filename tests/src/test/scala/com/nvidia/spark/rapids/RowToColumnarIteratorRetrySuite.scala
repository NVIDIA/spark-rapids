/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.{CpuSplitAndRetryOOM, GpuSplitAndRetryOOM, RmmSpark}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  private def newIterator(
      numRows: Int,
      goal: CoalesceSizeGoal,
      targetBatchSize: Long = batchSize): RowToColumnarIterator = {
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    new RowToColumnarIterator(
      rowIter, schema, goal, targetBatchSize, new GpuRowToColumnConverter(schema))
  }

  private def collectBatchRowCounts(iter: RowToColumnarIterator): Seq[Int] = {
    val counts = ArrayBuffer[Int]()
    while (iter.hasNext) {
      Arm.withResource(iter.next()) { batch =>
        counts += batch.numRows()
      }
    }
    counts.toSeq
  }

  test("test simple GPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test simple CPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 0)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test GPU split and retry OOM") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("test CPU split and retry OOM") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 0)
    assertThrows[CpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("gpu split and retry can continue across multiple batches") {
    val goal = TargetSize(32)
    val row2ColIter = newIterator(numRows = 32, goal = goal, targetBatchSize = 32)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val rowCounts = collectBatchRowCounts(row2ColIter)
    assert(rowCounts.length > 1, s"expected multiple batches but saw $rowCounts")
    assertResult(32)(rowCounts.sum)
  }

  test("require single batch fails if retry needs multiple output batches") {
    val row2ColIter = newIterator(numRows = 64, goal = RequireSingleBatch)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 26,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }
}
