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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  test("test simple GPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test simple CPU OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    // Inject CPU OOM after skipping the first few CPU allocations. The skipCount ensures
    // the OOM is thrown at a point where our retry logic can handle it (during row conversion,
    // after builder state has been captured).
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.CPU.ordinal, 3)
    withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test GPU OOM split and retry with RequireSingleBatch still throws") {
    // When RequireSingleBatch is specified but we need to split due to GPU OOM,
    // we should still throw because we can't satisfy the single batch requirement
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("test GPU OOM split and retry produces multiple batches") {
    // When GPU OOM occurs during host-to-GPU transfer, we should split the batch
    // and produce multiple smaller batches
    val numRows = 10
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    // Use TargetSize goal (not RequireSingleBatch) to allow multiple output batches
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    // Force a split-and-retry OOM - this should trigger the split logic
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    // Collect all batches
    val batches = new ArrayBuffer[ColumnarBatch]()
    try {
      while (row2ColIter.hasNext) {
        batches += row2ColIter.next()
      }

      // We should have produced at least 2 batches due to the split
      assert(batches.length >= 2,
        s"Expected at least 2 batches after split, got ${batches.length}")

      // Total rows across all batches should equal input rows
      val totalRows = batches.map(_.numRows()).sum
      assertResult(numRows)(totalRows)
    } finally {
      batches.foreach(_.close())
    }
  }

  test("test GPU OOM split and retry with multiple splits") {
    // Test that we can handle multiple consecutive splits
    val numRows = 16 // Power of 2 for clean splits
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    // Force multiple split-and-retry OOMs - this should trigger multiple splits
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 3,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    val batches = new ArrayBuffer[ColumnarBatch]()
    try {
      while (row2ColIter.hasNext) {
        batches += row2ColIter.next()
      }

      // With 3 splits, we should have at least 4 batches (2^2 from first, then more splits)
      assert(batches.length >= 4,
        s"Expected at least 4 batches after 3 splits, got ${batches.length}")

      // Total rows across all batches should equal input rows
      val totalRows = batches.map(_.numRows()).sum
      assertResult(numRows)(totalRows)
    } finally {
      batches.foreach(_.close())
    }
  }

  test("test GPU OOM split with single row cannot split further") {
    // When we have only 1 row and GPU OOM occurs, we can't split further
    val rowIter: Iterator[InternalRow] = Iterator(InternalRow(1))
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    // Force a split-and-retry OOM with a single row - should throw since we can't split
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }
}
