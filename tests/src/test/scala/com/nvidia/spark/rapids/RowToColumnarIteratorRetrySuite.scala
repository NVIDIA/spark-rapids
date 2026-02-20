/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

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

  test("test empty schema preserves row count") {
    // Spark uses OneRowRelation (1 row, 0 cols) for constant expressions like pi()/e().
    // Ensure R2C preserves the row count even when there are no columns.
    val emptySchema = StructType(Nil)
    val rowIter: Iterator[InternalRow] = Iterator(InternalRow.empty)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, emptySchema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(emptySchema))
    withResource(row2ColIter.next()) { batch =>
      assertResult(1)(batch.numRows())
      assertResult(0)(batch.numCols())
    }
    assert(!row2ColIter.hasNext)
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
    // and produce multiple smaller batches.
    // Verification that split-retry happened:
    // 1. Without split, we'd get exactly 1 batch (batchSize is huge, all rows fit)
    // 2. With forceSplitAndRetryOOM, we get 2+ batches proving the split occurred
    // 3. Total rows match input, proving no data loss during split
    // 4. All values are correct, proving data integrity
    val numRows = 10
    val rowIter: Iterator[InternalRow] = (1 to numRows).map(InternalRow(_)).toIterator
    // Use TargetSize goal (not RequireSingleBatch) to allow multiple output batches
    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, goal, batchSize, new GpuRowToColumnConverter(schema))

    // Force a split-and-retry OOM - this should trigger the split logic
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    // Collect all batches and verify values
    val batches = new ArrayBuffer[ColumnarBatch]()
    val allValues = new ArrayBuffer[Int]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        // Extract values from GPU batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            allValues += hostCol.getInt(i)
          }
        }
      }

      // We should have produced at least 2 batches due to the split
      assert(batches.length >= 2,
        s"Expected at least 2 batches after split, got ${batches.length}")

      // Total rows across all batches should equal input rows
      assertResult(numRows)(allValues.length)

      // Verify all values are correct (1 to numRows in order)
      assertResult((1 to numRows).toSeq)(allValues.toSeq)
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
    val allValues = new ArrayBuffer[Int]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        // Extract values from GPU batch
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            allValues += hostCol.getInt(i)
          }
        }
      }

      // With 3 sequential splits (each split divides one batch into two), we should have
      // at least 4 batches in total: 1 → 2 → 3 → 4.
      assert(batches.length >= 4,
        s"Expected at least 4 batches after 3 sequential splits, got ${batches.length}")

      // Total rows across all batches should equal input rows
      assertResult(numRows)(allValues.length)

      // Verify all values are correct (1 to numRows in order)
      assertResult((1 to numRows).toSeq)(allValues.toSeq)
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

  test("test GPU OOM split and retry with nested types") {
    // Test that split and retry works correctly with various nested types:
    // string, array, struct, map, array of struct, array of array, struct of array
    val nestedSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("str", StringType),
      StructField("arr", ArrayType(IntegerType, containsNull = true)),
      StructField("nested_struct", StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", StringType)
      ))),
      StructField("map", MapType(StringType, IntegerType, valueContainsNull = true)),
      StructField("arr_of_struct", ArrayType(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )),
        containsNull = true
      )),
      StructField("arr_of_arr", ArrayType(
        ArrayType(IntegerType, containsNull = true),
        containsNull = true
      )),
      StructField("struct_of_arr", StructType(Seq(
        StructField("nums", ArrayType(IntegerType, containsNull = true)),
        StructField("strs", ArrayType(StringType, containsNull = true))
      )))
    ))
    val numRows = 10
    val rowIter: Iterator[InternalRow] = (1 to numRows).map { i =>
      // String column
      val str = UTF8String.fromString(s"test_string_$i")
      // Array column
      val arr = ArrayData.toArrayData(Array(i, i + 1, null, i + 2))
      // Struct column
      val nestedStruct = new GenericInternalRow(Array[Any](i * 10, UTF8String.fromString(s"s_$i")))
      // Map column
      val keys = ArrayData.toArrayData(Array(
        UTF8String.fromString(s"key_${i}_a"),
        UTF8String.fromString(s"key_${i}_b")
      ))
      val values = ArrayData.toArrayData(Array(i, null))
      val mapData = new ArrayBasedMapData(keys, values)
      // Array of struct column
      val struct1 = new GenericInternalRow(Array[Any](i, UTF8String.fromString(s"a_$i")))
      val struct2 = new GenericInternalRow(Array[Any](i + 1, UTF8String.fromString(s"b_$i")))
      val arrOfStruct = ArrayData.toArrayData(Array(struct1, null, struct2))
      // Array of array column
      val innerArr1 = ArrayData.toArrayData(Array(i, i + 1))
      val innerArr2 = ArrayData.toArrayData(Array(i + 2, null, i + 3))
      val arrOfArr = ArrayData.toArrayData(Array(innerArr1, null, innerArr2))
      // Struct of array column
      val numsArr = ArrayData.toArrayData(Array(i * 100, i * 100 + 1, null))
      val strsArr = ArrayData.toArrayData(Array(
        UTF8String.fromString(s"x_$i"),
        null,
        UTF8String.fromString(s"y_$i")
      ))
      val structOfArr = new GenericInternalRow(Array[Any](numsArr, strsArr))

      new GenericInternalRow(Array[Any](
        i, str, arr, nestedStruct, mapData, arrOfStruct, arrOfArr, structOfArr))
    }.toIterator

    val goal = TargetSize(batchSize)
    val row2ColIter = new RowToColumnarIterator(
      rowIter, nestedSchema, goal, batchSize, new GpuRowToColumnConverter(nestedSchema))

    // Force a split-and-retry OOM
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

    val batches = new ArrayBuffer[ColumnarBatch]()
    val idValues = new ArrayBuffer[Int]()
    val strValues = new ArrayBuffer[String]()
    try {
      while (row2ColIter.hasNext) {
        val batch = row2ColIter.next()
        batches += batch
        // Extract id column (IntegerType) values
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            idValues += hostCol.getInt(i)
          }
        }
        // Extract str column (StringType) values - verifies string slicing with offsets
        withResource(batch.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          for (i <- 0 until batch.numRows()) {
            strValues += hostCol.getBase.getJavaString(i)
          }
        }
      }

      // We should have produced at least 2 batches due to the split
      assert(batches.length >= 2,
        s"Expected at least 2 batches after split with nested types, got ${batches.length}")

      // Total rows across all batches should equal input rows
      assertResult(numRows)(idValues.length)
      assertResult(numRows)(strValues.length)

      // Verify id column values are correct (1 to numRows in order)
      assertResult((1 to numRows).toSeq)(idValues.toSeq)

      // Verify string column values are correct
      val expectedStrings = (1 to numRows).map(i => s"test_string_$i")
      assertResult(expectedStrings)(strValues.toSeq)
    } finally {
      batches.foreach(_.close())
    }
  }
}
