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

import com.nvidia.spark.rapids.jni._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class RowToColumnarIteratorRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val batchSize = 1 * 1024 * 1024 * 1024

  test("test simple OOM retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(10)(batch.numRows())
    }
  }

  test("test simple OOM split and retry") {
    val rowIter: Iterator[InternalRow] = (1 to 10).map(InternalRow(_)).toIterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, RequireSingleBatch, batchSize, new GpuRowToColumnConverter(schema))
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      row2ColIter.next()
    }
  }

  test("host OOM retries buffered row") {
    val unsafeProj = UnsafeProjection.create(schema)
    val failingRows = Seq(
      new FailingRow(InternalRow(1), 0, unsafeProj),
      new FailingRow(InternalRow(2), 1, unsafeProj),
      new FailingRow(InternalRow(3), 0, unsafeProj))
    val rowIter: Iterator[InternalRow] = failingRows.iterator
    val row2ColIter = new RowToColumnarIterator(
      rowIter, schema, TargetSize(128), batchSize, new GpuRowToColumnConverter(schema))
    Arm.withResource(row2ColIter.next()) { batch =>
      assertResult(3)(batch.numRows())
    }
    assert(!row2ColIter.hasNext)
  }

  private class FailingRow(
      row: InternalRow,
      failTimes: Int,
      projection: UnsafeProjection)
    extends InternalRow {
    private val delegate = projection.apply(row).copy()
    private var remaining = failTimes

    override def numFields: Int = delegate.numFields

    override def setNullAt(i: Int): Unit =
      throw new UnsupportedOperationException("Not supported in test row")

    override def update(i: Int, value: Any): Unit =
      throw new UnsupportedOperationException("Not supported in test row")

    override def copy(): InternalRow = delegate.copy()

    override def isNullAt(i: Int): Boolean = delegate.isNullAt(i)

    override def getBoolean(i: Int): Boolean = delegate.getBoolean(i)

    override def getByte(i: Int): Byte = delegate.getByte(i)

    override def getShort(i: Int): Short = delegate.getShort(i)

    override def getInt(i: Int): Int = {
      if (remaining > 0) {
        remaining -= 1
        throw new CpuRetryOOM("Injected host OOM for testing")
      }
      delegate.getInt(i)
    }

    override def getLong(i: Int): Long = delegate.getLong(i)

    override def getFloat(i: Int): Float = delegate.getFloat(i)

    override def getDouble(i: Int): Double = delegate.getDouble(i)

    override def getDecimal(i: Int, precision: Int, scale: Int): Decimal =
      delegate.getDecimal(i, precision, scale)

    override def getUTF8String(i: Int): UTF8String = delegate.getUTF8String(i)

    override def getBinary(i: Int): Array[Byte] = delegate.getBinary(i)

    override def getInterval(i: Int): CalendarInterval = delegate.getInterval(i)

    override def getStruct(i: Int, numFields: Int): InternalRow =
      delegate.getStruct(i, numFields)

    override def getArray(i: Int): ArrayData = delegate.getArray(i)

    override def getMap(i: Int): MapData = delegate.getMap(i)

    override def get(i: Int, dataType: DataType): AnyRef = delegate.get(i, dataType)
  }
}
