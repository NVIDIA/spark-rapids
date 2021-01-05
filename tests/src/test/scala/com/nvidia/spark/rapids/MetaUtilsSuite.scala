/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.math.RoundingMode
import java.util

import ai.rapids.cudf.{ColumnView, ContiguousTable, DeviceMemoryBuffer, DType, HostColumnVector, Table}
import ai.rapids.cudf.HostColumnVector.{BasicType, StructData}
import com.nvidia.spark.rapids.format.{CodecType, ColumnMeta}
import org.scalatest.FunSuite

import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class MetaUtilsSuite extends FunSuite with Arm {
  private val contiguousTableSparkTypes: Array[DataType] = Array(
    IntegerType,
    StringType,
    DoubleType,
    DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5),
    ArrayType(StringType),
    StructType(Array(
      StructField("ints", IntegerType),
      StructField("strarray", ArrayType(StringType)))
    )
  )

  private def buildContiguousTable(): ContiguousTable = {
    def struct(vals: Object*): StructData = new StructData(vals:_*)
    val structType = new HostColumnVector.StructType(true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.ListType(true, new BasicType(true, DType.STRING)))
    withResource(new Table.TestBuilder()
        .column(5.asInstanceOf[Integer], null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
        .column(Array("1", "2", "three"), Array("4"), null, Array("five"))
        .column(structType,
          struct(1.asInstanceOf[Integer], util.Arrays.asList("a", "b", "c")),
          struct(2.asInstanceOf[Integer], util.Arrays.asList("1", "2", null)),
          struct(3.asInstanceOf[Integer], null),
          struct(null, util.Arrays.asList("xyz")))
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  def verifyColumnMeta(
      buffer: DeviceMemoryBuffer,
      col: ColumnView,
      columnMeta: ColumnMeta): Unit = {
    assertResult(col.getNullCount)(columnMeta.nullCount)
    assertResult(col.getRowCount)(columnMeta.rowCount)
    assertResult(col.getType.getTypeId.getNativeId)(columnMeta.dtypeId())
    assertResult(col.getType.getScale)(columnMeta.dtypeScale())
    val dataBuffer = col.getData
    if (dataBuffer != null) {
      assertResult(dataBuffer.getAddress - buffer.getAddress)(columnMeta.dataOffset())
      assertResult(dataBuffer.getLength)(columnMeta.dataLength())
    } else {
      assertResult(0)(columnMeta.dataOffset())
      assertResult(0)(columnMeta.dataLength())
    }
    val validBuffer = col.getValid
    if (validBuffer != null) {
      assertResult(validBuffer.getAddress - buffer.getAddress)(columnMeta.validityOffset())
    } else {
      assertResult(0)(columnMeta.validityOffset())
    }
    val offsetsBuffer = col.getOffsets
    if (offsetsBuffer != null) {
      assertResult(offsetsBuffer.getAddress - buffer.getAddress)(columnMeta.offsetsOffset())
    } else {
      assertResult(0)(columnMeta.offsetsOffset())
    }

    (0 until col.getNumChildren).foreach { i =>
      withResource(col.getChildColumnView(i)) { childView =>
        verifyColumnMeta(buffer, childView, columnMeta.children(i))
      }
    }
  }

  test("buildTableMeta") {
    withResource(buildContiguousTable()) { contigTable =>
      val table = contigTable.getTable
      val buffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(7, table, buffer)

      val bufferMeta = meta.bufferMeta
      assertResult(7)(bufferMeta.id)
      assertResult(buffer.getLength)(bufferMeta.size)
      assertResult(buffer.getLength)(bufferMeta.uncompressedSize)
      assertResult(0)(bufferMeta.codecBufferDescrsLength)
      assertResult(table.getRowCount)(meta.rowCount)

      assertResult(table.getNumberOfColumns)(meta.columnMetasLength)
      val columnMeta = new ColumnMeta
      (0 until table.getNumberOfColumns).foreach { i =>
        assert(meta.columnMetas(columnMeta, i) != null)
        verifyColumnMeta(buffer, table.getColumn(i), columnMeta)
      }
    }
  }

  test("buildTableMeta with codec") {
    withResource(buildContiguousTable()) { contigTable =>
      val tableId = 7
      val codecType = CodecType.COPY
      val compressedSize: Long = 123
      val table = contigTable.getTable
      val buffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(Some(tableId), table, buffer, codecType, compressedSize)

      val bufferMeta = meta.bufferMeta
      assertResult(tableId)(bufferMeta.id)
      assertResult(compressedSize)(bufferMeta.size)
      assertResult(table.getRowCount)(meta.rowCount)
      assertResult(1)(bufferMeta.codecBufferDescrsLength)
      val codecDescr = bufferMeta.codecBufferDescrs(0)
      assertResult(codecType)(codecDescr.codec)
      assertResult(compressedSize)(codecDescr.compressedSize)
      assertResult(0)(codecDescr.compressedOffset)
      assertResult(0)(codecDescr.uncompressedOffset)
      assertResult(buffer.getLength)(codecDescr.uncompressedSize)
    }
  }

  test("buildDegenerateTableMeta no columns") {
    val degenerateBatch = new ColumnarBatch(Array(), 127)
    val meta = MetaUtils.buildDegenerateTableMeta(degenerateBatch)
    assertResult(null)(meta.bufferMeta)
    assertResult(0)(meta.columnMetasLength)
    assertResult(127)(meta.rowCount)
  }

  test("buildDegenerateTableMeta no rows") {
    val schema = StructType.fromDDL("a INT, b STRING, c DOUBLE, d DECIMAL(15, 5)")
    withResource(GpuColumnVector.emptyBatch(schema)) { batch =>
      val meta = MetaUtils.buildDegenerateTableMeta(batch)
      assertResult(null)(meta.bufferMeta)
      assertResult(0)(meta.rowCount)
      assertResult(4)(meta.columnMetasLength)
      (0 until meta.columnMetasLength).foreach { i =>
        val columnMeta = meta.columnMetas(i)
        assertResult(0)(columnMeta.nullCount)
        assertResult(0)(columnMeta.rowCount)
        val expectedType = batch.column(i).asInstanceOf[GpuColumnVector].getBase.getType
        assertResult(expectedType.getTypeId.getNativeId)(columnMeta.dtypeId())
        assertResult(expectedType.getScale)(columnMeta.dtypeScale())
        assertResult(0)(columnMeta.dataOffset())
        assertResult(0)(columnMeta.dataLength())
        assertResult(0)(columnMeta.validityOffset())
        assertResult(0)(columnMeta.offsetsOffset())
      }
    }
  }

  test("buildDegenerateTableMeta no rows compressed table") {
    val schema = StructType.fromDDL("a INT, b STRING, c DOUBLE, d DECIMAL(15, 5)")
    withResource(GpuColumnVector.emptyBatch(schema)) { uncompressedBatch =>
      val uncompressedMeta = MetaUtils.buildDegenerateTableMeta(uncompressedBatch)
      withResource(DeviceMemoryBuffer.allocate(0)) { buffer =>
        val compressedTable = CompressedTable(0, uncompressedMeta, buffer)
        withResource(GpuCompressedColumnVector.from(compressedTable,
            GpuColumnVector.extractTypes(schema))) { batch =>
          val meta = MetaUtils.buildDegenerateTableMeta(batch)
          assertResult(null)(meta.bufferMeta)
          assertResult(0)(meta.rowCount)
          assertResult(4)(meta.columnMetasLength)
          (0 until meta.columnMetasLength).foreach { i =>
            val columnMeta = meta.columnMetas(i)
            assertResult(0)(columnMeta.nullCount)
            assertResult(0)(columnMeta.rowCount)
            val expectedType = uncompressedBatch.column(i).asInstanceOf[GpuColumnVector]
                .getBase.getType
            assertResult(expectedType.getTypeId.getNativeId)(columnMeta.dtypeId())
            assertResult(expectedType.getScale)(columnMeta.dtypeScale())
            assertResult(0)(columnMeta.dataOffset())
            assertResult(0)(columnMeta.dataLength())
            assertResult(0)(columnMeta.validityOffset())
            assertResult(0)(columnMeta.offsetsOffset())
          }
        }
      }
    }
  }

  test("getBatchFromMeta") {
    withResource(buildContiguousTable()) { contigTable =>
      val table = contigTable.getTable
      val origBuffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(10, table, origBuffer)
      val sparkTypes = Array[DataType](IntegerType, StringType, DoubleType,
        DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5))
      withResource(origBuffer.sliceWithCopy(0, origBuffer.getLength)) { buffer =>
        withResource(MetaUtils.getBatchFromMeta(buffer, meta,
          contiguousTableSparkTypes)) { batch =>
          assertResult(table.getRowCount)(batch.numRows)
          assertResult(table.getNumberOfColumns)(batch.numCols)
          (0 until table.getNumberOfColumns).foreach { i =>
            val batchColumn = batch.column(i)
            assert(batchColumn.isInstanceOf[GpuColumnVectorFromBuffer])
            TestUtils.compareColumns(table.getColumn(i),
              batchColumn.asInstanceOf[GpuColumnVector].getBase)
          }
        }
      }
    }
  }
}
