/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.io.File
import java.nio.charset.StandardCharsets

import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, Table, TableWriter}
import com.nvidia.spark.rapids.shims.spark310.{ParquetBufferConsumer, ParquetCachedBatchSerializer}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.types.{ByteType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * Tests for writing Parquet files with the GPU.
 */
class ParquetWriterSuite extends SparkQueryCompareTestSuite {
  test("file metadata") {
    val tempFile = File.createTempFile("stats", ".parquet")
    try {
      withGpuSparkSession(spark => {
        val df = mixedDfWithNulls(spark)
        df.write.mode("overwrite").parquet(tempFile.getAbsolutePath)

        val footer = ParquetFileReader.readFooters(spark.sparkContext.hadoopConfiguration,
          new Path(tempFile.getAbsolutePath)).get(0)

        val parquetMeta = footer.getParquetMetadata
        val fileMeta = footer.getParquetMetadata.getFileMetaData
        val extra = fileMeta.getKeyValueMetaData
        assert(extra.containsKey("org.apache.spark.version"))
        assert(extra.containsKey("org.apache.spark.sql.parquet.row.metadata"))

        val blocks = parquetMeta.getBlocks
        assertResult(1) { blocks.size }
        val block = blocks.get(0)
        assertResult(11) { block.getRowCount }
        val cols = block.getColumns
        assertResult(4) { cols.size }

        assertResult(3) { cols.get(0).getStatistics.getNumNulls }
        assertResult(-700L) { cols.get(0).getStatistics.genericGetMin }
        assertResult(1200L) { cols.get(0).getStatistics.genericGetMax }

        assertResult(4) { cols.get(1).getStatistics.getNumNulls }
        assertResult(1.0) { cols.get(1).getStatistics.genericGetMin }
        assertResult(9.0) { cols.get(1).getStatistics.genericGetMax }

        assertResult(4) { cols.get(2).getStatistics.getNumNulls }
        assertResult(90) { cols.get(2).getStatistics.genericGetMin }
        assertResult(99) { cols.get(2).getStatistics.genericGetMax }

        assertResult(1) { cols.get(3).getStatistics.getNumNulls }
        assertResult("A") {
          new String(cols.get(3).getStatistics.getMinBytes, StandardCharsets.UTF_8)
        }
        assertResult("\ud720\ud721") {
          new String(cols.get(3).getStatistics.getMaxBytes, StandardCharsets.UTF_8)
        }
      })
    } finally {
      tempFile.delete()
    }
  }

  testExpectedGpuException(
    "Old dates in EXCEPTION mode",
    classOf[SparkException],
    oldDatesDf,
    new SparkConf().set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")) {
    val tempFile = File.createTempFile("oldDates", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  testExpectedGpuException(
    "Old timestamps millis in EXCEPTION mode",
    classOf[SparkException],
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  testExpectedGpuException(
    "Old timestamps in EXCEPTION mode",
    classOf[SparkException],
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  val ROWS = 3 * 1024 * 1024

  private def getCudfAndGpuVectors(): (ColumnVector, GpuColumnVector)= {
    val spyCol = spy(ColumnVector.fromBytes(1))
    when(spyCol.getRowCount).thenReturn(ROWS)
    val mockDtype = mock(classOf[DType])
    when(mockDtype.getSizeInBytes).thenReturn(1024)
    val mockDataType = mock(classOf[DataType])
    val spyGpuCol = spy(GpuColumnVector.from(spyCol, ByteType))
    when(spyCol.getType()).thenReturn(mockDtype)
    when(spyGpuCol.dataType()).thenReturn(mockDataType)
    when(mockDataType.defaultSize).thenReturn(1024)

    (spyCol, spyGpuCol)
  }

  private def whenSplitCalled(cb: ColumnarBatch): Unit = {
    val rows = cb.numRows()
    val eachRowSize = cb.numCols() * 1024
    val _2GB = 2L * 1024 * 1024 * 1024
    val rowsAllowedInABatch = _2GB / eachRowSize
    val spillOver = cb.numRows() % rowsAllowedInABatch
    val splitRange = scala.Range(rowsAllowedInABatch.toInt, rows, rowsAllowedInABatch.toInt)
    scala.Range(0, cb.numCols()).indices.foreach { i =>
      val spyCol = cb.column(i).asInstanceOf[GpuColumnVector].getBase
      val splitCols0 = scala.Range(0, splitRange.length).map { _ =>
        val spySplitCol = spy(ColumnVector.fromBytes(4, 5, 6))
        when(spySplitCol.getRowCount()).thenReturn(rowsAllowedInABatch)
        spySplitCol
      }
      val splitCols = if (spillOver > 0) {
        val splitCol = spy(ColumnVector.fromBytes(3))
        when(splitCol.getRowCount()).thenReturn(spillOver)
        splitCols0 :+ splitCol
      } else {
        splitCols0
      }
      when(spyCol.split(any())).thenReturn(splitCols.toArray)
    }
  }

  private def testCompressColBatch(cudfCols: Array[ColumnVector],
                     gpuCols: Array[org.apache.spark.sql.vectorized.ColumnVector]): Unit = {
    if (!withCpuSparkSession(s => s.version < "3.1.0")) {
      val ser = new ParquetCachedBatchSerializer

      // mock static method for Table
      val theTableMock = mockStatic(classOf[Table], (_: InvocationOnMock) =>
        new TableWriter {
          override def write(table: Table): Unit = {
            val tableSize = table.getColumn(0).getType.getSizeInBytes * table.getRowCount
            if (tableSize > Int.MaxValue) {
              fail("parquet table exceeds the 2gb limit")
            }
          }

          override def close(): Unit = {
            // noop
          }
        })

      withResource(cudfCols) { _=>
        val cb = new ColumnarBatch(gpuCols, ROWS)
        whenSplitCalled(cb)
        ser.compressColumnarBatchWithParquet(cb)
        theTableMock.close()
      }
    }
  }

  test("convert large columnar batch to cachedbatch on CPU single col table") {
    val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors()
    testCompressColBatch(Array(spyCol0), Array(spyGpuCol0))
    verify(spyCol0).split(2 * 1024 * 1024)
  }

  test("convert large columnar batch to cachedbatch on CPU multi-col table") {
    val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors()
    val (spyCol1, spyGpuCol1) = getCudfAndGpuVectors()
    val (spyCol2, spyGpuCol2) = getCudfAndGpuVectors()
    testCompressColBatch(Array(spyCol0, spyCol1, spyCol2), Array(spyGpuCol0, spyGpuCol1, spyGpuCol2))
    val splitAt = Seq(699050, 1398100, 2097150, 2796200)
    verify(spyCol0).split(splitAt: _*)
    verify(spyCol1).split(splitAt: _*)
    verify(spyCol2).split(splitAt: _*)
  }
}
