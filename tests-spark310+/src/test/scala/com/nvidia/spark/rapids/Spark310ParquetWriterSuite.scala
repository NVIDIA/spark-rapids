/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, DType, Table, TableWriter}
import com.nvidia.spark.rapids.shims.spark311.{ParquetCachedBatchSerializer, ParquetOutputFileFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


/**
 * Tests for writing Parquet files with the GPU.
 */
class Spark310ParquetWriterSuite extends SparkQueryCompareTestSuite {

  test("convert large columnar batch to cachedbatch on single col table") {
    if (!withCpuSparkSession(s => s.version < "3.1.0")) {
      val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors()
      testCompressColBatch(Array(spyCol0), Array(spyGpuCol0))
      verify(spyCol0).split(2086912)
    }
  }

  test("convert large columnar batch to cachedbatch on multi-col table") {
    if (!withCpuSparkSession(s => s.version < "3.1.0")) {
      val (spyCol0, spyGpuCol0) = getCudfAndGpuVectors()
      val (spyCol1, spyGpuCol1) = getCudfAndGpuVectors()
      val (spyCol2, spyGpuCol2) = getCudfAndGpuVectors()
      testCompressColBatch(Array(spyCol0, spyCol1, spyCol2),
        Array(spyGpuCol0, spyGpuCol1, spyGpuCol2))
      val splitAt = Seq(695637, 1391274, 2086911, 2782548)
      verify(spyCol0).split(splitAt: _*)
      verify(spyCol1).split(splitAt: _*)
      verify(spyCol2).split(splitAt: _*)
    }
  }

  test("convert large InternalRow iterator to cached batch single col") {
    val (_, spyGpuCol0) = getCudfAndGpuVectors()
    val cb = new ColumnarBatch(Array(spyGpuCol0), ROWS)
    val mockByteType = mock(classOf[ByteType])
    when(mockByteType.defaultSize).thenReturn(1024)
    val schema = Seq(AttributeReference("field0", mockByteType, true)())
    testColumnarBatchToCachedBatchIterator(cb, schema)
  }

  test("convert large InternalRow iterator to cached batch multi-col") {
    val (_, spyGpuCol0) = getCudfAndGpuVectors()
    val (_, spyGpuCol1) = getCudfAndGpuVectors()
    val (_, spyGpuCol2) = getCudfAndGpuVectors()
    val cb = new ColumnarBatch(Array(spyGpuCol0, spyGpuCol1, spyGpuCol2), ROWS)
    val mockByteType = mock(classOf[ByteType])
    when(mockByteType.defaultSize).thenReturn(1024)
    val schema = Seq(AttributeReference("field0", mockByteType, true)(),
      AttributeReference("field1", mockByteType, true)(),
      AttributeReference("field2", mockByteType, true)())

    testColumnarBatchToCachedBatchIterator(cb, schema)
  }

  val ROWS = 3 * 1024 * 1024

  private def getCudfAndGpuVectors(onHost: Boolean = false): (ColumnVector, GpuColumnVector)= {
    val spyCol = spy(ColumnVector.fromBytes(1))
    when(spyCol.getRowCount).thenReturn(ROWS)
    val mockDtype = mock(classOf[DType])
    when(mockDtype.getSizeInBytes).thenReturn(1024)
    val spyGpuCol = spy(GpuColumnVector.from(spyCol, ByteType))
    when(spyCol.getDeviceMemorySize).thenReturn(1024L * ROWS)

    (spyCol, spyGpuCol)
  }

  val _2GB = 2L * 1024 * 1024 * 1024
  val APPROX_PAR_META_DATA = 10 * 1024 * 1024 // we are estimating 10MB
  val BYTES_ALLOWED_PER_BATCH = _2GB - APPROX_PAR_META_DATA

  private def whenSplitCalled(cb: ColumnarBatch): Unit = {
    val rows = cb.numRows()
    val eachRowSize = cb.numCols() * 1024
    val rowsAllowedInABatch = BYTES_ALLOWED_PER_BATCH / eachRowSize
    val spillOver = cb.numRows() % rowsAllowedInABatch
    val splitRange = scala.Range(rowsAllowedInABatch.toInt, rows, rowsAllowedInABatch.toInt)
    scala.Range(0, cb.numCols()).indices.foreach { i =>
      val spyCol = cb.column(i).asInstanceOf[GpuColumnVector].getBase
      val splitCols0 = splitRange.indices.map { _ =>
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

  private def testCompressColBatch(
     cudfCols: Array[ColumnVector],
     gpuCols: Array[org.apache.spark.sql.vectorized.ColumnVector]): Unit = {
    // mock static method for Table
    val theTableMock = mockStatic(classOf[Table], (_: InvocationOnMock) =>
      new TableWriter {
        override def write(table: Table): Unit = {
          val tableSize = table.getColumn(0).getType.getSizeInBytes * table.getRowCount
          if (tableSize > Int.MaxValue) {
            fail(s"Parquet file went over the allowed limit of $BYTES_ALLOWED_PER_BATCH")
          }
        }

        override def close(): Unit = {
          // noop
        }
      })

    withResource(cudfCols) { _ =>
      val cb = new ColumnarBatch(gpuCols, ROWS)
      whenSplitCalled(cb)
      val ser = new ParquetCachedBatchSerializer
      val dummySchema = new StructType(Array(new StructField("empty", BooleanType, false)))
      ser.compressColumnarBatchWithParquet(cb, dummySchema)
      theTableMock.close()
    }
  }

  private def testColumnarBatchToCachedBatchIterator(
     cb: ColumnarBatch,
     schema: Seq[AttributeReference]): Unit = {

    val cbIter = new Iterator[ColumnarBatch] {
      val queue = new mutable.Queue[ColumnarBatch]
      queue += cb

      override def hasNext: Boolean = queue.nonEmpty

      override def next(): ColumnarBatch = {
        queue.dequeue()
      }
    }
    val ser = new ParquetCachedBatchSerializer

    val producer = new ser.CachedBatchIteratorProducer[ColumnarBatch](cbIter, schema,
      withCpuSparkSession(spark =>
        spark.sparkContext.broadcast(new SerializableConfiguration(new Configuration(true)))),
      withCpuSparkSession(spark => spark.sparkContext.broadcast(new SQLConf().getAllConfs)))
    val mockParquetOutputFileFormat = mock(classOf[ParquetOutputFileFormat])
    var totalSize = 0L
    val mockRecordWriter = new RecordWriter[Void, InternalRow] {
      val estimatedSize = schema.indices.length * 1024
      var thisBatchSize = 0L

      override def write(k: Void, v: InternalRow): Unit = {
        thisBatchSize += estimatedSize
        if (thisBatchSize > BYTES_ALLOWED_PER_BATCH) {
          fail(s"Parquet file went over the allowed limit of $BYTES_ALLOWED_PER_BATCH")
        }
      }

      override def close(taskAttemptContext: TaskAttemptContext): Unit = {
        totalSize += thisBatchSize
        thisBatchSize = 0
      }
    }
    when(mockParquetOutputFileFormat.getRecordWriter(any(), any())).thenReturn(mockRecordWriter)
    val cachedBatchIter = producer.getColumnarBatchToCachedBatchIterator
    cachedBatchIter.asInstanceOf[producer.ColumnarBatchToCachedBatchIterator]
      .setParquetOutputFileFormat(mockParquetOutputFileFormat)
    var totalRows = 0
    while (cachedBatchIter.hasNext) {
      val cb = cachedBatchIter.next()
      totalRows += cb.numRows
    }
    assert(totalRows == ROWS)
    assert(totalSize == ROWS * schema.indices.length * 1024L)
  }
}
