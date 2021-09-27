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

import java.io.{File, FileOutputStream}
import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView, CompressionType, DType, HostBufferConsumer, HostMemoryBuffer, ParquetColumnWriterOptions, ParquetWriterOptions, Table, TableWriter}
import ai.rapids.cudf.ParquetColumnWriterOptions.{listBuilder, structBuilder, NestedBuilder}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object DumpUtils extends Logging with Arm {
  /**
   * Debug utility to dump columnar batch to parquet file. <br>
   * It's running on GPU. Parquet column names are generated from columnar batch type info. <br>
   *
   * @param columnarBatch the columnar batch to be dumped, should be GPU columnar batch
   * @param filePrefix    parquet file prefix, e.g. /tmp/my-debug-prefix-
   * @return parquet file path if dump is successful, e.g. /tmp/my-debug-prefix-123.parquet
   */
  def dumpToParquetFile(columnarBatch: ColumnarBatch, filePrefix: String): Option[String] = {
    if (columnarBatch.numCols() == 0) {
      logWarning("dump to parquet failed, has no column, file prefix is " + filePrefix)
      None
    } else {
      Some(dumpToParquetFileImpl(columnarBatch, filePrefix))
    }
  }

  /**
   * Debug utility to dump table to parquet file. <br>
   * It's running on GPU. Parquet column names are generated from table column type info. <br>
   *
   * @param table      the table to be dumped, should be GPU columnar batch
   * @param filePrefix parquet file prefix, e.g. /tmp/my-debug-prefix-
   * @return parquet file path if dump is successful, e.g. /tmp/my-debug-prefix-123.parquet
   */
  def dumpToParquetFile(table: Table, filePrefix: String): Option[String] = {
    logWarning("my-debug begin dump")
    if (table.getNumberOfColumns == 0) {
      logWarning("dump to parquet failed, has no column, file prefix is " + filePrefix)
      None
    } else {
      Some(dumpToParquetFileImp(table, filePrefix))
    }
  }

  private def dumpToParquetFileImp(table: Table, filePrefix: String): String = {
    val path = genPath(filePrefix)

    // write data, Note not close the table
    val dumper = new GpuTableToParquetDumper(path, table)

    try {
      // should not impact table
      dumper.writeTable(table)
      path
    } finally {
      dumper.close()
    }
  }

  private def dumpToParquetFileImpl(columnarBatch: ColumnarBatch, filePrefix: String): String = {
    // generate path, retry if file exists
    val path = genPath(filePrefix)

    // construct schema according field types
    val schema = genSchema(columnarBatch)

    // write data, Note not close the column
    val dumper = new GpuColumnBatchToParquetDumper(path, schema)
    try {
      dumper.writeBatch(columnarBatch)
    } finally {
      dumper.close()
    }

    path
  }

  private def genSchema(columnarBatch: ColumnarBatch): StructType = {
    val fields = new Array[StructField](columnarBatch.numCols())
    for (i <- 0 until columnarBatch.numCols()) {
      // ,() are invalid in column name, replace these characters
      val name = ("c" + i + "_" + columnarBatch.column(i).dataType())
        .replace(',', '_')
        .replace('(', '[')
        .replace(')', ']')
      fields(i) = StructField(name, columnarBatch.column(i).dataType())
    }
    StructType(fields)
  }

  private def genPath(filePrefix: String): String = {
    var path = ""
    val random = new Random
    var succeeded = false
    while (!succeeded) {
      path = filePrefix + random.nextInt(Int.MaxValue) + ".parquet"
      if (!new File(path).exists()) {
        succeeded = true
      }
    }
    path
  }
}

// used for dump column batch to parquet
class GpuColumnBatchToParquetDumper(
    path: String,
    schema: StructType)
  extends ParquetDumper(path) {

  override val tableWriter: TableWriter = {
    val writeOptionBuilder = GpuParquetFileFormat
      // avoid anything conversion, just dump as it is
      .parquetWriterOptionsFromSchema(ParquetWriterOptions.builder(), schema, writeInt96 = false)
      .withCompressionType(ParquetDumper.COMPRESS_TYPE)
    Table.writeParquetChunked(writeOptionBuilder.build(), this)
  }
}

// used for dump table to parquet
class GpuTableToParquetDumper(
    path: String,
    table: Table)
  extends ParquetDumper(path) {

  override val tableWriter: TableWriter = {
    // avoid anything conversion, just dump as it is
    val builder = ParquetDumper.parquetWriterOptionsFromTable(ParquetWriterOptions.builder(), table)
      .withCompressionType(ParquetDumper.COMPRESS_TYPE)
    Table.writeParquetChunked(builder.build(), this)
  }
}

// parquet dumper
abstract class ParquetDumper(path: String) extends HostBufferConsumer with Arm {
  private[this] val outputStream = new FileOutputStream(path)
  private[this] val tempBuffer = new Array[Byte](128 * 1024)
  private[this] val buffers = mutable.Queue[(HostMemoryBuffer, Long)]()

  val tableWriter: TableWriter

  override
  def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit =
    buffers += Tuple2(buffer, len)

  def writeBufferedData(): Unit = {
    ColumnarOutputWriter.writeBufferedData(buffers, tempBuffer, outputStream)
  }

  def writeBatch(batch: ColumnarBatch): Long = {
    val startTimestamp = System.nanoTime
    withResource(GpuColumnVector.from(batch)) { table =>
      tableWriter.write(table)
    }

    GpuSemaphore.releaseIfNecessary(TaskContext.get)
    val gpuTime = System.nanoTime - startTimestamp
    writeBufferedData()
    gpuTime
  }

  def writeTable(table: Table): Long = {
    val startTimestamp = System.nanoTime

    val columns = new Array[ColumnVector](table.getNumberOfColumns)
    for (i <- 0 until table.getNumberOfColumns) columns(i) = table.getColumn(i)

    // copy to new table and write
    withResource(new Table(columns: _*)) { newTable =>
      tableWriter.write(newTable)
    }

    // Batch is no longer needed, write process from here does not use GPU.
    GpuSemaphore.releaseIfNecessary(TaskContext.get)
    val gpuTime = System.nanoTime - startTimestamp
    writeBufferedData()
    gpuTime
  }

  /**
   * Closes the [[ParquetDumper]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  def close(): Unit = {
    tableWriter.close()
    writeBufferedData()
    outputStream.close()
  }
}

private class ColumnIndex() {
  var i = 0

  def inc(): Int = {
    i = i + 1
    i
  }
}

object ParquetDumper extends Arm {
  val COMPRESS_TYPE = CompressionType.SNAPPY

  def parquetWriterOptionsFromTable[T <: NestedBuilder[_, _], V <: ParquetColumnWriterOptions](
      builder: ParquetColumnWriterOptions.NestedBuilder[T, V],
      table: Table): T = {

    val cIndex = new ColumnIndex
    withResource(new ArrayBuffer[ColumnView]) { toClose =>
      for (i <- 0 until table.getNumberOfColumns) {
        parquetWriterOptionsFromColumnView(builder, table.getColumn(i), cIndex, toClose)
      }

      builder.asInstanceOf[T]
    }
  }

  private def parquetWriterOptionsFromColumnView[T <: NestedBuilder[_, _],
    V <: ParquetColumnWriterOptions](
      builder: ParquetColumnWriterOptions.NestedBuilder[T, V],
      cv: ColumnView,
      cIndex: ColumnIndex,
      toClose: ArrayBuffer[ColumnView]): T = {
    val dType = cv.getType
    if (dType.isDecimalType) {
      if (dType.getTypeId == DType.DTypeEnum.DECIMAL32) {
        builder.withDecimalColumn(getTypeName(dType) + cIndex.inc(),
          DType.DECIMAL32_MAX_PRECISION, true)
      } else if (dType.getTypeId == DType.DTypeEnum.DECIMAL64) {
        builder.withDecimalColumn(getTypeName(dType) + cIndex.inc(),
          DType.DECIMAL64_MAX_PRECISION, true)
      } else {
        // TODO for decimal 128 or other decimal
        throw new UnsupportedOperationException("not support " + dType.getTypeId)
      }
    } else if (dType == DType.STRUCT) {
      val subBuilder = structBuilder("c_struct" + cIndex.inc(), true)
      for (i <- 0 until cv.getNumChildren) {
        val subCv = cv.getChildColumnView(i)
        toClose += subCv
        parquetWriterOptionsFromColumnView(subBuilder, subCv, cIndex, toClose)
      }
      builder.withStructColumn(subBuilder.build())
    } else if (dType == DType.LIST) {
      val subCv = cv.getChildColumnView(0)
      toClose += subCv

      builder.withListColumn(
        parquetWriterOptionsFromColumnView(
          listBuilder("c_list" + cIndex.inc(), true),
          subCv,
          cIndex,
          toClose).build())
    } else {
      builder.withColumns(true, getTypeName(dType) + cIndex.inc())
    }
    builder.asInstanceOf[T]
  }

  private def getTypeName(t: DType): String = {
    "c_" + t.toString.replace(" ", "_")
  }
}
