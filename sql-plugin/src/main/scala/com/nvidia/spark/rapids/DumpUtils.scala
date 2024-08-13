/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import java.io.{File, FileOutputStream, OutputStream}
import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.cudf.ColumnWriterOptions._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

object DumpUtils extends Logging {
  /**
   * Debug utility to dump a host memory buffer to a file.
   * @param conf Hadoop configuration
   * @param data buffer containing data to dump to a file
   * @param offset starting offset in the buffer of the data
   * @param len size of the data in bytes
   * @param prefix prefix for a path to write the data
   * @param suffix suffix for a path to write the data
   * @return Hadoop path for where the data was written or null on error
   */
  def dumpBuffer(
      conf: Configuration,
      data: HostMemoryBuffer,
      offset: Long,
      len: Long,
      prefix: String,
      suffix: String): String = {
    try {
      val (out, path) = FileUtils.createTempFile(conf, prefix, suffix)
      withResource(out) { _ =>
        withResource(data.slice(offset, len)) { hmb =>
          withResource(new HostMemoryInputStream(hmb, hmb.getLength)) { in =>
            IOUtils.copy(in, out)
          }
        }
      }
      path.toString
    } catch {
      case e: Exception =>
        log.error(s"Error attempting to dump data", e)
        s"<error writing data $e>"
    }
  }

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
   * Dump columnar batch to output stream in parquet format. <br>
   *
   * @param columnarBatch The columnar batch to be dumped, should be GPU columnar batch. It
   *                      should be closed by caller.
   * @param outputStream Will be closed after writing.
   */
  def dumpToParquet(columnarBatch: ColumnarBatch, outputStream: OutputStream): Unit = {
    closeOnExcept(outputStream) { _ =>
      withResource(GpuColumnVector.from(columnarBatch)) { table =>
        withResource(new ParquetDumper(outputStream, table)) { dumper =>
          dumper.writeTable(table)
        }
      }
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
    if (table.getNumberOfColumns == 0) {
      logWarning("dump to parquet failed, has no column, file prefix is " + filePrefix)
      None
    } else {
      Some(dumpToParquetFileImp(table, filePrefix))
    }
  }

  private def dumpToParquetFileImp(table: Table, filePrefix: String): String = {
    val path = genPath(filePrefix)
    withResource(new ParquetDumper(path, table)) { dumper =>
      dumper.writeTable(table)
      path
    }
  }

  private def dumpToParquetFileImpl(columnarBatch: ColumnarBatch, filePrefix: String): String = {
    // transform to table then dump
    withResource(GpuColumnVector.from(columnarBatch)) { table =>
      dumpToParquetFileImp(table, filePrefix)
    }
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

// parquet dumper
class ParquetDumper(private val outputStream: OutputStream, table: Table) extends HostBufferConsumer
  with AutoCloseable {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  def this(path: String, table: Table) = {
    this(new FileOutputStream(path), table)
  }

  private lazy val tableWriter: TableWriter = {
    // avoid anything conversion, just dump as it is
    val builder = ParquetDumper.parquetWriterOptionsFromTable(ParquetWriterOptions.builder(), table)
      .withCompressionType(ParquetDumper.COMPRESS_TYPE)
    Table.writeParquetChunked(builder.build(), this)
  }

  override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit =
    ColumnarOutputWriter.writeBufferedData(mutable.Queue((buffer, len)), tempBuffer,
      outputStream)

  def writeTable(table: Table): Unit = {
    tableWriter.write(table)
  }

  /**
   * Closes the [[ParquetDumper]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  def close(): Unit = {
    tableWriter.close()
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

object ParquetDumper {
  val COMPRESS_TYPE = CompressionType.SNAPPY

  def parquetWriterOptionsFromTable[T <: NestedBuilder[_, _], V <: ColumnWriterOptions](
      builder: ColumnWriterOptions.NestedBuilder[T, V],
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
    V <: ColumnWriterOptions](
      builder: ColumnWriterOptions.NestedBuilder[T, V],
      cv: ColumnView,
      cIndex: ColumnIndex,
      toClose: ArrayBuffer[ColumnView]): T = {
    val dType = cv.getType
    if (dType.isDecimalType) {
      val precision = dType.getTypeId match {
        case DType.DTypeEnum.DECIMAL32 => DType.DECIMAL32_MAX_PRECISION
        case DType.DTypeEnum.DECIMAL64 => DType.DECIMAL64_MAX_PRECISION
        case DType.DTypeEnum.DECIMAL128 => DType.DECIMAL128_MAX_PRECISION
        case _ =>
          throw new UnsupportedOperationException("unsupported type " + dType.getTypeId)
      }
      builder.withDecimalColumn(getTypeName(dType) + cIndex.inc(), precision, true)
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
    "c_" + t.toString.replace(' ', '_')
  }
}
