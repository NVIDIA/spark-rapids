/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.jni.kudo.KudoSerializer
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

object DumpUtils extends Logging {
  /**
   * Debug utility to dump a host memory buffer to a file.
   *
   * @param conf   Hadoop configuration
   * @param data   buffer containing data to dump to a file
   * @param offset starting offset in the buffer of the data
   * @param len    size of the data in bytes
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

  def dumpBuffer(
      conf: Configuration,
      data: Array[HostMemoryBuffer],
      prefix: String,
      suffix: String): String = {
    try {
      val (out, path) = FileUtils.createTempFile(conf, prefix, suffix)
      withResource(out) { _ =>
        data.foreach { hmb =>
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
   * @param outputStream  Will be closed after writing.
   * @param kudoSerializer Optional. Only required when the batch contains a
   *                       KudoSerializedTableColumn.
   */
  def dumpToParquet(columnarBatch: ColumnarBatch, outputStream: OutputStream,
      kudoSerializer: Option[KudoSerializer] = None)
  : Unit = {
    closeOnExcept(outputStream) { _ =>
      dumpToParquetInternal(
        columnarBatch = columnarBatch,
        outputStream = outputStream,
        kudoSerializer = kudoSerializer,
        originalColumnNames = None,
        originalSparkTypes = None)
    }
  }

  /**
   * Dump columnar batch to output stream in parquet format with original Spark schema names.
   * Column names and nested field names are derived from the provided Spark types and names.
   */
  def dumpToParquet(
      columnarBatch: ColumnarBatch,
      outputStream: OutputStream,
      columnNames: Seq[String],
      sparkTypes: Seq[org.apache.spark.sql.types.DataType],
      kudoSerializer: Option[KudoSerializer])
  : Unit = {
    require(columnNames.length == columnarBatch.numCols(),
      s"Column names size ${columnNames.length} != numCols ${columnarBatch.numCols()}")
    require(sparkTypes.length == columnarBatch.numCols(),
      s"Spark types size ${sparkTypes.length} != numCols ${columnarBatch.numCols()}")

    closeOnExcept(outputStream) { _ =>
      dumpToParquetInternal(
        columnarBatch = columnarBatch,
        outputStream = outputStream,
        kudoSerializer = kudoSerializer,
        originalColumnNames = Some(columnNames),
        originalSparkTypes = Some(sparkTypes))
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

  /**
   * Deserialize a SerializedTableColumn back to a Table
   */
  private def deserializeSerializedTableColumn(column: SerializedTableColumn): Table = {
    import ai.rapids.cudf.JCudfSerialization
    val header = column.header
    val buffer = column.hostBuffer
    if (buffer == null || header == null) {
      throw new IllegalArgumentException("Cannot deserialize from null header or buffer")
    }

    withResource(JCudfSerialization.unpackHostColumnVectors(header, buffer)) { hostColumns =>
      withResource(hostColumns.map(_.copyToDevice())) { deviceColumns =>
        new Table(deviceColumns: _*)
      }
    }
  }

  /**
   * Deserialize a KudoSerializedTableColumn back to a Table
   */
  private def deserializeKudoSerializedTableColumn(kudoSerializer: KudoSerializer,
      column: KudoSerializedTableColumn): Table
  = {
    withResource(column.spillableKudoTable.makeKudoTable) { kudoTable =>
      kudoSerializer.mergeToTable(Array(kudoTable))
    }
  }

  private def dumpToParquetInternal(
      columnarBatch: ColumnarBatch,
      outputStream: OutputStream,
      kudoSerializer: Option[KudoSerializer],
      originalColumnNames: Option[Seq[String]],
      originalSparkTypes: Option[org.apache.spark.sql.types.Seq[org.apache.spark.sql.types.DataType]])
  : Unit = {
    // KudoSerializedTableColumn to a table first
    val table = if (columnarBatch.numCols() == 1) {
      columnarBatch.column(0) match {
        case serializedCol: SerializedTableColumn =>
          deserializeSerializedTableColumn(serializedCol)
        case kudoCol: KudoSerializedTableColumn =>
          require(kudoSerializer.isDefined,
            "KudoSerializer must be provided when handling KudoSerializedTableColumn")
          deserializeKudoSerializedTableColumn(kudoSerializer.get, kudoCol)
        case _ =>
          GpuColumnVector.from(columnarBatch)
      }
    } else {
      GpuColumnVector.from(columnarBatch)
    }

    withResource(table) { t =>
      val dumper = originalColumnNames match {
        case Some(names) =>
          new ParquetDumper(outputStream, t, Some(names), originalSparkTypes)
        case None =>
          new ParquetDumper(outputStream, t)
      }
      withResource(dumper) { d =>
        d.writeTable(t)
      }
    }
  }
}


// parquet dumper
class ParquetDumper(private val outputStream: OutputStream, table: Table,
    originalColumnNames: Option[Seq[String]] = None,
    originalSparkTypes: Option[Seq[org.apache.spark.sql.types.DataType]] = None)
  extends HostBufferConsumer
  with AutoCloseable {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  def this(path: String, table: Table) = {
    this(new FileOutputStream(path), table)
  }

  private lazy val tableWriter: TableWriter = {
    val builder = originalColumnNames match {
      case Some(names) =>
        ParquetDumper.parquetWriterOptionsFromSparkSchema(
          ParquetWriterOptions.builder(), table, names, originalSparkTypes.get)
      case None =>
        ParquetDumper.parquetWriterOptionsFromTable(ParquetWriterOptions.builder(), table)
    }
    Table.writeParquetChunked(builder.withCompressionType(ParquetDumper.COMPRESS_TYPE).build(),
      this)
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

  /**
   * Build writer options using original Spark schema names and types.
   */
  def parquetWriterOptionsFromSparkSchema[T <: NestedBuilder[_, _], V <: ColumnWriterOptions](
      builder: ColumnWriterOptions.NestedBuilder[T, V],
      table: Table,
      columnNames: Seq[String],
      sparkTypes: Seq[org.apache.spark.sql.types.DataType]): T = {
    require(columnNames.length == table.getNumberOfColumns,
      s"Column names size ${columnNames.length} != number of columns ${table.getNumberOfColumns}")
    require(sparkTypes.length == table.getNumberOfColumns,
      s"Spark types size ${sparkTypes.length} != number of columns ${table.getNumberOfColumns}")

    withResource(new ArrayBuffer[ColumnView]) { toClose =>
      var i = 0
      while (i < table.getNumberOfColumns) {
        parquetWriterOptionsFromSparkType(
          builder,
          table.getColumn(i),
          columnNames(i),
          sparkTypes(i),
          toClose)
        i += 1
      }
      builder.asInstanceOf[T]
    }
  }

  private def parquetWriterOptionsFromSparkType[T <: NestedBuilder[_, _], V <: ColumnWriterOptions](
      builder: ColumnWriterOptions.NestedBuilder[T, V],
      cv: ColumnView,
      fieldName: String,
      sparkType: org.apache.spark.sql.types.DataType,
      toClose: ArrayBuffer[ColumnView]): T = {
    import org.apache.spark.sql.types._
    sparkType match {
      case StructType(fields) =>
        val subBuilder = structBuilder(fieldName, true)
        var childIndex = 0
        fields.foreach { f =>
          val subCv = cv.getChildColumnView(childIndex)
          toClose += subCv
          parquetWriterOptionsFromSparkType(subBuilder, subCv, f.name, f.dataType, toClose)
          childIndex += 1
        }
        builder.withStructColumn(subBuilder.build())
      case ArrayType(elementType, _) =>
        val subCv = cv.getChildColumnView(0)
        toClose += subCv
        val lb = listBuilder(fieldName, true)
        val built = parquetWriterOptionsFromSparkType(lb, subCv, "element", elementType, toClose)
        builder.withListColumn(built.build())
      case MapType(keyType, valueType, _) =>
        // Map is represented as a list of struct<key,value>
        val listCv = cv.getChildColumnView(0)
        toClose += listCv
        val structCv = listCv.getChildColumnView(0)
        toClose += structCv
        val lb = listBuilder(fieldName, true)
        val sb = structBuilder("key_value", true)
        val keyCv = structCv.getChildColumnView(0)
        val valCv = structCv.getChildColumnView(1)
        toClose ++= Array(keyCv, valCv)
        parquetWriterOptionsFromSparkType(sb, keyCv, "key", keyType, toClose)
        parquetWriterOptionsFromSparkType(sb, valCv, "value", valueType, toClose)
        builder.withListColumn(lb.withStructColumn(sb.build()).build())
      case _ =>
        builder.withColumns(true, fieldName)
    }
    builder.asInstanceOf[T]
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
