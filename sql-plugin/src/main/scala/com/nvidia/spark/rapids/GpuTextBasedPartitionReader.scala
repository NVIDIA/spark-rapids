/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.time.DateTimeException

import scala.collection.mutable.ListBuffer
import scala.math.max

import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, NvtxColor, NvtxRange, Scalar, Schema, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.TaskContext
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.rapids.ExceptionTimeParserPolicy
import org.apache.spark.sql.types.{DataTypes, DecimalType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The text based PartitionReader
 * @param conf the Hadoop configuration
 * @param partFile file split to read
 * @param dataSchema schema of the data
 * @param readDataSchema the Spark schema describing what will be read
 * @param lineSeparatorInRead An optional byte line sep.
 * @param maxRowsPerChunk maximum number of rows to read in a batch
 * @param maxBytesPerChunk maximum number of bytes to read in a batch
 * @param execMetrics metrics to update during read
 */
abstract class GpuTextBasedPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    lineSeparatorInRead: Option[Array[Byte]],
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric])
  extends PartitionReader[ColumnarBatch] with ScanWithMetrics with Arm {
  import GpuMetric._

  private var batch: Option[ColumnarBatch] = None
  private val lineReader = new HadoopFileLinesReader(partFile, lineSeparatorInRead, conf)
  private var isFirstChunkForIterator: Boolean = true
  private var isExhausted: Boolean = false
  private var maxDeviceMemory: Long = 0

  metrics = execMetrics

  private lazy val estimatedHostBufferSize: Long = {
    val rawPath = new Path(partFile.filePath)
    val fs = rawPath.getFileSystem(conf)
    val path = fs.makeQualified(rawPath)
    val fileSize = fs.getFileStatus(path).getLen
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(path)
    if (codec != null) {
      // wild guess that compression is 2X or less
      partFile.length * 2
    } else if (partFile.start + partFile.length == fileSize) {
      // last split doesn't need to read an additional record
      partFile.length
    } else {
      // wild guess for extra space needed for the record after the split end offset
      partFile.length + 128 * 1024
    }
  }

  /**
   * Grows a host buffer, returning a new buffer and closing the original
   * after copying the data into the new buffer.
   * @param original the original host memory buffer
   */
  private def growHostBuffer(original: HostMemoryBuffer, needed: Long): HostMemoryBuffer = {
    val newSize = Math.max(original.getLength * 2, needed)
    closeOnExcept(HostMemoryBuffer.allocate(newSize)) { result =>
      result.copyFromHostBuffer(0, original, 0, original.getLength)
      original.close()
      result
    }
  }

  private def readPartFile(): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      isFirstChunkForIterator = false
      val separator = lineSeparatorInRead.getOrElse(Array('\n'.toByte))
      var succeeded = false
      var totalSize: Long = 0L
      var totalRows: Integer = 0
      var hmb = HostMemoryBuffer.allocate(estimatedHostBufferSize)
      try {
        while (lineReader.hasNext
          && totalRows != maxRowsPerChunk
          && totalSize <= maxBytesPerChunk /* soft limit and returns at least one row */) {
          val line = lineReader.next()
          val lineSize = line.getLength
          val newTotal = totalSize + lineSize + separator.length
          if (newTotal > hmb.getLength) {
            hmb = growHostBuffer(hmb, newTotal)
          }
          // Can have an empty line, do not write this to buffer but add the separator
          // and totalRows
          if (lineSize != 0) {
            hmb.setBytes(totalSize, line.getBytes, 0, lineSize)
          }
          hmb.setBytes(totalSize + lineSize, separator, 0, separator.length)
          totalRows += 1
          totalSize = newTotal
        }
        //Indicate this is the last chunk
        isExhausted = !lineReader.hasNext
        succeeded = true
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
      (hmb, totalSize)
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange(getFileFormatShortName + " readBatch", NvtxColor.GREEN)) { _ =>
      val isFirstChunk = partFile.start == 0 && isFirstChunkForIterator
      val table = readToTable(isFirstChunk)
      try {
        if (readDataSchema.isEmpty) {
          table.map(t => new ColumnarBatch(Array.empty, t.getRowCount.toInt))
        } else {
          table.map(GpuColumnVector.from(_, readDataSchema.toArray.map(_.dataType)))
        }
      } finally {
        metrics(NUM_OUTPUT_BATCHES) += 1
        table.foreach(_.close())
      }
    }
  }

  private def readToTable(isFirstChunk: Boolean): Option[Table] = {
    val (dataBuffer, dataSize) = readPartFile()
    try {
      if (dataSize == 0) {
        None
      } else {
        val newReadDataSchema: StructType = if (readDataSchema.isEmpty) {
          val smallestField =
            dataSchema.min(Ordering.by[StructField, Integer](_.dataType.defaultSize))
          StructType(Seq(smallestField))
        } else {
          readDataSchema
        }

        // read boolean and numeric columns as strings in cuDF
        val dataSchemaWithStrings = StructType(dataSchema.fields
          .map(f => {
            f.dataType match {
              case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
                   DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
                   DataTypes.DoubleType | _: DecimalType | DataTypes.DateType =>
                f.copy(dataType = DataTypes.StringType)
              case _ =>
                f
            }
          }))
        val cudfSchema = GpuColumnVector.from(dataSchemaWithStrings)

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

        // The buffer that is sent down
        val table = withResource(new NvtxWithMetrics(getFileFormatShortName + " decode",
          NvtxColor.DARK_GREEN, metrics(GPU_DECODE_TIME))) { _ =>
          readToTable(dataBuffer, dataSize, cudfSchema, newReadDataSchema, isFirstChunk)
        }
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)

        // parse boolean and numeric columns that were read as strings
        val castTable = withResource(table) { _ =>
          val columns = new ListBuffer[ColumnVector]()
          // Table increases the ref counts on the columns so we have
          // to close them after creating the table
          withResource(columns) { _ =>
            for (i <- 0 until table.getNumberOfColumns) {
              val castColumn = newReadDataSchema.fields(i).dataType match {
                case DataTypes.BooleanType =>
                  castStringToBool(table.getColumn(i))
                case DataTypes.ByteType =>
                  castStringToInt(table.getColumn(i), DType.INT8)
                case DataTypes.ShortType =>
                  castStringToInt(table.getColumn(i), DType.INT16)
                case DataTypes.IntegerType =>
                  castStringToInt(table.getColumn(i), DType.INT32)
                case DataTypes.LongType =>
                  castStringToInt(table.getColumn(i), DType.INT64)
                case DataTypes.FloatType =>
                  castStringToFloat(table.getColumn(i), DType.FLOAT32)
                case DataTypes.DoubleType =>
                  castStringToFloat(table.getColumn(i), DType.FLOAT64)
                case dt: DecimalType =>
                  castStringToDecimal(table.getColumn(i), dt)
                case DataTypes.DateType =>
                  castStringToDate(table.getColumn(i))
                case _ =>
                  table.getColumn(i).incRefCount()
              }
              columns += castColumn
            }
            new Table(columns: _*)
          }
        }

        handleResult(newReadDataSchema, castTable)
      }
    } finally {
      dataBuffer.close()
    }
  }

  def dateFormat: String

  def castStringToDate(input: ColumnVector): ColumnVector = {
    val cudfFormat = DateUtils.toStrf(dateFormat, parseString = true)
    withResource(input.isTimestamp(cudfFormat)) { isDate =>
      if (GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy) {
        withResource(isDate.all()) { all =>
          if (all.isValid && !all.getBoolean) {
            throw new DateTimeException("One or more values is not a valid date")
          }
        }
      }
      withResource(input.asTimestamp(DType.TIMESTAMP_DAYS, cudfFormat)) { asDate =>
        withResource(Scalar.fromNull(DType.TIMESTAMP_DAYS)) { nullScalar =>
          isDate.ifElse(asDate, nullScalar)
        }
      }
    }
  }

  def castStringToBool(input: ColumnVector): ColumnVector

  def castStringToFloat(input: ColumnVector, dt: DType): ColumnVector = {
    GpuCast.castStringToFloats(input, ansiEnabled = false, dt)
  }

  def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {
    GpuCast.castStringToDecimal(input, ansiEnabled = false, dt)
  }

  def castStringToInt(input: ColumnVector, intType: DType): ColumnVector = {
    withResource(input.isInteger(intType)) { isInt =>
      withResource(input.castTo(intType)) { asInt =>
        withResource(Scalar.fromNull(intType)) { nullValue =>
          isInt.ifElse(asInt, nullValue)
        }
      }
    }
  }

  /**
   * Read the host buffer to GPU table
   * @param dataBuffer host buffer to be read
   * @param dataSize the size of host buffer
   * @param cudfSchema the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param isFirstChunk if it is the first chunk
   * @return table
   */
  def readToTable(
    dataBuffer: HostMemoryBuffer,
    dataSize: Long,
    cudfSchema: Schema,
    readDataSchema: StructType,
    isFirstChunk: Boolean): Table

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  /**
   * Handle the table decoded by GPU
   *
   * Please note that, this function owns table which is supposed to be closed in this function
   * But for the optimization, we just return the original table.
   *
   * @param readDataSchema the Spark schema describing what will be read
   * @param table the table decoded by GPU
   * @return the new optional Table
   */
  def handleResult(readDataSchema: StructType, table: Table): Option[Table] = {
    val numColumns = table.getNumberOfColumns

    closeOnExcept(table) { _ =>
      if (readDataSchema.length != numColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but only read ${table.getNumberOfColumns} from $partFile")
      }
    }

    // For the GPU resource handling convention, we should close input table and return a new
    // table just like below code. But for optimization, we just return the input table.
    // withResource(table) { _
    //  val cols = (0 until  table.getNumberOfColumns).map(i => table.getColumn(i))
    //  Some(new Table(cols: _*))
    // }
    Some(table)
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = if (isExhausted) {
      metrics(PEAK_DEVICE_MEMORY).set(maxDeviceMemory)
      None
    } else {
      readBatch()
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    lineReader.close()
    batch.foreach(_.close())
    batch = None
    isExhausted = true
  }
}
