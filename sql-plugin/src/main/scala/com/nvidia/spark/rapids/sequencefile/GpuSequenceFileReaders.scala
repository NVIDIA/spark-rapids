/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.sequencefile

import java.io.IOException
import java.net.URI

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.jni.SequenceFile
import com.nvidia.spark.rapids.shims.ShimFilePartitionReaderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * GPU-native SequenceFile reader using CUDA kernels for parsing.
 *
 * This reader:
 * 1. Parses the SequenceFile header on CPU to extract the sync marker
 * 2. Reads the file data into GPU device memory
 * 3. Uses CUDA kernels to parse records in parallel
 * 4. Returns cuDF LIST[UINT8] columns (Spark BinaryType)
 */
class GpuSequenceFilePartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    requiredSchema: StructType,
    execMetrics: Map[String, GpuMetric])
  extends PartitionReader[ColumnarBatch] with Logging {

  private val path = new Path(new URI(partFile.filePath.toString))

  private val wantsKey = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD))
  private val wantsValue = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD))

  private var batch: Option[ColumnarBatch] = None
  private var exhausted = false

  private def readMetric: GpuMetric = execMetrics.getOrElse(READ_FS_TIME, NoopMetric)
  private def decodeMetric: GpuMetric = execMetrics.getOrElse(GPU_DECODE_TIME, NoopMetric)
  private def bufferMetric: GpuMetric = execMetrics.getOrElse(BUFFER_TIME, NoopMetric)

  override def next(): Boolean = {
    // Close any batch that was prepared but never consumed
    val previousBatch = batch
    batch = None
    previousBatch.foreach(_.close())

    if (exhausted) {
      false
    } else {
      batch = readFile()
      exhausted = true
      batch.isDefined
    }
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException("No batch available"))
    batch = None
    ret
  }

  private def readFile(): Option[ColumnarBatch] = {

    // Step 1: Parse header on CPU to get sync marker
    val header = bufferMetric.ns {
      try {
        SequenceFileHeader.parse(path, conf)
      } catch {
        case e: Exception =>
          logError(s"Failed to parse SequenceFile header: $path", e)
          throw new IOException(s"Failed to parse SequenceFile header: $path", e)
      }
    }

    // Validate that file is GPU-parseable (uncompressed)
    if (!header.isGpuParseable) {
      val msg = s"${SequenceFileBinaryFileFormat.SHORT_NAME} does not support " +
        s"compressed SequenceFiles, file=$path, isCompressed=${header.isCompressed}, " +
        s"isBlockCompressed=${header.isBlockCompressed}"
      throw new UnsupportedOperationException(msg)
    }

    // Step 2: Read file data (excluding header) into host memory, then copy to GPU
    val fs = path.getFileSystem(conf)
    val fileStatus = fs.getFileStatus(path)
    val fileSize = fileStatus.getLen
    val dataSize = fileSize - header.headerSize

    logInfo(s"SequenceFile $path: fileSize=$fileSize, headerSize=${header.headerSize}, " +
      s"dataSize=$dataSize, syncMarker=${header.syncMarker.map(b => f"$b%02x").mkString}")

    if (dataSize <= 0) {
      // Empty file - no records to return
      logInfo(s"[GPU-SEQFILE] SequenceFile $path has no data after header (empty file). " +
        s"fileSize=$fileSize, headerSize=${header.headerSize}, dataSize=$dataSize")
      return None
    }

    // Read data portion into device memory
    var firstBytesDebug: String = ""
    val deviceBuffer = readMetric.ns {
      val hostBuffer = closeOnExcept(HostMemoryBuffer.allocate(dataSize)) { hostBuf =>
        val in = fs.open(path)
        try {
          // Skip header
          in.seek(header.headerSize)
          // Read into host buffer
          val bytes = new Array[Byte](math.min(dataSize, 8 * 1024 * 1024).toInt)
          var remaining = dataSize
          var offset = 0L
          while (remaining > 0) {
            val toRead = math.min(remaining, bytes.length).toInt
            val bytesRead = in.read(bytes, 0, toRead)
            if (bytesRead < 0) {
              throw new IOException(
                s"Unexpected end of file at offset $offset, expected $dataSize bytes")
            }
            hostBuf.setBytes(offset, bytes, 0, bytesRead)
            // Store first bytes for debugging
            if (offset == 0 && bytesRead >= 20) {
              firstBytesDebug = bytes.take(math.min(60, bytesRead))
                .map(b => f"$b%02x").mkString(" ")
            }
            offset += bytesRead
            remaining -= bytesRead
          }
          hostBuf
        } finally {
          in.close()
        }
      }

      // Copy to device
      closeOnExcept(hostBuffer) { _ =>
        withResource(hostBuffer) { hb =>
          val db = DeviceMemoryBuffer.allocate(dataSize)
          closeOnExcept(db) { _ =>
            db.copyFromHostBuffer(hb)
          }
          db
        }
      }
    }

    // Step 3: Parse on GPU using CUDA kernel
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val columns = withResource(deviceBuffer) { devBuf =>
      decodeMetric.ns {
        SequenceFile.parseSequenceFile(
          devBuf,
          dataSize,
          header.syncMarker,
          wantsKey,
          wantsValue)
      }
    }

    if (columns == null || columns.isEmpty) {
      throw new RuntimeException(
        s"GPU SequenceFile parser returned null/empty columns for $path. " +
        s"Debug info: fileSize=$fileSize, headerSize=${header.headerSize}, " +
        s"dataSize=$dataSize, wantsKey=$wantsKey, wantsValue=$wantsValue, " +
        s"syncMarker=${header.syncMarker.map(b => f"$b%02x").mkString(",")}, " +
        s"firstDataBytes=[$firstBytesDebug]")
    }

    // Step 4: Build ColumnarBatch
    // Determine numRows from one of the columns
    val numRows = columns(0).getRowCount.toInt
    if (numRows == 0) {
      // Throw exception with debug info instead of silently returning None
      columns.foreach(_.close())
      throw new RuntimeException(
        s"GPU SequenceFile parser found 0 records in $path. " +
        s"Debug info: fileSize=$fileSize, headerSize=${header.headerSize}, " +
        s"dataSize=$dataSize, numColumns=${columns.length}, " +
        s"syncMarker=${header.syncMarker.map(b => f"$b%02x").mkString(",")}, " +
        s"firstDataBytes=[$firstBytesDebug]")
    }

    // Validate column structure before proceeding
    columns.foreach { col =>
      if (col.getNullCount > numRows) {
        logWarning(s"Column has more nulls (${col.getNullCount}) than rows ($numRows)")
      }
    }

    // Map columns based on wantsKey/wantsValue order
    var colIdx = 0
    val keyCol = if (wantsKey && colIdx < columns.length) {
      val col = columns(colIdx)
      colIdx += 1
      Some(col)
    } else None

    val valueCol = if (wantsValue && colIdx < columns.length) {
      val col = columns(colIdx)
      colIdx += 1
      Some(col)
    } else None

    closeOnExcept(keyCol) { _ =>
      closeOnExcept(valueCol) { _ =>
        val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
          if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)) {
            keyCol match {
              case Some(col) => GpuColumnVector.from(col.incRefCount(), BinaryType)
              case None => GpuColumnVector.fromNull(numRows, f.dataType)
            }
          } else if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)) {
            valueCol match {
              case Some(col) => GpuColumnVector.from(col.incRefCount(), BinaryType)
              case None => GpuColumnVector.fromNull(numRows, f.dataType)
            }
          } else {
            GpuColumnVector.fromNull(numRows, f.dataType)
          }
        }

        closeOnExcept(cols) { _ =>
          // Close the original columns after we've created the GpuColumnVectors
          keyCol.foreach(_.close())
          valueCol.foreach(_.close())
          Some(new ColumnarBatch(cols, numRows))
        }
      }
    }
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    exhausted = true
  }
}

/**
 * Factory for creating GPU SequenceFile partition readers.
 */
case class GpuSequenceFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) with Logging {

  private val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val baseReader = new GpuSequenceFilePartitionReader(
        conf,
        partFile,
        readDataSchema,
      metrics)
    val reader = new PartitionReaderWithBytesRead(baseReader)
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }
}
