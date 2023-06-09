/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import java.io.OutputStream

import scala.collection.mutable

import ai.rapids.cudf.{HostBufferConsumer, HostMemoryBuffer, NvtxColor, NvtxRange, Table, TableWriter}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.{ColumnarWriteTaskStatsTracker, GpuWriteTaskStatsTracker}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A factory that produces [[ColumnarOutputWriter]]s.  A new [[ColumnarOutputWriterFactory]] is
 * created on the driver side, and then gets serialized to executor side to create
 * [[ColumnarOutputWriter]]s. This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.OutputWriterFactory`.
 */
abstract class ColumnarOutputWriterFactory extends Serializable {

  /** Returns the file extension to be used when writing files out. */
  def getFileExtension(context: TaskAttemptContext): String

  /**
   * When writing to a `org.apache.spark.sql.execution.datasources.HadoopFsRelation`, this method
   * gets called by each task on executor side to instantiate new [[ColumnarOutputWriter]]s.
   *
   * @param path Path to write the file.
   * @param dataSchema Schema of the columnar data to be written. Partition columns are not
   *                   included in the schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   */
  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): ColumnarOutputWriter
}

/**
 * This is used to write columnar data to a file system. Subclasses of [[ColumnarOutputWriter]]
 * must provide a zero-argument constructor. This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.OutputWriter`.
 */
abstract class ColumnarOutputWriter(context: TaskAttemptContext,
    dataSchema: StructType,
    rangeName: String,
    includeRetry: Boolean) extends HostBufferConsumer {

  val tableWriter: TableWriter
  val conf = context.getConfiguration

  private[this] val outputStream: FSDataOutputStream = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(conf)
    fs.create(hadoopPath, false)
  }
  private[this] val tempBuffer = new Array[Byte](128 * 1024)
  private[this] var anythingWritten = false
  private[this] val buffers = mutable.Queue[(HostMemoryBuffer, Long)]()

  override
  def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit =
    buffers += Tuple2(buffer, len)

  def writeBufferedData(): Unit = {
    ColumnarOutputWriter.writeBufferedData(buffers, tempBuffer, outputStream)
  }

  def dropBufferedData(): Unit = buffers.dequeueAll {
    case (buffer, _) =>
      buffer.close()
      true
  }

  /**
   * Persists a columnar batch. Invoked on the executor side. When writing to dynamically
   * partitioned tables, dynamic partition columns are not included in columns to be written.
   *
   * NOTE: This method will close `batch`. We do this because we want
   * to free GPU memory after the GPU has finished encoding the data but before
   * it is written to the distributed filesystem. The GPU semaphore is released
   * during the distributed filesystem transfer to allow other tasks to start/continue
   * GPU processing.
   */
  def writeAndClose(
      batch: ColumnarBatch,
      statsTrackers: Seq[ColumnarWriteTaskStatsTracker]): Unit = {
    var needToCloseBatch = true
    try {
      val writeStartTimestamp = System.nanoTime
      val writeRange = new NvtxRange("File write", NvtxColor.YELLOW)
      val gpuTime = try {
        needToCloseBatch = false
        writeBatch(batch)
      } finally {
        writeRange.close()
      }

      // Update statistics
      val writeTime = System.nanoTime - writeStartTimestamp - gpuTime
      statsTrackers.foreach {
        case gpuTracker: GpuWriteTaskStatsTracker =>
          gpuTracker.addWriteTime(writeTime)
          gpuTracker.addGpuTime(gpuTime)
        case _ =>
      }
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
    }
  }

  protected def scanTableBeforeWrite(table: Table): Unit = {
    // NOOP for now, but allows a child to override this
  }

  /**
   * Writes the columnar batch and returns the time in ns taken to write
   *
   * NOTE: This method will close `batch`. We do this because we want
   * to free GPU memory after the GPU has finished encoding the data but before
   * it is written to the distributed filesystem. The GPU semaphore is released
   * during the distributed filesystem transfer to allow other tasks to start/continue
   * GPU processing.
   *
   * @param batch Columnar batch that needs to be written
   * @return time in ns taken to write the batch
   */
  private[this] def writeBatch(batch: ColumnarBatch): Long = {
    if (includeRetry) {
      writeBatchWithRetry(batch)
    } else {
      writeBatchNoRetry(batch)
    }
  }

  /** Apply any necessary casts before writing batch out */
  def transform(cb: ColumnarBatch): Option[ColumnarBatch] = None

  private[this] def writeBatchWithRetry(batch: ColumnarBatch): Long = {
    val sb = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    RmmRapidsRetryIterator.withRetry(sb, RmmRapidsRetryIterator.splitSpillableInHalfByRows) { sb =>
      val cr = new CheckpointRestore {
        override def checkpoint(): Unit = ()
        override def restore(): Unit = dropBufferedData()
      }
      val startTimestamp = System.nanoTime
      withResource(sb.getColumnarBatch()) { cb =>
        //TODO: we should really apply the transformations to cast timestamps
        // to the expected types before spilling but we need a SpillableTable
        // rather than a SpillableColumnBatch to be able to do that
        // See https://github.com/NVIDIA/spark-rapids/issues/8262
        RmmRapidsRetryIterator.withRestoreOnRetry(cr) {
          withResource(new NvtxRange(s"GPU $rangeName write", NvtxColor.BLUE)) { _ =>
            transform(cb) match {
              case Some(transformed) =>
                // because we created a new transformed batch, we need to make sure we close it
                withResource(transformed) { _ =>
                  scanAndWrite(transformed)
                }
              case _ =>
                scanAndWrite(cb)
            }
          }
        }
      }
      GpuSemaphore.releaseIfNecessary(TaskContext.get)
      val gpuTime = System.nanoTime - startTimestamp
      writeBufferedData()
      gpuTime
    }.sum
  }

  private[this] def writeBatchNoRetry(batch: ColumnarBatch): Long = {
    var needToCloseBatch = true
    try {
      val startTimestamp = System.nanoTime
      withResource(new NvtxRange(s"GPU $rangeName write", NvtxColor.BLUE)) { _ =>
        transform(batch) match {
          case Some(transformed) =>
            // because we created a new transformed batch, we need to make sure we close it
            withResource(transformed) { _ =>
              scanAndWrite(transformed)
            }
          case _ =>
            scanAndWrite(batch)
        }
      }

      // Batch is no longer needed, write process from here does not use GPU.
      batch.close()
      needToCloseBatch = false
      GpuSemaphore.releaseIfNecessary(TaskContext.get)
      val gpuTime = System.nanoTime - startTimestamp
      writeBufferedData()
      gpuTime
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
    }
  }

  private def scanAndWrite(batch: ColumnarBatch): Unit = {
    withResource(GpuColumnVector.from(batch)) { table =>
      scanTableBeforeWrite(table)
      anythingWritten = true
      tableWriter.write(table)
    }
  }

  /**
   * Closes the [[ColumnarOutputWriter]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  def close(): Unit = {
    if (!anythingWritten) {
      // This prevents writing out bad files
      writeBatch(GpuColumnVector.emptyBatch(dataSchema))
    }
    tableWriter.close()
    writeBufferedData()
    outputStream.close()
  }

  /**
   * The file path to write. Invoked on the executor side.
   */
  def path(): String
}

object ColumnarOutputWriter {
  // write buffers to outputStream via tempBuffer and close buffers
  def writeBufferedData(buffers: mutable.Queue[(HostMemoryBuffer, Long)],
      tempBuffer: Array[Byte], outputStream: OutputStream): Unit = {
    val toProcess = buffers.dequeueAll(_ => true)
    try {
      toProcess.foreach { case (buffer, len) =>
        var offset: Long = 0
        var left = len
        while (left > 0) {
          val toCopy = math.min(tempBuffer.length, left).toInt
          buffer.getBytes(tempBuffer, 0, offset, toCopy)
          outputStream.write(tempBuffer, 0, toCopy)
          left = left - toCopy
          offset = offset + toCopy
        }
      }
    } finally {
      toProcess.map { case (buffer, _) => buffer }.safeClose()
    }
  }
}
