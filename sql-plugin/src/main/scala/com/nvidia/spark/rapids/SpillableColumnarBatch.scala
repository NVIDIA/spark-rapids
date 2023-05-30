/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Holds a ColumnarBatch that the backing buffers on it can be spilled.
 */
trait SpillableColumnarBatch extends AutoCloseable {
  /**
   * The number of rows stored in this batch.
   */
  def numRows(): Int

  /**
   * Set a new spill priority.
   */
  def setSpillPriority(priority: Long): Unit

  /**
   * Get the columnar batch.
   * @note It is the responsibility of the caller to close the batch.
   * @note If the buffer is compressed data then the resulting batch will be built using
   *       `GpuCompressedColumnVector`, and it is the responsibility of the caller to deal
   *       with decompressing the data if necessary.
   */
  def getColumnarBatch(): ColumnarBatch

  def sizeInBytes: Long

  def dataTypes: Array[DataType]
}

/**
 * Cudf does not support a table with columns and no rows. This takes care of making one of those
 * spillable, even though in reality there is no backing buffer.  It does this by just keeping the
 * row count in memory, and not dealing with the catalog at all.
 */
class JustRowsColumnarBatch(numRows: Int)
    extends SpillableColumnarBatch {
  override def numRows(): Int = numRows
  override def setSpillPriority(priority: Long): Unit = () // NOOP nothing to spill

  def getColumnarBatch(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    new ColumnarBatch(Array.empty, numRows)
  }

  override def close(): Unit = () // NOOP nothing to close
  override val sizeInBytes: Long = 0L

  override def dataTypes: Array[DataType] = Array.empty
}

/**
 * The implementation of [[SpillableColumnarBatch]] that points to buffers that can be spilled.
 * @note the buffer should be in the cache by the time this is created and this is taking over
 *       ownership of the life cycle of the batch.  So don't call this constructor directly please
 *       use `SpillableColumnarBatch.apply` instead.
 */
class SpillableColumnarBatchImpl (
    handle: RapidsBufferHandle,
    rowCount: Int,
    sparkTypes: Array[DataType])
    extends SpillableColumnarBatch {

  override def dataTypes: Array[DataType] = sparkTypes
  /**
   * The number of rows stored in this batch.
   */
  override def numRows(): Int = rowCount

  private def withRapidsBuffer[T](fn: RapidsBuffer => T): T = {
    withResource(RapidsBufferCatalog.acquireBuffer(handle)) { rapidsBuffer =>
      fn(rapidsBuffer)
    }
  }

  override lazy val sizeInBytes: Long =
    withRapidsBuffer(_.getMemoryUsedBytes)

  /**
   * Set a new spill priority.
   */
  override def setSpillPriority(priority: Long): Unit = {
    handle.setSpillPriority(priority)
  }

  override def getColumnarBatch(): ColumnarBatch = {
    withRapidsBuffer { rapidsBuffer =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      rapidsBuffer.getColumnarBatch(sparkTypes)
    }
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    // closing my reference
    handle.close()
  }
}

object SpillableColumnarBatch {
  /**
   * Create a new SpillableColumnarBatch.
   *
   * @note This takes over ownership of batch, and batch should not be used after this.
   * @param batch         the batch to make spillable
   * @param priority      the initial spill priority of this batch
   */
  def apply(batch: ColumnarBatch,
      priority: Long): SpillableColumnarBatch = {
    val numRows = batch.numRows()
    if (batch.numCols() <= 0) {
      // We consumed it
      batch.close()
      new JustRowsColumnarBatch(numRows)
    } else {
      val types = GpuColumnVector.extractTypes(batch)
      val handle = addBatch(batch, priority)
      new SpillableColumnarBatchImpl(
        handle,
        numRows,
        types)
    }
  }

  /**
   * Create a new SpillableColumnarBatch
   * @note The caller is responsible for closing the contiguous table parameter.
   * @param ct contiguous table containing the batch GPU data
   * @param sparkTypes array of Spark types describing the data schema
   * @param priority the initial spill priority of this batch
   */
  def apply(
      ct: ContiguousTable,
      sparkTypes: Array[DataType],
      priority: Long): SpillableColumnarBatch = {
    val handle = RapidsBufferCatalog.addContiguousTable(ct, priority)
    withResource(RapidsBufferCatalog.acquireBuffer(handle)) { _ =>
      new SpillableColumnarBatchImpl(
        handle,
        ct.getRowCount.toInt,
        sparkTypes)
    }
  }

  private[this] def allFromSameBuffer(batch: ColumnarBatch): Boolean = {
    var bufferAddr = 0L
    var isSet = false
    val numColumns = batch.numCols()
    (0 until numColumns).forall { i =>
      batch.column(i) match {
        case fb: GpuColumnVectorFromBuffer =>
          if (!isSet) {
            bufferAddr = fb.getBuffer.getAddress
            isSet = true
            true
          } else {
            bufferAddr == fb.getBuffer.getAddress
          }
        case _ => false
      }
    }
  }

  private[this] def addBatch(
      batch: ColumnarBatch,
      initialSpillPriority: Long): RapidsBufferHandle = {
    withResource(batch) { batch =>
      val numColumns = batch.numCols()
      if (GpuCompressedColumnVector.isBatchCompressed(batch)) {
        val cv = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
        val buff = cv.getTableBuffer
        RapidsBufferCatalog.addBuffer(buff, cv.getTableMeta, initialSpillPriority)
      } else if (GpuPackedTableColumn.isBatchPacked(batch)) {
        val cv = batch.column(0).asInstanceOf[GpuPackedTableColumn]
        RapidsBufferCatalog.addContiguousTable(
          cv.getContiguousTable,
          initialSpillPriority)
      } else if (numColumns > 0 &&
          allFromSameBuffer(batch)) {
        val cv = batch.column(0).asInstanceOf[GpuColumnVectorFromBuffer]
        val buff = cv.getBuffer
        RapidsBufferCatalog.addBuffer(buff, cv.getTableMeta, initialSpillPriority)
      } else {
        RapidsBufferCatalog.addBatch(batch, initialSpillPriority)
      }
    }
  }

}


/**
 * Just like a SpillableColumnarBatch but for buffers.
 */
class SpillableBuffer(
    handle: RapidsBufferHandle) extends AutoCloseable {

  /**
   * Set a new spill priority.
   */
  def setSpillPriority(priority: Long): Unit = {
    handle.setSpillPriority(priority)
  }

  /**
   * Use the device buffer.
   */
  def getDeviceBuffer(): DeviceMemoryBuffer = {
    withResource(RapidsBufferCatalog.acquireBuffer(handle)) { rapidsBuffer =>
      rapidsBuffer.getDeviceMemoryBuffer
    }
  }

  /**
   * Remove the buffer from the cache.
   */
  override def close(): Unit = {
    handle.close()
  }
}

object SpillableBuffer {

  /**
   * Create a new SpillableBuffer.
   * @note This takes over ownership of buffer, and buffer should not be used after this.
   * @param buffer the buffer to make spillable
   * @param priority the initial spill priority of this buffer
   */
  def apply(buffer: DeviceMemoryBuffer,
      priority: Long): SpillableBuffer = {
    val meta = MetaUtils.getTableMetaNoTable(buffer)
    val handle = withResource(buffer) { _ => 
      RapidsBufferCatalog.addBuffer(buffer, meta, priority)
    }
    new SpillableBuffer(handle)
  }
}
