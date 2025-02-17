/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.spill.{SpillableColumnarBatchFromBufferHandle, SpillableColumnarBatchHandle, SpillableCompressedColumnarBatchHandle, SpillableDeviceBufferHandle, SpillableHostBufferHandle, SpillableHostColumnarBatchHandle}

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
   * Increment the reference count for this batch (if applicable) and
   * return this for easy chaining.
   */
  def incRefCount(): SpillableColumnarBatch

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

  override def toString: String =
    s"SCB size:$sizeInBytes, types:${dataTypes.toList}, rows:${numRows()}"
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

  // There is no off heap data and close is a noop so just return this
  override def incRefCount(): SpillableColumnarBatch = this

  override def toString: String = s"JustRowsSCB size:$sizeInBytes, rows:$numRows"
}

/**
 * The implementation of [[SpillableColumnarBatch]] that points to buffers that can be spilled.
 * @note the buffer should be in the cache by the time this is created and this is taking over
 *       ownership of the life cycle of the batch.  So don't call this constructor directly please
 *       use `SpillableColumnarBatch.apply` instead.
 */
class SpillableColumnarBatchImpl (
    handle: SpillableColumnarBatchHandle,
    rowCount: Int,
    sparkTypes: Array[DataType])
    extends SpillableColumnarBatch {
  private var refCount = 1

  override def dataTypes: Array[DataType] = sparkTypes
  /**
   * The number of rows stored in this batch.
   */
  override def numRows(): Int = rowCount

  override lazy val sizeInBytes: Long = handle.approxSizeInBytes

  /**
   * Set a new spill priority.
   */
  override def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  override def getColumnarBatch(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    handle.materialize(sparkTypes)
  }

  override def incRefCount(): SpillableColumnarBatch = {
    if (refCount <= 0) {
      throw new IllegalStateException("Use after free on SpillableColumnarBatchImpl")
    }
    refCount += 1
    this
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    refCount -= 1
    if (refCount == 0) {
      // closing my reference
      handle.close()
    }
    // TODO this is causing problems so we need to look into this
    //  https://github.com/NVIDIA/spark-rapids/issues/10161
    //else if (refCount < 0) {
    //  throw new IllegalStateException("Double free on SpillableColumnarBatchImpl")
    //}
  }

  override def toString: String =
    s"SCB size:$sizeInBytes, handle:$handle, rows:$rowCount, types:${sparkTypes.toList}," +
      s" refCount:$refCount"
}

class SpillableCompressedColumnarBatchImpl(
    handle: SpillableCompressedColumnarBatchHandle, rowCount: Int)
  extends SpillableColumnarBatch {

  private var refCount = 1

  /**
   * The number of rows stored in this batch.
   */
  override def numRows(): Int = rowCount

  override lazy val sizeInBytes: Long = handle.compressedSizeInBytes

  /**
   * Set a new spill priority.
   */
  override def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  override def getColumnarBatch(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    handle.materialize()
  }

  override def incRefCount(): SpillableColumnarBatch = {
    if (refCount <= 0) {
      throw new IllegalStateException("Use after free on SpillableColumnarBatchImpl")
    }
    refCount += 1
    this
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    refCount -= 1
    if (refCount == 0) {
      // closing my reference
      handle.close()
    }
    // TODO this is causing problems so we need to look into this
    //  https://github.com/NVIDIA/spark-rapids/issues/10161
    //else if (refCount < 0) {
    //  throw new IllegalStateException("Double free on SpillableColumnarBatchImpl")
    //}
  }

  override def toString: String =
    s"SCCB size:$sizeInBytes, handle:$handle, rows:$rowCount, refCount:$refCount"

  override def dataTypes: Array[DataType] = null
}

class SpillableColumnarBatchFromBufferImpl(
    handle: SpillableColumnarBatchFromBufferHandle,
    rowCount: Int,
    sparkTypes: Array[DataType])
  extends SpillableColumnarBatch {
  private var refCount = 1

  override def dataTypes: Array[DataType] = sparkTypes
  /**
   * The number of rows stored in this batch.
   */
  override def numRows(): Int = rowCount

  override lazy val sizeInBytes: Long = handle.sizeInBytes

  /**
   * Set a new spill priority.
   */
  override def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  override def getColumnarBatch(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    handle.materialize(dataTypes)
  }

  override def incRefCount(): SpillableColumnarBatch = {
    if (refCount <= 0) {
      throw new IllegalStateException("Use after free on SpillableColumnarBatchImpl")
    }
    refCount += 1
    this
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    refCount -= 1
    if (refCount == 0) {
      // closing my reference
      handle.close()
    }
    // TODO this is causing problems so we need to look into this
    //  https://github.com/NVIDIA/spark-rapids/issues/10161
    //else if (refCount < 0) {
    //  throw new IllegalStateException("Double free on SpillableColumnarBatchImpl")
    //}
  }

  override def toString: String =
    s"GpuSCB size:$sizeInBytes, handle:$handle, rows:$rowCount, types:${sparkTypes.toList}," +
      s" refCount:$refCount"
}

class JustRowsHostColumnarBatch(numRows: Int)
  extends SpillableColumnarBatch {
  override def numRows(): Int = numRows
  override def setSpillPriority(priority: Long): Unit = () // NOOP nothing to spill

  def getColumnarBatch(): ColumnarBatch = {
    new ColumnarBatch(Array.empty, numRows)
  }

  override def close(): Unit = () // NOOP nothing to close
  override val sizeInBytes: Long = 0L

  override def dataTypes: Array[DataType] = Array.empty

  // There is no off heap data and close is a noop so just return this
  override def incRefCount(): SpillableColumnarBatch = this

  override def toString: String = s"JustRowsHostSCB size:$sizeInBytes, rows:$numRows"
}

/**
 * The implementation of [[SpillableHostColumnarBatch]] that points to buffers that can be spilled.
 * @note the buffer should be in the cache by the time this is created and this is taking over
 *       ownership of the life cycle of the batch.  So don't call this constructor directly please
 *       use `SpillableHostColumnarBatch.apply` instead.
 */
class SpillableHostColumnarBatchImpl (
    handle: SpillableHostColumnarBatchHandle,
    rowCount: Int,
    sparkTypes: Array[DataType])
  extends SpillableColumnarBatch {
  private var refCount = 1

  override def dataTypes: Array[DataType] = sparkTypes

  /**
   * The number of rows stored in this batch.
   */
  override def numRows(): Int = rowCount

  override lazy val sizeInBytes: Long = handle.approxSizeInBytes

  /**
   * Set a new spill priority.
   */
  override def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  override def getColumnarBatch(): ColumnarBatch = {
    handle.materialize(sparkTypes)
  }

  override def incRefCount(): SpillableColumnarBatch = {
    if (refCount <= 0) {
      throw new IllegalStateException("Use after free on SpillableHostColumnarBatchImpl")
    }
    refCount += 1
    this
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    refCount -= 1
    if (refCount == 0) {
      // closing my reference
      handle.close()
    } else if (refCount < 0) {
      throw new IllegalStateException("Double free on SpillableHostColumnarBatchImpl")
    }
  }

  override def toString: String =
    s"HostSCB size:$sizeInBytes, handle:$handle, rows:$rowCount, types:${sparkTypes.toList}," +
      s" refCount:$refCount"
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
    Cuda.DEFAULT_STREAM.sync()
    val numRows = batch.numRows()
    if (batch.numCols() <= 0) {
      // We consumed it
      batch.close()
      new JustRowsColumnarBatch(numRows)
    } else {
      if (GpuCompressedColumnVector.isBatchCompressed(batch)) {
        new SpillableCompressedColumnarBatchImpl(
          SpillableCompressedColumnarBatchHandle(batch),
          numRows)
      } else if (GpuColumnVectorFromBuffer.isFromBuffer(batch)) {
        new SpillableColumnarBatchFromBufferImpl(
          SpillableColumnarBatchFromBufferHandle(batch),
          numRows,
          GpuColumnVector.extractTypes(batch)
        )
      } else {
        new SpillableColumnarBatchImpl(
          SpillableColumnarBatchHandle(batch),
          numRows,
          GpuColumnVector.extractTypes(batch))
      }
    }
  }

  /**
   * Create a new SpillableColumnarBatch
   * @note This takes over ownership of `ct`, and `ct` should not be used after this.
   * @param ct contiguous table containing the batch GPU data
   * @param sparkTypes array of Spark types describing the data schema
   * @param priority the initial spill priority of this batch
   */
  def apply(
      ct: ContiguousTable,
      sparkTypes: Array[DataType],
      priority: Long): SpillableColumnarBatch = {
    Cuda.DEFAULT_STREAM.sync()
    new SpillableColumnarBatchFromBufferImpl(
      SpillableColumnarBatchFromBufferHandle(ct, sparkTypes),
      ct.getRowCount.toInt,
      sparkTypes)
  }
}

object SpillableHostColumnarBatch {
  /**
   * Create a new SpillableColumnarBatch backed by host columns.
   *
   * @note This takes over ownership of batch, and batch should not be used after this.
   * @param batch         the batch to make spillable
   * @param priority      the initial spill priority of this batch
   */
  def apply(batch: ColumnarBatch, priority: Long): SpillableColumnarBatch = {
    val numRows = batch.numRows()
    if (batch.numCols() <= 0) {
      // We consumed it
      batch.close()
      new JustRowsHostColumnarBatch(numRows)
    } else {
      val types = RapidsHostColumnVector.extractColumns(batch).map(_.dataType())
      val handle = SpillableHostColumnarBatchHandle(batch)
      new SpillableHostColumnarBatchImpl(handle, numRows, types)
    }
  }
}

/**
 * Just like a SpillableColumnarBatch but for buffers.
 */
class SpillableBuffer(
    handle: SpillableDeviceBufferHandle) extends AutoCloseable {

  /**
   * Set a new spill priority.
   */
  def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  /**
   * Use the device buffer.
   */
  def getDeviceBuffer(): DeviceMemoryBuffer = {
    handle.materialize()
  }

  /**
   * Remove the buffer from the cache.
   */
  override def close(): Unit = {
    handle.close()
  }

  override def toString: String = {
    val size = handle.sizeInBytes
    s"SpillableBuffer size:$size, handle:$handle"
  }
}

/**
 * This represents a spillable `HostMemoryBuffer` and adds an interface to access
 * this host buffer at the host layer, unlike `SpillableBuffer` (device)
 * @param handle an object used to refer to this buffer in the spill framework
 * @param length a metadata-only length that is kept in the `SpillableHostBuffer`
 *               instance. Used in cases where the backing host buffer is larger
 *               than the number of usable bytes.
 */
class SpillableHostBuffer(handle: SpillableHostBufferHandle,
                          val length: Long)
    extends AutoCloseable {
  /**
   * Set a new spill priority.
   */
  def setSpillPriority(priority: Long): Unit = {
    // TODO: handle.setSpillPriority(priority)
  }

  /**
   * Remove the buffer from the cache.
   */
  override def close(): Unit = {
    handle.close()
  }

  def getHostBuffer(): HostMemoryBuffer = {
    handle.materialize()
  }

  override def toString: String =
    s"SpillableHostBuffer length:$length, handle:$handle"
}

object SpillableBuffer {

  /**
   * Create a new SpillableBuffer.
   * @note This takes over ownership of buffer, and buffer should not be used after this.
   * @param buffer the buffer to make spillable
   * @param priority the initial spill priority of this buffer
   */
  def apply(
      buffer: DeviceMemoryBuffer,
      priority: Long): SpillableBuffer = {
    Cuda.DEFAULT_STREAM.sync()
    val handle = SpillableDeviceBufferHandle(buffer) // TODO: AB: priority
    new SpillableBuffer(handle)
  }
}

object SpillableHostBuffer {

  /**
   * Create a new SpillableBuffer.
   * @note This takes over ownership of buffer, and buffer should not be used after this.
   * @param length the actual length of the data within the host buffer, which
   *               must be <= than buffer.getLength, otherwise this function throws
   *               and closes `buffer`
   * @param buffer the buffer to make spillable
   * @param priority the initial spill priority of this buffer
   */
  def apply(buffer: HostMemoryBuffer,
            length: Long,
            priority: Long): SpillableHostBuffer = {
    closeOnExcept(buffer) { _ =>
      require(length <= buffer.getLength,
        s"Attempted to add a host spillable with a length ${length} B which is " +
          s"greater than the backing host buffer length ${buffer.getLength} B")
    }
    new SpillableHostBuffer(SpillableHostBufferHandle(buffer), length)
  }
}
