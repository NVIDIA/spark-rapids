/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import java.util.Optional

import ai.rapids.cudf.{DType, MemoryBuffer}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.{ColumnMeta, TableMeta}

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * An identifier for a RAPIDS buffer that can be automatically spilled between buffer stores.
 * NOTE: Derived classes MUST implement proper hashCode and equals methods, as these objects are
 *       used as keys in hash maps. Scala case classes are recommended.
 */
trait RapidsBufferId {
  val tableId: Int

  /**
   * Indicates whether the buffer may share a spill file with other buffers.
   * If false then the spill file will be automatically removed when the buffer is freed.
   * If true then the spill file will not be automatically removed, and another subsystem needs
   * to be responsible for cleaning up the spill files for those types of buffers.
   */
  val canShareDiskPaths: Boolean = false

  /**
   * Generate a path to a local file that can be used to spill the corresponding buffer to disk.
   * The path must be unique across all buffers unless canShareDiskPaths is true.
   */
  def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File
}

/** Enumeration of the storage tiers */
object StorageTier extends Enumeration {
  type StorageTier = Value
  val DEVICE: StorageTier = Value(0, "device memory")
  val HOST: StorageTier = Value(1, "host memory")
  val DISK: StorageTier = Value(2, "local disk")
}

/** Interface provided by all types of RAPIDS buffers */
trait RapidsBuffer extends AutoCloseable {
  /** The buffer identifier for this buffer. */
  val id: RapidsBufferId

  /** The size of this buffer in bytes. */
  val size: Long

  /** Descriptor for how the memory buffer is formatted */
  val meta: TableMeta

  /** The storage tier for this buffer */
  val storageTier: StorageTier

  /**
   * Get the columnar batch within this buffer. The caller must have
   * successfully acquired the buffer beforehand.
   * @see [[addReference]]
   * @note It is the responsibility of the caller to close the batch.
   * @note If the buffer is compressed data then the resulting batch will be built using
   *       `GpuCompressedColumnVector`, and it is the responsibility of the caller to deal
   *       with decompressing the data if necessary.
   */
  def getColumnarBatch: ColumnarBatch

  /**
   * Get the underlying memory buffer. This may be either a HostMemoryBuffer
   * or a DeviceMemoryBuffer depending on where the buffer currently resides.
   * The caller must have successfully acquired the buffer beforehand.
   * @see [[addReference]]
   * @note It is the responsibility of the caller to close the buffer.
   */
  def getMemoryBuffer: MemoryBuffer

  /**
   * Try to add a reference to this buffer to acquire it.
   * @note The close method must be called for every successfully obtained reference.
   * @return true if the reference was added or false if this buffer is no longer valid
   */
  def addReference(): Boolean

  /**
   * Schedule the release of the buffer's underlying resources.
   * Subsequent attempts to acquire the buffer will fail. As soon as the
   * buffer has no outstanding references, the resources will be released.
   * <p>
   * This is separate from the close method which does not normally release
   * resources. close will only release resources if called as the last
   * outstanding reference and the buffer was previously marked as freed.
   */
  def free(): Unit

  /**
   * Get the spill priority value for this buffer. Lower values are higher
   * priority for spilling, meaning buffers with lower values will be
   * preferred for spilling over buffers with a higher value.
   */
  def getSpillPriority: Long

  /**
   * Set the spill priority for this buffer. Lower values are higher priority
   * for spilling, meaning buffers with lower values will be preferred for
   * spilling over buffers with a higher value.
   * @param priority new priority value for this buffer
   */
  def setSpillPriority(priority: Long): Unit
}

/**
 * A buffer with no corresponding device data (zero rows or columns).
 * These buffers are not tracked in buffer stores since they have no
 * device memory. They are only tracked in the catalog and provide
 * a representative `ColumnarBatch` but cannot provide a
 * `MemoryBuffer`.
 * @param id buffer ID to associate with the buffer
 * @param meta schema metadata
 */
sealed class DegenerateRapidsBuffer(
    override val id: RapidsBufferId,
    override val meta: TableMeta) extends RapidsBuffer {
  override val size: Long = 0L
  override val storageTier: StorageTier = StorageTier.DEVICE

  override def getColumnarBatch: ColumnarBatch = {
    val rowCount = meta.rowCount
    val nullCount = Optional.of(java.lang.Long.valueOf(0))
    val columnMeta = new ColumnMeta
    val columns = new Array[ColumnVector](meta.columnMetasLength)
    (0 until meta.columnMetasLength).foreach { i =>
      meta.columnMetas(columnMeta, i)
      assert(columnMeta.childrenLength == 0, "child columns are not yet supported")
      val dtype = DType.fromNative(columnMeta.dtype)
      columns(i) = GpuColumnVector.from(new ai.rapids.cudf.ColumnVector(
        dtype, rowCount, nullCount, null, null, null))
    }
    new ColumnarBatch(columns, rowCount.toInt)
  }

  override def free(): Unit = {}

  override def getMemoryBuffer: MemoryBuffer =
    throw new UnsupportedOperationException("degenerate buffer has no memory buffer")

  override def addReference(): Boolean = true

  override def getSpillPriority: Long = Long.MaxValue

  override def setSpillPriority(priority: Long): Unit = {}

  override def close(): Unit = {}
}
