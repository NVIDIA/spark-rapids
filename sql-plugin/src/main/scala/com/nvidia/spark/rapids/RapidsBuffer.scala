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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

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
  val GDS: StorageTier = Value(3, "GPUDirect Storage")
}

/**
 * ChunkedPacker is an Iterator that uses a cudf::chunked_pack to copy a cuDF `Table`
 * to a target buffer in chunks.
 *
 * Each chunk is sized at most `bounceBuffer.getLength`, and the caller should cudaMemcpy
 * bytes from `bounceBuffer` to a target buffer after each call to `next()`.
 *
 * @note `ChunkedPacker` must be closed by the caller as it has GPU and host resources
 *       associated with it.
 *
 * @param id The RapidsBufferId for this pack operation to be included in the metadata
 * @param table cuDF Table to chunk_pack
 * @param bounceBuffer GPU memory to be used for packing. The buffer should be at least 1MB
 *                     in length.
 */
class ChunkedPacker(
    id: RapidsBufferId,
    table: Table,
    bounceBuffer: DeviceMemoryBuffer)
    extends Iterator[MemoryBuffer]
        with Logging
        with AutoCloseable {

  private var closed: Boolean = false

  private val chunkedPack =
    table.makeChunkedPack(
      bounceBuffer.getLength,
      GpuDeviceManager.chunkedPackMemoryResource)

  private val tableMeta = withResource(chunkedPack.buildMetadata()) { packedMeta =>
    MetaUtils.buildTableMeta(
      id.tableId,
      chunkedPack.getTotalContiguousSize,
      packedMeta.getMetadataDirectBuffer,
      table.getRowCount)
  }

  // take out a lease on the bounce buffer
  bounceBuffer.incRefCount()

  def getTotalContiguousSize: Long = chunkedPack.getTotalContiguousSize

  def getMeta: TableMeta = {
    tableMeta
  }

  override def hasNext: Boolean = {
    !closed && chunkedPack.hasNext
  }

  def next(): MemoryBuffer = {
    val bytesWritten = chunkedPack.next(bounceBuffer)
    // we increment the refcount because the caller has no idea where
    // this memory came from, so it should close it.
    bounceBuffer.slice(0, bytesWritten)
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      val toClose = new ArrayBuffer[AutoCloseable]()
      toClose.append(chunkedPack, bounceBuffer)
      toClose.safeClose()
    }
  }
}

/**
 * This iterator encapsulates a buffer's internal `MemoryBuffer` access
 * for spill reasons. Internally, there are two known implementations:
 * - either this is a "single shot" copy, where the entirety of the `RapidsBuffer` is
 *   already represented as a single contiguous blob of memory, then the expectation
 *   is that this iterator is exhausted with a single call to `next`
 * - or, we have a `RapidsBuffer` that isn't contiguous. This iteration will then
 *   drive a `ChunkedPacker` to pack the `RapidsBuffer`'s table as needed. The
 *   iterator will likely need several calls to `next` to be exhausted.
 *
 * @param buffer `RapidsBuffer` to copy out of its tier.
 */
class RapidsBufferCopyIterator(buffer: RapidsBuffer)
    extends Iterator[MemoryBuffer] with AutoCloseable with Logging {

  private val chunkedPacker: Option[ChunkedPacker] = if (buffer.supportsChunkedPacker) {
    Some(buffer.getChunkedPacker)
  } else {
    None
  }

  def isChunked: Boolean = chunkedPacker.isDefined

  // this is used for the single shot case to flag when `next` is call
  // to satisfy the Iterator interface
  private var singleShotCopyHasNext: Boolean = false
  private var singleShotBuffer: MemoryBuffer = _

  if (!isChunked) {
    singleShotCopyHasNext = true
    singleShotBuffer = buffer.getMemoryBuffer
  }

  override def hasNext: Boolean =
    chunkedPacker.map(_.hasNext).getOrElse(singleShotCopyHasNext)

  override def next(): MemoryBuffer = {
    require(hasNext,
      "next called on exhausted iterator")
    chunkedPacker.map(_.next()).getOrElse {
      singleShotCopyHasNext = false
      singleShotBuffer.slice(0, singleShotBuffer.getLength)
    }
  }

  def getTotalCopySize: Long = {
    chunkedPacker
        .map(_.getTotalContiguousSize)
        .getOrElse(singleShotBuffer.getLength)
  }

  override def close(): Unit = {
    val hasNextBeforeClose = hasNext
    val toClose = new ArrayBuffer[AutoCloseable]()
    toClose.appendAll(chunkedPacker)
    toClose.appendAll(Option(singleShotBuffer))

    toClose.safeClose()
    require(!hasNextBeforeClose,
      "RapidsBufferCopyIterator was closed before exhausting")
  }
}

/** Interface provided by all types of RAPIDS buffers */
trait RapidsBuffer extends AutoCloseable {
  /** The buffer identifier for this buffer. */
  val id: RapidsBufferId

  /**
   * The size of this buffer in bytes in its _current_ store. As the buffer goes through
   * contiguous split (either added as a contiguous table already, or spilled to host),
   * its size changes because contiguous_split adds its own alignment padding.
   *
   * @note Do not use this size to allocate a target buffer to copy, always use `getPackedSize.`
   */
  def getMemoryUsedBytes: Long

  /**
   * The size of this buffer if it has already gone through contiguous_split.
   *
   * @note Use this function when allocating a target buffer for spill or shuffle purposes.
   */
  def getPackedSizeBytes: Long = getMemoryUsedBytes

  /**
   * At spill time, obtain an iterator used to copy this buffer to a different tier.
   */
  def getCopyIterator: RapidsBufferCopyIterator =
    new RapidsBufferCopyIterator(this)

  /** Descriptor for how the memory buffer is formatted */
  def getMeta: TableMeta

  /** The storage tier for this buffer */
  val storageTier: StorageTier

  /**
   * Get the columnar batch within this buffer. The caller must have
   * successfully acquired the buffer beforehand.
   * @param sparkTypes the spark data types the batch should have
   * @see [[addReference]]
   * @note It is the responsibility of the caller to close the batch.
   * @note If the buffer is compressed data then the resulting batch will be built using
   *       `GpuCompressedColumnVector`, and it is the responsibility of the caller to deal
   *       with decompressing the data if necessary.
   */
  def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch

  /**
   * Get the underlying memory buffer. This may be either a HostMemoryBuffer or a DeviceMemoryBuffer
   * depending on where the buffer currently resides.
   * The caller must have successfully acquired the buffer beforehand.
   * @see [[addReference]]
   * @note It is the responsibility of the caller to close the buffer.
   */
  def getMemoryBuffer: MemoryBuffer

  val supportsChunkedPacker: Boolean = false

  def getChunkedPacker: ChunkedPacker = {
    throw new NotImplementedError("not implemented for this store")
  }

  /**
   * Copy the content of this buffer into the specified memory buffer, starting from the given
   * offset.
   *
   * @param srcOffset offset to start copying from.
   * @param dst the memory buffer to copy into.
   * @param dstOffset offset to copy into.
   * @param length number of bytes to copy.
   * @param stream CUDA stream to use
   */
  def copyToMemoryBuffer(
      srcOffset: Long, dst: MemoryBuffer, dstOffset: Long, length: Long, stream: Cuda.Stream)

  /**
   * Get the device memory buffer from the underlying storage. If the buffer currently resides
   * outside of device memory, a new DeviceMemoryBuffer is created with the data copied over.
   * The caller must have successfully acquired the buffer beforehand.
   * @see [[addReference]]
   * @note It is the responsibility of the caller to close the buffer.
   */
  def getDeviceMemoryBuffer: DeviceMemoryBuffer

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
   * @note should only be called from the buffer catalog
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
    val meta: TableMeta) extends RapidsBuffer {

  override def getMemoryUsedBytes: Long = 0L

  override val storageTier: StorageTier = StorageTier.DEVICE

  override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
    val rowCount = meta.rowCount
    val packedMeta = meta.packedMetaAsByteBuffer()
    if (packedMeta != null) {
      withResource(DeviceMemoryBuffer.allocate(0)) { deviceBuffer =>
        withResource(Table.fromPackedTable(meta.packedMetaAsByteBuffer(), deviceBuffer)) { table =>
          GpuColumnVectorFromBuffer.from(table, deviceBuffer, meta, sparkTypes)
        }
      }
    } else {
      // no packed metadata, must be a table with zero columns
      new ColumnarBatch(Array.empty, rowCount.toInt)
    }
  }

  override def free(): Unit = {}

  override def getMemoryBuffer: MemoryBuffer =
    throw new UnsupportedOperationException("degenerate buffer has no memory buffer")

  override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long, length: Long,
      stream: Cuda.Stream): Unit =
    throw new UnsupportedOperationException("degenerate buffer cannot copy to memory buffer")

  override def getDeviceMemoryBuffer: DeviceMemoryBuffer =
    throw new UnsupportedOperationException("degenerate buffer has no device memory buffer")

  override def addReference(): Boolean = true

  override def getSpillPriority: Long = Long.MaxValue

  override def setSpillPriority(priority: Long): Unit = {}

  override def close(): Unit = {}

  /** Descriptor for how the memory buffer is formatted */
  override def getMeta: TableMeta = meta
}
