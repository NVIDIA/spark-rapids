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

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Buffer storage using device memory.
 * @param catalog catalog to register this store
 */
class RapidsDeviceMemoryStore(
    catalog: RapidsBufferCatalog) extends RapidsBufferStore("GPU", catalog) {
  override protected def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    throw new IllegalStateException("should not be spilling to device memory")
  }

  /**
   * Adds a contiguous table to the device storage, taking ownership of the table.
   * @param id buffer ID to associate with this buffer
   * @param table cudf table based from the contiguous buffer
   * @param contigBuffer device memory buffer backing the table
   * @param initialSpillPriority starting spill priority value for the buffer
   */
  def addTable(
      id: RapidsBufferId,
      table: Table,
      contigBuffer: DeviceMemoryBuffer,
      initialSpillPriority: Long): Unit = {
    val buffer = uncompressedBufferFromTable(id, table, contigBuffer, initialSpillPriority)
    try {
      logDebug(s"Adding table for: [id=$id, size=${buffer.size}, " +
          s"uncompressed=${buffer.meta.bufferMeta.uncompressedSize}, " +
          s"meta_id=${buffer.meta.bufferMeta.id}, meta_size=${buffer.meta.bufferMeta.size}, " +
          s"meta_num_cols=${buffer.meta.columnMetasLength}]")
      addBuffer(buffer)
    } catch {
      case t: Throwable =>
        logError(s"Error while adding, freeing the buffer ${buffer.id}: ", t)
        buffer.free()
        throw t
    }
  }

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   * @param id buffer ID to associate with this buffer
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   */
  def addBuffer(
      id: RapidsBufferId,
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long): Unit = {
    logDebug(s"Adding receive side table for: [id=$id, size=${buffer.getLength}, " +
        s"meta_id=${tableMeta.bufferMeta.id}, " +
        s"meta_size=${tableMeta.bufferMeta.size}, " +
        s"meta_num_cols=${tableMeta.columnMetasLength}]")

    val table = if (tableMeta.bufferMeta.codecBufferDescrsLength() > 0) {
      // buffer is compressed so there is no Table.
      None
    } else {
      val batch = MetaUtils.getBatchFromMeta(buffer, tableMeta) // REFCOUNT 1 + # COLS
      // hold the 1 ref count extra in buffer, it will be removed later in releaseResources
      try {
        Some(GpuColumnVector.from(batch)) // batch cols have 2 ref count
      } finally {
        batch.close() // cols should have single references
      }
    }

    val buff = new RapidsDeviceMemoryBuffer(
      id,
      buffer.getLength,
      tableMeta,
      table,
      buffer,
      initialSpillPriority)

    addBuffer(buff)
  }

  private def uncompressedBufferFromTable(
      id: RapidsBufferId,
      table: Table,
      contigBuffer: DeviceMemoryBuffer,
      initialSpillPriority: Long): RapidsDeviceMemoryBuffer = {
    val size = contigBuffer.getLength
    val meta = MetaUtils.buildTableMeta(id.tableId, table, contigBuffer)
    new RapidsDeviceMemoryBuffer(
      id,
      size,
      meta,
      Some(table),
      contigBuffer,
      initialSpillPriority)
  }

  class RapidsDeviceMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      table: Option[Table],
      contigBuffer: DeviceMemoryBuffer,
      spillPriority: Long) extends RapidsBufferBase(id, size, meta, spillPriority) {
    require(table.isDefined || meta.bufferMeta.codecBufferDescrsLength() > 0)

    override val storageTier: StorageTier = StorageTier.DEVICE

    override protected def releaseResources(): Unit = {
      contigBuffer.close()
      table.foreach(_.close())
    }

    override def getMemoryBuffer: MemoryBuffer = contigBuffer.slice(0, contigBuffer.getLength)

    override def getColumnarBatch: ColumnarBatch = {
      if (table.isDefined) {
        GpuColumnVector.from(table.get) //REFCOUNT ++ of all columns
      } else {
        columnarBatchFromDeviceBuffer(contigBuffer)
      }
    }
  }
}
