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

package ai.rapids.spark

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, MemoryBuffer, Table}
import ai.rapids.spark.StorageTier.StorageTier
import ai.rapids.spark.format.TableMeta
import com.google.flatbuffers.FlatBufferBuilder

import org.apache.spark.sql.vectorized.ColumnarBatch

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
    val size = contigBuffer.getLength
    val meta = MetaUtils.buildTableMeta(id.tableId, table, contigBuffer)

    logDebug(s"Adding table for: [id=$id, size=$size, meta_id=${meta.bufferMeta().id()}, " +
      s"meta_size=${meta.bufferMeta().actualSize()}, meta_num_cols=${meta.columnMetasLength()}]")

    val buffer = new RapidsDeviceMemoryBuffer(
      id,
      size,
      meta,
      table,
      contigBuffer,
      initialSpillPriority)

    try {
      addBuffer(buffer)
    } catch {
      case t: Throwable =>
        logError(s"Error while adding, freeing the buffer ${buffer.id}: ", t)
        buffer.free()
        throw t
    }
  }

  def addBuffer(
      id: RapidsBufferId,
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long): Unit = {
    logDebug(s"Adding receive side table for: [id=$id, size=${buffer.getLength}, " +
        s"meta_id=${tableMeta.bufferMeta().id()}, " +
        s"meta_size=${tableMeta.bufferMeta().actualSize()}, " +
        s"meta_num_cols=${tableMeta.columnMetasLength()}]")

    val batch = RapidsBufferStore.getBatchFromMeta(buffer, tableMeta) // REFCOUNT 1 + # COLS
    // hold the 1 ref count extra in buffer, it will be removed later in releaseResources
    val table = try {
      GpuColumnVector.from(batch) // batch cols have 2 ref count
    } finally {
      batch.close() // cols should have single references
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

  class RapidsDeviceMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      table: Table,
      contigBuffer: DeviceMemoryBuffer,
      spillPriority: Long) extends RapidsBufferBase(id, size, meta, spillPriority) {
    override val storageTier: StorageTier = StorageTier.DEVICE

    override protected def releaseResources(): Unit = {
      contigBuffer.close()
      table.close()
    }

    override def getMemoryBuffer: MemoryBuffer = contigBuffer.slice(0, contigBuffer.getLength)

    override def getColumnarBatch: ColumnarBatch = {
      GpuColumnVector.from(table) //REFCOUNT ++ of all columns
    }
  }
}
