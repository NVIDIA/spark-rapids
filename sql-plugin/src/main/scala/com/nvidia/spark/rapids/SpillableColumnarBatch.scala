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

import org.apache.spark.sql.rapids.TempSpillBufferId
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A class that holds a ColumnarBatch that can be spilled
 */
class SpillableColumnarBatch private (id: TempSpillBufferId, rowCount: Int)
    extends Arm with AutoCloseable {
  private var closed = false

  /**
   * The number of rows stored in this batch.
   */
  def numRows(): Int = rowCount

  /**
   * The ID that this is stored under.
   * @note Use with caution because if this has been closed the id is no longer valid.
   */
  def spillId: TempSpillBufferId = id

  /**
   * Set a new spill priority.
   */
  def setSpillPriority(priority: Long): Unit = {
    withResource(RapidsBufferCatalog.acquireBuffer(id)) { rapidsBuffer =>
      rapidsBuffer.setSpillPriority(priority)
    }
  }

  /**
   * Get the columnar batch.
   * @note It is the responsibility of the caller to close the batch.
   * @note If the buffer is compressed data then the resulting batch will be built using
   *       `GpuCompressedColumnVector`, and it is the responsibility of the caller to deal
   *       with decompressing the data if necessary.
   */
  def getColumnarBatch(): ColumnarBatch = {
    withResource(RapidsBufferCatalog.acquireBuffer(id)) { rapidsBuffer =>
      rapidsBuffer.getColumnarBatch
    }
  }

  /**
   * Remove the `ColumnarBatch` from the cache.
   */
  override def close(): Unit = {
    if (!closed) {
      withResource(RapidsBufferCatalog.acquireBuffer(id)) { rapidsBuffer =>
        rapidsBuffer.free()
      }
      closed = true
    }
  }
}

object SpillableColumnarBatch {
  /**
   * Create a new SpillableColumnarBatch.
   * @note this takes over ownership of batch, and batch should not be used after this unless
   *       you have incremented the reference count on it beforehand.
   */
  def apply(batch: ColumnarBatch, priority: Long): SpillableColumnarBatch = {
    val numRows = batch.numRows()
    val id = TempSpillBufferId()
    RapidsBufferCatalog.addBatch(id, batch, priority)
    new SpillableColumnarBatch(id, numRows)
  }
}