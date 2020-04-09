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

import java.io.File

import ai.rapids.cudf.MemoryBuffer
import ai.rapids.spark.StorageTier.StorageTier
import ai.rapids.spark.format.TableMeta

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
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
   * Get the columnar batch within this buffer.
   * NOTE: It is the responsibility of the caller to close the batch.
   */
  def getColumnarBatch: ColumnarBatch

  /**
   * Get the underlying memory buffer. This may be either a HostMemoryBuffer
   * or a DeviceMemoryBuffer depending on where the buffer currently resides.
   * NOTE: It is the responsibility of the caller to close the buffer.
   */
  def getMemoryBuffer: MemoryBuffer

  /**
   * Add a reference to this buffer.
   * NOTE: The close method must be called for every successfully obtained reference.
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
