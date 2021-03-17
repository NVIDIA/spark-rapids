/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

/** A buffer store using GPUDirect Storage (GDS). */
class RapidsGdsStore(
    diskBlockManager: RapidsDiskBlockManager,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends RapidsBufferStore(StorageTier.GDS, catalog) with Arm {
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]

  override protected def createBuffer(other: RapidsBuffer, otherBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val deviceBuffer = otherBuffer match {
        case d: DeviceMemoryBuffer => d
        case _ => throw new IllegalStateException("copying from buffer without device memory")
      }
      val id = other.id
      val path = if (id.canShareDiskPaths) {
        sharedBufferFiles.computeIfAbsent(id, _ => id.getDiskPath(diskBlockManager))
      } else {
        id.getDiskPath(diskBlockManager)
      }
      // When sharing files, append to the file; otherwise, write from the beginning.
      val fileOffset = if (id.canShareDiskPaths) {
        // only one writer at a time for now when using shared files
        path.synchronized {
          CuFile.appendDeviceBufferToFile(path, deviceBuffer)
        }
      } else {
        CuFile.writeDeviceBufferToFile(path, 0, deviceBuffer)
        0
      }
      logDebug(s"Spilled to $path $fileOffset:${other.size} via GDS")
      new RapidsGdsBuffer(id, fileOffset, other.size, other.meta, other.getSpillPriority,
        other.spillCallback)
    }
  }

  class RapidsGdsBuffer(
      id: RapidsBufferId,
      val fileOffset: Long,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      spillCallback: RapidsBuffer.SpillCallback)
      extends RapidsBufferBase(id, size, meta, spillPriority, spillCallback) {
    override val storageTier: StorageTier = StorageTier.GDS

    override def getMemoryBuffer: MemoryBuffer = getDeviceMemoryBuffer

    override def materializeMemoryBuffer: MemoryBuffer = {
      val path = if (id.canShareDiskPaths) {
        sharedBufferFiles.get(id)
      } else {
        id.getDiskPath(diskBlockManager)
      }
      closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
        CuFile.readFileToDeviceBuffer(buffer, path, fileOffset)
        logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
        buffer
      }
    }

    override protected def releaseResources(): Unit = {
      // Buffers that share paths must be cleaned up elsewhere
      if (id.canShareDiskPaths) {
        sharedBufferFiles.remove(id)
      } else {
        val path = id.getDiskPath(diskBlockManager)
        if (!path.delete() && path.exists()) {
          logWarning(s"Unable to delete GDS spill path $path")
        }
      }
    }
  }
}
