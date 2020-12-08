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
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** A buffer store using GPUDirect Storage (GDS). */
class RapidsGdsStore(
    diskBlockManager: RapidsDiskBlockManager,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends RapidsBufferStore("gds", catalog) {
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]

  override def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    val otherBuffer = other.getMemoryBuffer
    try {
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
      val fileOffset = if (id.canShareDiskPaths) {
        // only one writer at a time for now when using shared files
        path.synchronized {
          CuFile.copyDeviceBufferToFile(path, deviceBuffer, true)
        }
      } else {
        CuFile.copyDeviceBufferToFile(path, deviceBuffer, false)
      }
      logDebug(s"Spilled to $path $fileOffset:${other.size} via GDS")
      new RapidsGdsBuffer(id, fileOffset, other.size, other.meta, other.getSpillPriority)
    } finally {
      otherBuffer.close()
    }
  }

  class RapidsGdsBuffer(
      id: RapidsBufferId,
      fileOffset: Long,
      size: Long,
      meta: TableMeta,
      spillPriority: Long) extends RapidsBufferBase(id, size, meta, spillPriority) {
    private[this] var deviceBuffer: Option[DeviceMemoryBuffer] = None

    override val storageTier: StorageTier = StorageTier.GDS

    private def getDeviceBuffer: DeviceMemoryBuffer = synchronized {
      if (deviceBuffer.isEmpty) {
        val path = if (id.canShareDiskPaths) {
          sharedBufferFiles.get(id)
        } else {
          id.getDiskPath(diskBlockManager)
        }
        val buffer = DeviceMemoryBuffer.allocate(size);
        CuFile.copyFileToDeviceBuffer(buffer, path, fileOffset)
        logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
        deviceBuffer = Some(buffer)
      }
      deviceBuffer.foreach(_.incRefCount())
      deviceBuffer.get
    }

    override def getMemoryBuffer: MemoryBuffer = getDeviceBuffer

    override def close(): Unit = synchronized {
      if (refcount == 1) {
        // free the memory mapping since this is the last active reader
        deviceBuffer.foreach { b =>
          logDebug(s"closing device buffer $b created via GDS")
          b.close()
        }
        deviceBuffer = None
      }
      super.close()
    }

    override protected def releaseResources(): Unit = {
      require(deviceBuffer.isEmpty,
        "Releasing a GDS disk buffer with non-empty device buffer")
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

    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      withResource(getDeviceBuffer) { deviceBuffer =>
        columnarBatchFromDeviceBuffer(deviceBuffer, sparkTypes)
      }
    }
  }

}
