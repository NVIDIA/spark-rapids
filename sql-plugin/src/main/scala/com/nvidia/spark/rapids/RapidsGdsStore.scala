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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.{RapidsDiskBlockManager, TempSpillBufferId}

/** A buffer store using GPUDirect Storage (GDS). */
class RapidsGdsStore(
    diskBlockManager: RapidsDiskBlockManager,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends RapidsBufferStore(StorageTier.GDS, catalog) with Arm {
  private val BlockSize = 4096;
  private val BatchWriteBufferSize = 1 << 27; // 128 MiB
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]
  private[this] val batchWriteBuffer = CuFileBuffer.allocate(BatchWriteBufferSize, true)
  private[this] var batchWriteBufferId = TempSpillBufferId()
  private[this] var batchWriteBufferOffset = 0L
  private[this] val pendingBuffers = ArrayBuffer.empty[RapidsGdsBuffer]

  override protected def createBuffer(other: RapidsBuffer, otherBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val deviceBuffer = otherBuffer match {
        case d: DeviceMemoryBuffer => d
        case _ => throw new IllegalStateException("copying from buffer without device memory")
      }
      if (isBatched(deviceBuffer.getLength)) {
        batchSpill(other, deviceBuffer)
      } else {
        singleShotSpill(other, deviceBuffer)
      }
    }
  }

  private def isBatched(length: Long): Boolean = {
    length < BatchWriteBufferSize
  }

  private def singleShotSpill(
      other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase = {
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
    new RapidsGdsBuffer(id, None, fileOffset, other.size, other.meta, other.getSpillPriority,
      other.spillCallback)
  }

  private def batchSpill(
      other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase = this.synchronized {
      if (deviceBuffer.getLength > BatchWriteBufferSize - batchWriteBufferOffset) {
        val path = batchWriteBufferId.getDiskPath(diskBlockManager).getAbsolutePath
        withResource(new CuFileWriteHandle(path)) { handle =>
          handle.write(batchWriteBuffer, batchWriteBufferOffset, 0)
          logDebug(s"Spilled to $path 0:$batchWriteBufferOffset via GDS")
        }
        pendingBuffers.foreach(_.cuFileBuffer = None)
        pendingBuffers.clear
        batchWriteBufferId = TempSpillBufferId()
        batchWriteBufferOffset = 0
      }

      val currentBufferOffset = batchWriteBufferOffset
      batchWriteBuffer.copyFromMemoryBuffer(
        batchWriteBufferOffset, deviceBuffer, 0, deviceBuffer.getLength, Cuda.DEFAULT_STREAM)
      batchWriteBufferOffset += alignUp(deviceBuffer.getLength)

      val id = other.id
      sharedBufferFiles.computeIfAbsent(id, _ => batchWriteBufferId.getDiskPath(diskBlockManager))
      val gdsBuffer = new RapidsGdsBuffer(id, Some(batchWriteBuffer), currentBufferOffset,
        other.size, other.meta, other.getSpillPriority, other.spillCallback)
      pendingBuffers += gdsBuffer
      gdsBuffer
    }

  private def alignUp(length: Long): Long = {
    (length + BlockSize - 1) & ~(BlockSize - 1)
  }

  class RapidsGdsBuffer(
      id: RapidsBufferId,
      var cuFileBuffer: Option[CuFileBuffer],
      val fileOffset: Long,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      spillCallback: RapidsBuffer.SpillCallback)
      extends RapidsBufferBase(id, size, meta, spillPriority, spillCallback) {
    override val storageTier: StorageTier = StorageTier.GDS

    override def getMemoryBuffer: MemoryBuffer = getDeviceMemoryBuffer

    override def materializeMemoryBuffer: MemoryBuffer = {
      val path = if (id.canShareDiskPaths || isBatched(size)) {
        sharedBufferFiles.get(id)
      } else {
        id.getDiskPath(diskBlockManager)
      }
      closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
        if (cuFileBuffer.isEmpty) {
          CuFile.readFileToDeviceBuffer(buffer, path, fileOffset)
          logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
        } else {
          buffer.copyFromMemoryBuffer(0, cuFileBuffer.get, fileOffset, size, Cuda.DEFAULT_STREAM)
        }
        buffer
      }
    }

    override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long,
        length: Long, stream: Cuda.Stream): Unit = {
      val path = if (id.canShareDiskPaths || isBatched(size)) {
        sharedBufferFiles.get(id)
      } else {
        id.getDiskPath(diskBlockManager)
      }
      dst match {
        case dmOriginal: DeviceMemoryBuffer =>
          val dm = dmOriginal.slice(dstOffset, length)
          if (cuFileBuffer.isEmpty) {
            // TODO: switch to async API when it's released, using the passed in CUDA stream.
            CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
            logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
          } else {
            dm.copyFromMemoryBuffer(
              0, cuFileBuffer.get, fileOffset + srcOffset, size, Cuda.DEFAULT_STREAM)
          }
        case _ => throw new IllegalStateException(
          s"GDS can only copy to device buffer, not ${dst.getClass}")
      }
    }

    override protected def releaseResources(): Unit = {
      // Buffers that share paths must be cleaned up elsewhere
      if (id.canShareDiskPaths || isBatched(size)) {
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
