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
import java.util.function.BiFunction

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
  private val blockSize = 4096
  private val batchWriteBufferSize = 1 << 27 // 128 MiB
  private[this] val batchedBuffers = new ConcurrentHashMap[File, Set[RapidsBufferId]]
  private[this] val batchWriteBuffer = CuFileBuffer.allocate(batchWriteBufferSize, true)
  private[this] var batchWriteFile = TempSpillBufferId().getDiskPath(diskBlockManager)
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
    length < batchWriteBufferSize
  }

  private def singleShotSpill(
      other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase = {
    val id = other.id
    val path = id.getDiskPath(diskBlockManager)
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
    new RapidsGdsBuffer(id, path, None, fileOffset, other.size, other.meta, other.getSpillPriority,
      other.spillCallback)
  }

  private def batchSpill(other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase =
    this.synchronized {
      if (deviceBuffer.getLength > batchWriteBufferSize - batchWriteBufferOffset) {
        val path = batchWriteFile.getAbsolutePath
        withResource(new CuFileWriteHandle(path)) { handle =>
          handle.write(batchWriteBuffer, batchWriteBufferOffset, 0)
          logDebug(s"Spilled to $path 0:$batchWriteBufferOffset via GDS")
        }
        pendingBuffers.foreach(_.cuFileBuffer = None)
        pendingBuffers.clear
        batchWriteFile = TempSpillBufferId().getDiskPath(diskBlockManager)
        batchWriteBufferOffset = 0
      }

      val currentBufferOffset = batchWriteBufferOffset
      batchWriteBuffer.copyFromMemoryBuffer(
        batchWriteBufferOffset, deviceBuffer, 0, deviceBuffer.getLength, Cuda.DEFAULT_STREAM)
      batchWriteBufferOffset += alignUp(deviceBuffer.getLength)

      val id = other.id
      addBatchedBuffer(batchWriteFile, id)
      val gdsBuffer = new RapidsGdsBuffer(id, batchWriteFile, Some(batchWriteBuffer),
        currentBufferOffset, other.size, other.meta, other.getSpillPriority, other.spillCallback)
      pendingBuffers += gdsBuffer
      gdsBuffer
    }

  private def alignUp(length: Long): Long = {
    (length + blockSize - 1) & ~(blockSize - 1)
  }

  private def addBatchedBuffer(path: File, id: RapidsBufferId): Set[RapidsBufferId] = {
    val updater = new BiFunction[File, Set[RapidsBufferId], Set[RapidsBufferId]] {
      override def apply(key: File, value: Set[RapidsBufferId]): Set[RapidsBufferId] = {
        if (value == null) {
          Set(id)
        } else {
          value + id
        }
      }
    }
    batchedBuffers.compute(path, updater)
  }

  private def removeBatchedBuffer(path: File, id: RapidsBufferId): Set[RapidsBufferId] = {
    val updater = new BiFunction[File, Set[RapidsBufferId], Set[RapidsBufferId]] {
      override def apply(key: File, value: Set[RapidsBufferId]): Set[RapidsBufferId] = {
        value - id
      }
    }
    batchedBuffers.computeIfPresent(path, updater)
  }

  class RapidsGdsBuffer(
      id: RapidsBufferId,
      path: File,
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
      if (isBatched(size)) {
        val ids = removeBatchedBuffer(path, id)
        if (ids.isEmpty) {
          deletePath()
        }
      } else if (id.canShareDiskPaths) {
        // Buffers that share paths must be cleaned up elsewhere
      } else {
        deletePath()
      }
    }

    private def deletePath(): Unit = {
      if (!path.delete() && path.exists()) {
        logWarning(s"Unable to delete GDS spill path $path")
      }
    }
  }
}
