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
    batchWriteBufferSize: Long,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends RapidsBufferStore(StorageTier.GDS, catalog) with Arm {
  private[this] val singleShotSpiller = new SingleShotSpiller()
  private[this] val batchSpiller = new BatchSpiller()

  override protected def createBuffer(
      other: RapidsBuffer, otherBuffer: MemoryBuffer, stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val deviceBuffer = otherBuffer match {
        case d: DeviceMemoryBuffer => d
        case _ => throw new IllegalStateException("copying from buffer without device memory")
      }
      if (deviceBuffer.getLength < batchWriteBufferSize) {
        batchSpiller.spill(other, deviceBuffer)
      } else {
        singleShotSpiller.spill(other, deviceBuffer)
      }
    }
  }

  abstract class RapidsGdsBuffer(
      override val id: RapidsBufferId,
      override val size: Long,
      override val meta: TableMeta,
      spillPriority: Long,
      override val spillCallback: RapidsBuffer.SpillCallback)
      extends RapidsBufferBase(id, size, meta, spillPriority, spillCallback) {
    override val storageTier: StorageTier = StorageTier.GDS

    override def getMemoryBuffer: MemoryBuffer = getDeviceMemoryBuffer
  }

  class SingleShotSpiller() {

    def spill(other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase = {
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
      new RapidsGdsSingleShotBuffer(id, path, fileOffset, other.size, other.meta,
        other.getSpillPriority, other.spillCallback)
    }

    class RapidsGdsSingleShotBuffer(
        id: RapidsBufferId, path: File, fileOffset: Long, size: Long, meta: TableMeta,
        spillPriority: Long, spillCallback: RapidsBuffer.SpillCallback)
        extends RapidsGdsBuffer(id, size, meta, spillPriority, spillCallback) {

      override def materializeMemoryBuffer: MemoryBuffer = {
        closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
          CuFile.readFileToDeviceBuffer(buffer, path, fileOffset)
          logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
          buffer
        }
      }

      override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long,
          length: Long, stream: Cuda.Stream): Unit = {
        dst match {
          case dmOriginal: DeviceMemoryBuffer =>
            val dm = dmOriginal.slice(dstOffset, length)
            // TODO: switch to async API when it's released, using the passed in CUDA stream.
            CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
            logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
          case _ => throw new IllegalStateException(
            s"GDS can only copy to device buffer, not ${dst.getClass}")
        }
      }

      override protected def releaseResources(): Unit = {
        if (id.canShareDiskPaths) {
          // Buffers that share paths must be cleaned up elsewhere
        } else {
          if (!path.delete() && path.exists()) {
            logWarning(s"Unable to delete GDS spill path $path")
          }
        }
      }
    }
  }

  class BatchSpiller() {
    private val blockSize = 4096
    private[this] val batchedBuffers = new ConcurrentHashMap[File, Set[RapidsBufferId]]
    private[this] val batchWriteBuffer = CuFileBuffer.allocate(batchWriteBufferSize, true)
    private[this] var batchWriteFile = TempSpillBufferId().getDiskPath(diskBlockManager)
    private[this] var batchWriteBufferOffset = 0L
    private[this] val pendingBuffers = ArrayBuffer.empty[RapidsGdsBatchedBuffer]

    def spill(other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase =
      this.synchronized {
        if (deviceBuffer.getLength > batchWriteBufferSize - batchWriteBufferOffset) {
          val path = batchWriteFile.getAbsolutePath
          withResource(new CuFileWriteHandle(path)) { handle =>
            handle.write(batchWriteBuffer, batchWriteBufferOffset, 0)
            logDebug(s"Spilled to $path 0:$batchWriteBufferOffset via GDS")
          }
          pendingBuffers.foreach(_.flush())
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
        val gdsBuffer = new RapidsGdsBatchedBuffer(id, batchWriteFile, currentBufferOffset,
          other.size, other.meta, other.getSpillPriority, other.spillCallback)
        pendingBuffers += gdsBuffer
        gdsBuffer
      }

    private def alignUp(length: Long): Long = {
      (length + blockSize - 1) & ~(blockSize - 1)
    }

    private def copyToBuffer(
        buffer: MemoryBuffer, offset: Long, size: Long, stream: Cuda.Stream): Unit = {
      buffer.copyFromMemoryBuffer(0, batchWriteBuffer, offset, size, stream)
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
          val newValue = value - id
          if (newValue.isEmpty) {
            null
          } else {
            newValue
          }
        }
      }
      batchedBuffers.computeIfPresent(path, updater)
    }

    class RapidsGdsBatchedBuffer(
        id: RapidsBufferId,
        path: File,
        val fileOffset: Long,
        size: Long,
        meta: TableMeta,
        spillPriority: Long,
        spillCallback: RapidsBuffer.SpillCallback,
        var isPending: Boolean = true)
        extends RapidsGdsBuffer(id, size, meta, spillPriority, spillCallback) {

      override def materializeMemoryBuffer: MemoryBuffer = this.synchronized {
        closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
          if (isPending) {
            copyToBuffer(buffer, fileOffset, size, Cuda.DEFAULT_STREAM)
          } else {
            CuFile.readFileToDeviceBuffer(buffer, path, fileOffset)
            logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
          }
          buffer
        }
      }

      override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long,
          length: Long, stream: Cuda.Stream): Unit = this.synchronized {
        dst match {
          case dmOriginal: DeviceMemoryBuffer =>
            val dm = dmOriginal.slice(dstOffset, length)
            if (isPending) {
              copyToBuffer(dm, fileOffset + srcOffset, size, stream)
            } else {
              // TODO: switch to async API when it's released, using the passed in CUDA stream.
              CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
              logDebug(s"Created device buffer for $path $fileOffset:$size via GDS")
            }
          case _ => throw new IllegalStateException(
            s"GDS can only copy to device buffer, not ${dst.getClass}")
        }
      }

      /**
       * Mark this buffer as disk based, no longer in device memory.
       */
      def flush(): Unit = this.synchronized {
        isPending = false
      }

      override protected def releaseResources(): Unit = {
        val ids = removeBatchedBuffer(path, id)
        if (ids == null) {
          if (!path.delete() && path.exists()) {
            logWarning(s"Unable to delete GDS spill path $path")
          }
        }
      }
    }
  }
}