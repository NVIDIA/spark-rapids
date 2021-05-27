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

/**
 * A buffer store using GPUDirect Storage (GDS).
 *
 * GDS is more efficient when IO is aligned.
 *
 * An IO is unaligned if one of the following conditions is true:
 * - The file_offset that was issued in cuFileRead/cuFileWrite is not 4K aligned.
 * - The size that was issued in cuFileRead/cuFileWrite is not 4K aligned.
 * - The devPtr_base that was issued in cuFileRead/cuFileWrite is not 4K aligned.
 * - The devPtr_offset that was issued in cuFileRead/cuFileWrite is not 4K aligned.
 *
 * To avoid unaligned IO, when GDS spilling is enabled, the RMM `aligned_resource_adapter` is used
 * so that large buffers above certain size threshold are allocated with 4K aligned base pointer
 * and size.
 *
 * When reading and writing these large buffers through GDS, the size is aligned up to the next 4K
 * boundary. Although the aligned size appears to be out of bound, the extra space needed is held
 * in reserve by the RMM `aligned_resource_adapter`.
 */
class RapidsGdsStore(
    diskBlockManager: RapidsDiskBlockManager,
    batchWriteBufferSize: Long,
    alignedIO: Boolean,
    alignmentThreshold: Long,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends RapidsBufferStore(StorageTier.GDS, catalog) with Arm {
  private[this] val batchSpiller = new BatchSpiller()

  override protected def createBuffer(other: RapidsBuffer, otherBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val deviceBuffer = otherBuffer match {
        case d: DeviceMemoryBuffer => d
        case _ => throw new IllegalStateException("copying from buffer without device memory")
      }
      if (deviceBuffer.getLength < batchWriteBufferSize) {
        batchSpiller.spill(other, deviceBuffer)
      } else {
        singleShotSpill(other, deviceBuffer)
      }
    }
  }

  private def alignBufferSize(buffer: DeviceMemoryBuffer): Long = {
    if (alignedIO) {
      RapidsGdsStore.alignUp(buffer.getLength)
    } else {
      buffer.getLength
    }
  }

  private def alignLargeBufferSize(buffer: DeviceMemoryBuffer): Long = {
    val length = buffer.getLength
    if (alignedIO && length >= alignmentThreshold) {
      RapidsGdsStore.alignUp(length)
    } else {
      length
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

  class RapidsGdsSingleShotBuffer(
      id: RapidsBufferId, path: File, fileOffset: Long, size: Long, meta: TableMeta,
      spillPriority: Long, spillCallback: RapidsBuffer.SpillCallback)
      extends RapidsGdsBuffer(id, size, meta, spillPriority, spillCallback) {

    override def materializeMemoryBuffer: MemoryBuffer = {
      closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
        CuFile.readFileToDeviceMemory(
          buffer.getAddress, alignLargeBufferSize(buffer), path, fileOffset)
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
          stream.sync()
          // TODO: align the reads once https://github.com/NVIDIA/spark-rapids/issues/2492 is
          //  resolved.
          CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
          logDebug(s"Created device buffer for $path ${fileOffset + srcOffset}:$length via GDS")
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

  private def singleShotSpill(other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer)
  : RapidsBufferBase = {
    val id = other.id
    val path = id.getDiskPath(diskBlockManager)
    val alignedSize = alignLargeBufferSize(deviceBuffer)
    // When sharing files, append to the file; otherwise, write from the beginning.
    val fileOffset = if (id.canShareDiskPaths) {
      // only one writer at a time for now when using shared files
      path.synchronized {
        CuFile.appendDeviceMemoryToFile(path, deviceBuffer.getAddress, alignedSize)
      }
    } else {
      CuFile.writeDeviceMemoryToFile(path, 0, deviceBuffer.getAddress, alignedSize)
      0
    }
    logDebug(s"Spilled to $path $fileOffset:${other.size} via GDS")
    new RapidsGdsSingleShotBuffer(id, path, fileOffset, other.size, other.meta,
      other.getSpillPriority, other.spillCallback)
  }

  class BatchSpiller() {
    private[this] val spilledBuffers = new ConcurrentHashMap[File, Set[RapidsBufferId]]
    private[this] val pendingBuffers = ArrayBuffer.empty[RapidsGdsBatchedBuffer]
    private[this] val batchWriteBuffer = CuFileBuffer.allocate(batchWriteBufferSize, true)
    private[this] var currentFile = TempSpillBufferId().getDiskPath(diskBlockManager)
    private[this] var currentOffset = 0L

    def spill(other: RapidsBuffer, deviceBuffer: DeviceMemoryBuffer): RapidsBufferBase =
      this.synchronized {
        val alignedSize = alignBufferSize(deviceBuffer)
        if (alignedSize > batchWriteBufferSize - currentOffset) {
          val path = currentFile.getAbsolutePath
          withResource(new CuFileWriteHandle(path)) { handle =>
            handle.write(batchWriteBuffer, batchWriteBufferSize, 0)
            logDebug(s"Spilled to $path 0:$currentOffset via GDS")
          }
          pendingBuffers.foreach(_.unsetPending())
          pendingBuffers.clear
          currentFile = TempSpillBufferId().getDiskPath(diskBlockManager)
          currentOffset = 0
        }

        batchWriteBuffer.copyFromMemoryBuffer(
          currentOffset, deviceBuffer, 0, deviceBuffer.getLength, Cuda.DEFAULT_STREAM)

        val id = other.id
        addBuffer(currentFile, id)
        val gdsBuffer = new RapidsGdsBatchedBuffer(id, currentFile, currentOffset,
          other.size, other.meta, other.getSpillPriority, other.spillCallback)
        currentOffset += alignedSize
        pendingBuffers += gdsBuffer
        gdsBuffer
      }

    private def copyToBuffer(
        buffer: MemoryBuffer, offset: Long, size: Long, stream: Cuda.Stream): Unit = {
      buffer.copyFromMemoryBuffer(0, batchWriteBuffer, offset, size, stream)
    }

    private def addBuffer(path: File, id: RapidsBufferId): Set[RapidsBufferId] = {
      val updater = new BiFunction[File, Set[RapidsBufferId], Set[RapidsBufferId]] {
        override def apply(key: File, value: Set[RapidsBufferId]): Set[RapidsBufferId] = {
          if (value == null) {
            Set(id)
          } else {
            value + id
          }
        }
      }
      spilledBuffers.compute(path, updater)
    }

    private def removeBuffer(path: File, id: RapidsBufferId): Set[RapidsBufferId] = {
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
      spilledBuffers.computeIfPresent(path, updater)
    }

    class RapidsGdsBatchedBuffer(
        id: RapidsBufferId,
        path: File,
        fileOffset: Long,
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
            Cuda.DEFAULT_STREAM.sync()
            logDebug(s"Created device buffer $size from batch write buffer")
          } else {
            CuFile.readFileToDeviceMemory(
              buffer.getAddress, alignLargeBufferSize(buffer), path, fileOffset)
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
              stream.sync()
              logDebug(s"Created device buffer $size from batch write buffer")
            } else {
              // TODO: switch to async API when it's released, using the passed in CUDA stream.
              stream.sync()
              // TODO: align the reads once https://github.com/NVIDIA/spark-rapids/issues/2492 is
              //  resolved.
              CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
              logDebug(s"Created device buffer for $path ${fileOffset + srcOffset}:$length via GDS")
            }
          case _ => throw new IllegalStateException(
            s"GDS can only copy to device buffer, not ${dst.getClass}")
        }
      }

      /**
       * Mark this buffer as disk based, no longer in device memory.
       */
      def unsetPending(): Unit = this.synchronized {
        isPending = false
      }

      override protected def releaseResources(): Unit = {
        val ids = removeBuffer(path, id)
        if (ids == null) {
          if (!path.delete() && path.exists()) {
            logWarning(s"Unable to delete GDS spill path $path")
          }
        }
      }
    }
  }
}

object RapidsGdsStore {
  val AllocationAlignment = 4096L

  def alignUp(length: Long): Long = {
    (length + AllocationAlignment - 1) & ~(AllocationAlignment - 1)
  }
}
