/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.{RapidsDiskBlockManager, TempSpillBufferId}

/** A buffer store using GPUDirect Storage (GDS). */
class RapidsGdsStore(
    diskBlockManager: RapidsDiskBlockManager,
    batchWriteBufferSize: Long)
    extends RapidsBufferStore(StorageTier.GDS) {
  private[this] val batchSpiller = new BatchSpiller()

  override protected def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    // assume that we get 1 buffer
    val otherBuffer = withResource(other.getCopyIterator) { it =>
      it.next()
    }

    withResource(otherBuffer) { _ =>
      val deviceBuffer = otherBuffer match {
        case d: BaseDeviceMemoryBuffer => d
        case _ => throw new IllegalStateException("copying from buffer without device memory")
      }
      if (deviceBuffer.getLength < batchWriteBufferSize) {
        batchSpiller.spill(other, deviceBuffer)
      } else {
        singleShotSpill(other, deviceBuffer)
      }
    }
  }

  override def close(): Unit = {
    super.close()
    batchSpiller.close()
  }

  abstract class RapidsGdsBuffer(
      override val id: RapidsBufferId,
      val size: Long,
      override val meta: TableMeta,
      spillPriority: Long)
      extends RapidsBufferBase(id, meta, spillPriority) {
    override val storageTier: StorageTier = StorageTier.GDS

    override def getMemoryUsedBytes(): Long = size

    override def getMemoryBuffer: MemoryBuffer = getDeviceMemoryBuffer
  }

  class RapidsGdsSingleShotBuffer(
      id: RapidsBufferId, path: File, fileOffset: Long, size: Long, meta: TableMeta,
      spillPriority: Long)
      extends RapidsGdsBuffer(id, size, meta, spillPriority) {

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
        case dmOriginal: BaseDeviceMemoryBuffer =>
          val sliced = dmOriginal.slice(dstOffset, length).asInstanceOf[BaseDeviceMemoryBuffer]
          withResource(sliced) { dm =>
            // TODO: switch to async API when it's released, using the passed in CUDA stream.
            stream.sync()
            CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
            logDebug(s"Created device buffer for $path ${fileOffset + srcOffset}:$length via GDS")
          }
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

  private def singleShotSpill(other: RapidsBuffer, deviceBuffer: BaseDeviceMemoryBuffer)
  : RapidsBufferBase = {
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
    logDebug(s"Spilled to $path $fileOffset:${deviceBuffer.getLength} via GDS")
    new RapidsGdsSingleShotBuffer(
      id,
      path,
      fileOffset,
      deviceBuffer.getLength,
      other.meta,
      other.getSpillPriority)
  }

  class BatchSpiller() extends AutoCloseable {
    private val blockSize = 4096
    private[this] val spilledBuffers = new ConcurrentHashMap[File, Set[RapidsBufferId]]
    private[this] val pendingBuffers = ArrayBuffer.empty[RapidsGdsBatchedBuffer]
    private[this] val batchWriteBuffer = CuFileBuffer.allocate(batchWriteBufferSize, true)
    private[this] var currentFile = TempSpillBufferId().getDiskPath(diskBlockManager)
    private[this] var currentOffset = 0L

    override def close(): Unit = {
      pendingBuffers.safeFree()
      pendingBuffers.clear()
      batchWriteBuffer.close()
    }

    def spill(other: RapidsBuffer, deviceBuffer: BaseDeviceMemoryBuffer): RapidsBufferBase =
      this.synchronized {
        if (deviceBuffer.getLength > batchWriteBufferSize - currentOffset) {
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
        val gdsBuffer = new RapidsGdsBatchedBuffer(
          id,
          currentFile,
          currentOffset,
          deviceBuffer.getLength,
          other.meta,
          other.getSpillPriority)
        currentOffset += alignUp(deviceBuffer.getLength)
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
        var isPending: Boolean = true)
        extends RapidsGdsBuffer(id, size, meta, spillPriority) {

      override def getMemoryUsedBytes(): Long = size

      override def materializeMemoryBuffer: MemoryBuffer = this.synchronized {
        closeOnExcept(DeviceMemoryBuffer.allocate(size)) { buffer =>
          if (isPending) {
            copyToBuffer(buffer, fileOffset, size, Cuda.DEFAULT_STREAM)
            Cuda.DEFAULT_STREAM.sync()
            logDebug(s"Created device buffer $size from batch write buffer")
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
          case dmOriginal: BaseDeviceMemoryBuffer =>
            val sliced = dmOriginal.slice(dstOffset, length).asInstanceOf[BaseDeviceMemoryBuffer]
            withResource(sliced) { dm =>
              if (isPending) {
                copyToBuffer(dm, fileOffset + srcOffset, length, stream)
                stream.sync()
                logDebug(s"Created device buffer $length from batch write buffer")
              } else {
                // TODO: switch to async API when it's released, using the passed in CUDA stream.
                stream.sync()
                CuFile.readFileToDeviceBuffer(dm, path, fileOffset + srcOffset)
                logDebug(s"Created device buffer for $path ${fileOffset + srcOffset}:$length " +
                  s"via GDS")
              }
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