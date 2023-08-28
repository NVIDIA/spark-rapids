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

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf.{Cuda, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.rapids.execution.SerializedHostTableUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** A buffer store using files on the local disks. */
class RapidsDiskStore(diskBlockManager: RapidsDiskBlockManager)
    extends RapidsBufferStore(StorageTier.DISK) {
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]

  override protected def createBuffer(
      incoming: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    // assuming that the disk store gets contiguous buffers
    val id = incoming.id
    val path = if (id.canShareDiskPaths) {
      sharedBufferFiles.computeIfAbsent(id, _ => id.getDiskPath(diskBlockManager))
    } else {
      id.getDiskPath(diskBlockManager)
    }

    val (fileOffset, diskLength) = if (id.canShareDiskPaths) {
      // only one writer at a time for now when using shared files
      path.synchronized {
        writeToFile(incoming, path, append = true)
      }
    } else {
      writeToFile(incoming, path, append = false)
    }

    logDebug(s"Spilled to $path $fileOffset:$diskLength")
    incoming match {
      case _: RapidsHostBatchBuffer =>
        new RapidsDiskColumnarBatch(
          id,
          fileOffset,
          diskLength,
          incoming.meta,
          incoming.getSpillPriority)

      case _ =>
        new RapidsDiskBuffer(
          id,
          fileOffset,
          diskLength,
          incoming.meta,
          incoming.getSpillPriority)
    }
  }

  /** Copy a host buffer to a file, returning the file offset at which the data was written. */
  private def writeToFile(
      incoming: RapidsBuffer,
      path: File,
      append: Boolean): (Long, Long) = {
    incoming match {
      case fileWritable: RapidsBufferChannelWritable =>
        withResource(new FileOutputStream(path, append)) { fos =>
          withResource(fos.getChannel) { outputChannel =>
            val startOffset = outputChannel.position()
            val writtenBytes = fileWritable.writeToChannel(outputChannel)
            (startOffset, writtenBytes)
          }
        }
      case other =>
        throw new IllegalStateException(
          s"Unable to write $other to file")
    }
  }

  /**
   * A RapidsDiskBuffer that is mean to represent device-bound memory. This
   * buffer can produce a device-backed ColumnarBatch.
   */
  class RapidsDiskBuffer(
      id: RapidsBufferId,
      fileOffset: Long,
      size: Long,
      meta: TableMeta,
      spillPriority: Long)
      extends RapidsBufferBase(
        id, meta, spillPriority) {
    private[this] var hostBuffer: Option[HostMemoryBuffer] = None

    override def getMemoryUsedBytes(): Long = size

    override val storageTier: StorageTier = StorageTier.DISK

    override def getMemoryBuffer: MemoryBuffer = synchronized {
      if (hostBuffer.isEmpty) {
        val path = id.getDiskPath(diskBlockManager)
        val mappedBuffer = HostMemoryBuffer.mapFile(path, MapMode.READ_WRITE,
          fileOffset, size)
        logDebug(s"Created mmap buffer for $path $fileOffset:$size")
        hostBuffer = Some(mappedBuffer)
      }
      hostBuffer.foreach(_.incRefCount())
      hostBuffer.get
    }

    override def close(): Unit = synchronized {
      if (refcount == 1) {
        // free the memory mapping since this is the last active reader
        hostBuffer.foreach { b =>
          logDebug(s"closing mmap buffer $b")
          b.close()
        }
        hostBuffer = None
      }
      super.close()
    }

    override protected def releaseResources(): Unit = {
      require(hostBuffer.isEmpty,
        "Releasing a disk buffer with non-empty host buffer")
      // Buffers that share paths must be cleaned up elsewhere
      if (id.canShareDiskPaths) {
        sharedBufferFiles.remove(id)
      } else {
        val path = id.getDiskPath(diskBlockManager)
        if (!path.delete() && path.exists()) {
          logWarning(s"Unable to delete spill path $path")
        }
      }
    }
  }

  /**
   * A RapidsDiskBuffer that should remain in the host, producing host-backed
   * ColumnarBatch if the caller invokes getHostColumnarBatch, but not producing
   * anything on the device.
   */
  class RapidsDiskColumnarBatch(
      id: RapidsBufferId,
      fileOffset: Long,
      size: Long,
      // TODO: remove meta
      meta: TableMeta,
      spillPriority: Long)
    extends RapidsDiskBuffer(
      id, fileOffset, size, meta, spillPriority)
        with RapidsHostBatchBuffer {

    override def getMemoryBuffer: MemoryBuffer =
      throw new IllegalStateException(
        "Called getMemoryBuffer on a disk buffer that needs deserialization")

    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch =
      throw new IllegalStateException(
        "Called getColumnarBatch on a disk buffer that needs deserialization")

    override def getHostColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      require(fileOffset == 0,
        "Attempted to obtain a HostColumnarBatch from a spilled RapidsBuffer that is sharing " +
          "paths on disk")
      val path = id.getDiskPath(diskBlockManager)
      withResource(new FileInputStream(path)) { fis =>
        val (header, hostBuffer) = SerializedHostTableUtils.readTableHeaderAndBuffer(fis)
        val hostCols = closeOnExcept(hostBuffer) { _ =>
          SerializedHostTableUtils.buildHostColumns(header, hostBuffer, sparkTypes)
        }
        new ColumnarBatch(hostCols.toArray, header.getNumRows)
      }
    }
  }
}
