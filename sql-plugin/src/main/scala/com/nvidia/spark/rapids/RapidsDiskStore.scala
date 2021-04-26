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

import java.io.{File, FileOutputStream}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf.{Cuda, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

/** A buffer store using files on the local disks. */
class RapidsDiskStore(
    diskBlockManager: RapidsDiskBlockManager,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
    deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
    extends RapidsBufferStore(StorageTier.DISK, catalog) {
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]

  override protected def createBuffer(incoming: RapidsBuffer, incomingBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(incomingBuffer) { _ =>
      val hostBuffer = incomingBuffer match {
        case h: HostMemoryBuffer => h
        case _ => throw new UnsupportedOperationException("buffer without host memory")
      }
      val id = incoming.id
      val path = if (id.canShareDiskPaths) {
        sharedBufferFiles.computeIfAbsent(id, _ => id.getDiskPath(diskBlockManager))
      } else {
        id.getDiskPath(diskBlockManager)
      }
      val fileOffset = if (id.canShareDiskPaths) {
        // only one writer at a time for now when using shared files
        path.synchronized {
          copyBufferToPath(hostBuffer, path, append = true)
        }
      } else {
        copyBufferToPath(hostBuffer, path, append = false)
      }
      logDebug(s"Spilled to $path $fileOffset:${incoming.size}")
      new this.RapidsDiskBuffer(id, fileOffset, incoming.size, incoming.meta,
        incoming.getSpillPriority, incoming.spillCallback, deviceStorage)
    }
  }

  /** Copy a host buffer to a file, returning the file offset at which the data was written. */
  private def copyBufferToPath(
      buffer: HostMemoryBuffer,
      path: File,
      append: Boolean): Long = {
    val iter = new HostByteBufferIterator(buffer)
    val fos = new FileOutputStream(path, append)
    try {
      val channel = fos.getChannel
      val fileOffset = channel.position
      iter.foreach { bb =>
        while (bb.hasRemaining) {
          channel.write(bb)
        }
      }
      fileOffset
    } finally {
      fos.close()
    }
  }


  class RapidsDiskBuffer(
      id: RapidsBufferId,
      fileOffset: Long,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      spillCallback: RapidsBuffer.SpillCallback,
      deviceStorage: RapidsDeviceMemoryStore)
      extends RapidsBufferBase(
        id, size, meta, spillPriority, spillCallback, deviceStorage = deviceStorage) {
    private[this] var hostBuffer: Option[HostMemoryBuffer] = None

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
}
