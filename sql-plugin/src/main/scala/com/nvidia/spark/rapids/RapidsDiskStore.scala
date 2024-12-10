/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import java.io.{File, FileInputStream}
import java.nio.channels.{Channels, FileChannel}
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf.{Cuda, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta
import org.apache.commons.io.IOUtils

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.{GpuTaskMetrics, RapidsDiskBlockManager}
import org.apache.spark.sql.rapids.execution.{SerializedHostTableUtils, TrampolineUtil}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** A buffer store using files on the local disks. */
class RapidsDiskStore(diskBlockManager: RapidsDiskBlockManager)
    extends RapidsBufferStoreWithoutSpill(StorageTier.DISK) {
  private[this] val sharedBufferFiles = new ConcurrentHashMap[RapidsBufferId, File]

  private def reportDiskAllocMetrics(metrics: GpuTaskMetrics): String = {
    val taskId = TaskContext.get().taskAttemptId()
    val totalSize = metrics.getDiskBytesAllocated
    val maxSize = metrics.getMaxDiskBytesAllocated
    s"total size for task $taskId is $totalSize, max size is $maxSize"
  }

  override protected def createBuffer(
      incoming: RapidsBuffer,
      catalog: RapidsBufferCatalog,
      stream: Cuda.Stream): Option[RapidsBufferBase] = {
    // assuming that the disk store gets contiguous buffers
    val id = incoming.id
    val path = if (id.canShareDiskPaths) {
      sharedBufferFiles.computeIfAbsent(id, _ => id.getDiskPath(diskBlockManager))
    } else {
      id.getDiskPath(diskBlockManager)
    }

    val (fileOffset, uncompressedSize, diskLength) = if (id.canShareDiskPaths) {
      // only one writer at a time for now when using shared files
      path.synchronized {
        writeToFile(incoming, path, append = true, stream)
      }
    } else {
      writeToFile(incoming, path, append = false, stream)
    }
    logDebug(s"Spilled to $path $fileOffset:$diskLength")
    val buff = incoming match {
      case _: RapidsHostBatchBuffer =>
        new RapidsDiskColumnarBatch(
          id,
          fileOffset,
          uncompressedSize,
          diskLength,
          incoming.meta,
          incoming.getSpillPriority)

      case _ =>
        new RapidsDiskBuffer(
          id,
          fileOffset,
          uncompressedSize,
          diskLength,
          incoming.meta,
          incoming.getSpillPriority)
    }
    TrampolineUtil.incTaskMetricsDiskBytesSpilled(uncompressedSize)

    val metrics = GpuTaskMetrics.get
    metrics.incDiskBytesAllocated(uncompressedSize)
    logDebug(s"acquiring resources for disk buffer $id of size $uncompressedSize bytes")
    logDebug(reportDiskAllocMetrics(metrics))
    Some(buff)
  }

  /**
   * Copy a host buffer to a file. It leverages [[RapidsSerializerManager]] from
   * [[RapidsDiskBlockManager]] to do compression or encryption if needed.
   *
   * @param incoming the rapid buffer to be written into a file
   * @param path     file path
   * @param append   whether to append or written into the beginning of the file
   * @param stream   cuda stream
   * @return a tuple of file offset, memory byte size and written size on disk. File offset is where
   *         buffer starts in the targeted file path. Memory byte size is the size of byte buffer
   *         occupied in memory before writing to disk. Written size on disk is actual byte size
   *         written to disk.
   */
  private def writeToFile(
      incoming: RapidsBuffer,
      path: File,
      append: Boolean,
      stream: Cuda.Stream): (Long, Long, Long) = {
    incoming match {
      case fileWritable: RapidsBufferChannelWritable =>
        val option = if (append) {
          Array(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        } else {
          Array(StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        }
        var currentPos, writtenBytes = 0L

        GpuTaskMetrics.get.spillToDiskTime {
          withResource(FileChannel.open(path.toPath, option: _*)) { fc =>
            currentPos = fc.position()
            withResource(Channels.newOutputStream(fc)) { os =>
              withResource(diskBlockManager.getSerializerManager()
                .wrapStream(incoming.id, os)) { cos =>
                val outputChannel = Channels.newChannel(cos)
                writtenBytes = fileWritable.writeToChannel(outputChannel, stream)
              }
            }
            (currentPos, writtenBytes, path.length() - currentPos)
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
      uncompressedSize: Long,
      onDiskSizeInBytes: Long,
      meta: TableMeta,
      spillPriority: Long)
      extends RapidsBufferBase(id, meta, spillPriority) {

    // FIXME: Need to be clean up. Tracked in https://github.com/NVIDIA/spark-rapids/issues/9496
    override val memoryUsedBytes: Long = uncompressedSize

    override val storageTier: StorageTier = StorageTier.DISK

    override def getMemoryBuffer: MemoryBuffer = synchronized {
      require(onDiskSizeInBytes > 0,
        s"$this attempted an invalid 0-byte mmap of a file")
      val path = id.getDiskPath(diskBlockManager)
      val serializerManager = diskBlockManager.getSerializerManager()
      val memBuffer = if (serializerManager.isRapidsSpill(id)) {
        // Only go through serializerManager's stream wrapper for spill case
          closeOnExcept(HostAlloc.alloc(uncompressedSize)) {
            decompressed => GpuTaskMetrics.get.readSpillFromDiskTime {
            withResource(FileChannel.open(path.toPath, StandardOpenOption.READ)) { c =>
              c.position(fileOffset)
              withResource(Channels.newInputStream(c)) { compressed =>
                withResource(serializerManager.wrapStream(id, compressed)) { in =>
                  withResource(new HostMemoryOutputStream(decompressed)) { out =>
                    IOUtils.copy(in, out)
                  }
                  decompressed
                }
              }
            }
          }
        }
      } else {
        // Reserved mmap read fashion for UCX shuffle path. Also it's skipping encryption and
        // compression.
        HostMemoryBuffer.mapFile(path, MapMode.READ_WRITE, fileOffset, onDiskSizeInBytes)
      }
      memBuffer
    }

    override def close(): Unit = synchronized {
      super.close()
    }

    override protected def releaseResources(): Unit = {
      logDebug(s"releasing resources for disk buffer $id of size $memoryUsedBytes bytes")
      val metrics = GpuTaskMetrics.get
      metrics.decDiskBytesAllocated(memoryUsedBytes)
      logDebug(reportDiskAllocMetrics(metrics))

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
      uncompressedSize: Long,
      // TODO: remove meta
      meta: TableMeta,
      spillPriority: Long)
    extends RapidsDiskBuffer(
      id, fileOffset, size, uncompressedSize, meta, spillPriority)
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
        withResource(diskBlockManager.getSerializerManager()
          .wrapStream(id, fis)) { fs =>
          val (header, hostBuffer) = SerializedHostTableUtils.readTableHeaderAndBuffer(fs)
          val hostCols = withResource(hostBuffer) { _ =>
            SerializedHostTableUtils.buildHostColumns(header, hostBuffer, sparkTypes)
          }
          new ColumnarBatch(hostCols.toArray, header.getNumRows)
        }
      }
    }
  }
}
