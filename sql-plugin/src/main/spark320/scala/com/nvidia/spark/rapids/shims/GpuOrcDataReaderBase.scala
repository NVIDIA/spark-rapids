/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.io.{EOFException, IOException}
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, HostMemoryOutputStream, NoopMetric}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.filecache.FileCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{DataReader, OrcProto, StripeInformation}
import org.apache.orc.impl.DataReaderProperties

abstract class GpuOrcDataReaderBase(
    props: DataReaderProperties,
    conf: Configuration,
    metrics: Map[String, GpuMetric]) extends DataReader {
  protected val filePathString = props.getPath.toString
  protected var file: Option[FSDataInputStream] = None
  protected val compression = props.getCompression
  private val hitMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_HITS)
  private val hitSizeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_HITS_SIZE)
  private val readTimeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_READ_TIME)
  private val missMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_MISSES)
  private val missSizeMetric = getMetric(GpuMetric.FILECACHE_DATA_RANGE_MISSES_SIZE)

  // cache of the last stripe footer that was read and the corresponding stripe info for it
  private var lastStripeFooter: OrcProto.StripeFooter = null
  private var lastStripeFooterInfo: StripeInformation = null

  protected trait BlockLoader {
    /** Load data and potentially populate the filecache, returning the next range after last */
    def loadRemoteBlocks(
        baseOffset: Long,
        first: DiskRangeList,
        last: DiskRangeList,
        data: ByteBuffer): DiskRangeList

    /** Load a single cached block, returning the possibly new disk range node */
    def loadCachedBlock(block: DiskRangeList, channel: SeekableByteChannel): DiskRangeList
  }

  private class HostStreamLoader(out: HostMemoryOutputStream) extends BlockLoader {
    override def loadRemoteBlocks(
        baseOffset: Long,
        first: DiskRangeList,
        last: DiskRangeList,
        data: ByteBuffer): DiskRangeList = {
      var bufferPos = out.getPos
      out.write(data)
      // see if the filecache wants any of this data
      var current = first
      while (current ne last.next) {
        val cacheToken = FileCache.get.startDataRangeCache(filePathString,
          baseOffset + current.getOffset, current.getLength, conf)
        cacheToken.foreach { token =>
          token.complete(out.buffer.slice(bufferPos, current.getLength))
        }
        bufferPos += current.getLength
        current = current.next
      }
      current
    }

    override def loadCachedBlock(
        block: DiskRangeList,
        channel: SeekableByteChannel): DiskRangeList = {
      out.copyFromChannel(channel, block.getLength)
      block
    }
  }

  protected def parseStripeFooter(buf: ByteBuffer, size: Int): OrcProto.StripeFooter

  override def open(): Unit = {
    // File cache may preclude need to open remote file, so open remote file lazily.
  }

  override def readStripeFooter(stripe: StripeInformation): OrcProto.StripeFooter = {
    if (stripe == lastStripeFooterInfo) {
      return lastStripeFooter
    }
    val offset = stripe.getOffset + stripe.getIndexLength + stripe.getDataLength
    val tailLength = stripe.getFooterLength.toInt
    val tailBuf = ByteBuffer.allocate(tailLength)
    val cacheChannel = FileCache.get.getDataRangeChannel(filePathString, offset, tailLength, conf)
    if (cacheChannel.isDefined) {
      withResource(cacheChannel.get) { channel =>
        hitMetric += 1
        hitSizeMetric += tailLength
        readTimeMetric.ns {
          while (tailBuf.hasRemaining) {
            if (channel.read(tailBuf) < 0) {
              throw new EOFException("Unexpected EOF while reading stripe footer")
            }
          }
          tailBuf.flip()
        }
      }
    } else {
      missMetric += 1
      missSizeMetric += tailLength
      ensureFile()
      file.get.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength)
      val cacheToken = FileCache.get.startDataRangeCache(filePathString, offset, tailLength, conf)
      cacheToken.foreach { token =>
        closeOnExcept(HostMemoryBuffer.allocate(tailLength, false)) { hmb =>
          hmb.setBytes(0, tailBuf.array(), tailBuf.arrayOffset(), tailLength)
          token.complete(hmb)
        }
      }
    }
    lastStripeFooter = parseStripeFooter(tailBuf, tailLength)
    lastStripeFooterInfo = stripe
    lastStripeFooter
  }

  override def isTrackingDiskRanges: Boolean = false

  override def releaseBuffer(buffer: ByteBuffer): Unit = {
    throw new IllegalStateException("should not be trying to release buffer")
  }

  def copyFileDataToHostStream(out: HostMemoryOutputStream, ranges: DiskRangeList): Unit = {
    readDiskRanges(ranges, 0, new HostStreamLoader(out))
  }

  override def close(): Unit = {
    file.foreach { f =>
      file = None
      f.close()
    }
  }

  private def getMetric(metricName: String): GpuMetric = metrics.getOrElse(metricName, NoopMetric)

  private def ensureFile(): Unit = {
    if (file.isEmpty) {
      file = Some(props.getFileSystemSupplier.get.open(props.getPath))
    }
  }

  protected def readDiskRanges(
      ranges: DiskRangeList,
      baseOffset: Long,
      loader: BlockLoader): Unit = {
    var current = ranges
    while (current != null) {
      val offset = current.getOffset + baseOffset
      val size = current.getLength
      val cacheChannel = FileCache.get.getDataRangeChannel(filePathString, offset, size, conf)
      if (cacheChannel.isDefined) {
        withResource(cacheChannel.get) { channel =>
          hitMetric += 1
          hitSizeMetric += size
          readTimeMetric.ns {
            current = loader.loadCachedBlock(current, channel)
          }
          current = current.next
        }
      } else {
        current = remoteReadSingle(current, baseOffset, loader)
      }
    }
  }

  /**
   * Performs a single, contiguous remote read of possibly multiple disk ranges,
   * returning the next range in the list to be processed. The ranges are scanned
   * for a large, contiguous block that all needs to be read remotely. The remote
   * block could be cut short by a locally cached range, and in that case the
   * cached range is also read.
   */
  private def remoteReadSingle(
      first: DiskRangeList,
      baseOffset: Long,
      loader: BlockLoader): DiskRangeList = {
    var last = first
    var currentEnd = first.getEnd
    missMetric += 1
    missSizeMetric += last.getLength
    var cachedChannel: Option[SeekableByteChannel] = None
    try {
      while (cachedChannel.isEmpty &&
          last.next != null &&
          last.next.getOffset == currentEnd &&
          last.next.getEnd - first.getOffset <= Int.MaxValue) {
        val offset = baseOffset + last.next.getOffset
        cachedChannel = FileCache.get.getDataRangeChannel(filePathString, offset,
          last.next.getLength, conf)
        if (cachedChannel.isEmpty) {
          last = last.next
          currentEnd = currentEnd.max(last.getEnd)
          missMetric += 1
          missSizeMetric += last.getLength
        }
      }
      assert(currentEnd - first.getOffset <= Int.MaxValue)
      val readSize = (currentEnd - first.getOffset).toInt
      var endRange = remoteRead(loader, baseOffset, first, last, readSize)
      // If the remote read ended because of a cached range, load the cached range.
      cachedChannel.foreach { channel =>
        hitMetric += 1
        hitSizeMetric += last.getLength
        readTimeMetric.ns {
          endRange = loader.loadCachedBlock(endRange, channel)
        }
        endRange = endRange.next
      }
      endRange
    } finally {
      cachedChannel.foreach(_.close)
    }
  }

  /** Read a single, contiguous block of remote data which may span multiple ranges. */
  private def remoteRead(
      loader: BlockLoader,
      baseOffset: Long,
      first: DiskRangeList,
      last: DiskRangeList,
      readSize: Int): DiskRangeList = {
    ensureFile()
    val offset = baseOffset + first.getOffset
    try {
      val buffer = new Array[Byte](readSize)
      file.get.readFully(offset, buffer, 0, buffer.length)
      loader.loadRemoteBlocks(baseOffset, first, last, ByteBuffer.wrap(buffer))
    } catch {
      case e: IOException =>
        throw new IOException(s"Failed to read $filePathString $offset:$readSize", e)
    }
  }

  // [Scala 2.13] This is needed because org.apache.orc.DataReader defines a public clone() method 
  // which should be overidden here as a public member. The Scala 2.13 compiler enforces this now
  // which was a bug in the compiler previously.
  override def clone(): DataReader = {
    super.clone().asInstanceOf[DataReader]
  }
}
