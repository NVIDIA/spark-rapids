/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "321cdh"}
{"spark": "330cdh"}
{"spark": "332cdh"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import scala.util.control.Breaks._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.filecache.FileCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.io.{DiskRange, DiskRangeList}
import org.apache.orc.{CompressionCodec, OrcFile, OrcProto, StripeInformation, TypeDescription}
import org.apache.orc.impl.{BufferChunk, DataReaderProperties, InStream, OrcCodecPool, OrcIndex, RecordReaderUtils}

class GpuOrcDataReader(
    props: DataReaderProperties,
    conf: Configuration,
    metrics: Map[String, GpuMetric]) extends GpuOrcDataReaderBase(props, conf, metrics) {

  private var codec = OrcCodecPool.getCodec(compression)
  private val bufferSize = props.getBufferSize
  private val typeCount = props.getTypeCount

  /**
   * Loads blocks into ORC BufferChunks for old versions of ORC (1.5.x). The old ORC versions
   * mutate the DiskRangeList nodes to add the data buffers to each node in the list.
   */
  private class BufferChunkLoader(useDirect: Boolean) extends BlockLoader {
    override def loadRemoteBlocks(
        baseOffset: Long,
        first: DiskRangeList,
        last: DiskRangeList,
        data: ByteBuffer): DiskRangeList = {
      var current = first
      val offset = current.getOffset
      val endRange = last.next
      while (current ne endRange) {
        val buffer = if (current eq last) data else data.duplicate()
        buffer.position((current.getOffset - offset).toInt)
        buffer.limit((current.getEnd - offset).toInt)
        current = current.replaceSelfWith(new BufferChunk(buffer, current.getOffset))
        // see if the filecache wants any of this data
        val cacheToken = FileCache.get.startDataRangeCache(filePathString,
          baseOffset + current.getOffset, current.getLength, conf)
        cacheToken.foreach { token =>
          val hmb = closeOnExcept(HostMemoryBuffer.allocate(current.getLength, false)) { hmb =>
            hmb.setBytes(0, buffer.array(),
              buffer.arrayOffset() + buffer.position(), current.getLength)
            hmb
          }
          token.complete(hmb)
        }
        current = current.next
      }
      current
    }

    /**
     * Loads a cached block for old versions of ORC (1.5.x). The old ORC versions
     * mutate the DiskRangeList node to add the data buffer.
     */
    override def loadCachedBlock(
        range: DiskRangeList,
        channel: SeekableByteChannel): DiskRangeList = {
      val buffer = if (useDirect) {
        ByteBuffer.allocateDirect(range.getLength)
      } else {
        ByteBuffer.allocate(range.getLength)
      }
      while (buffer.remaining() > 0) {
        if (channel.read(buffer) < 0) {
          throw new EOFException(s"Unexpected EOF while reading cached block for $filePathString")
        }
      }
      buffer.flip()
      range.replaceSelfWith(new BufferChunk(buffer, range.getOffset))
    }
  }

  override protected def parseStripeFooter(buffer: ByteBuffer, size: Int): OrcProto.StripeFooter = {
    val ranges = new java.util.ArrayList[DiskRange]()
    ranges.add(new BufferChunk(buffer, 0))
    OrcProto.StripeFooter.parseFrom(
      InStream.createCodedInputStream("footer", ranges, size, codec,bufferSize))
  }

  override def getCompressionCodec: CompressionCodec = codec

  override def readFileData(
      ranges: DiskRangeList,
      baseOffset: Long,
      forceDirect: Boolean): DiskRangeList = {
    val prev = if (ranges.prev == null) new DiskRangeList.MutateHelper(ranges) else ranges.prev
    readDiskRanges(ranges, baseOffset, new BufferChunkLoader(forceDirect))
    prev.next
  }

  // Mostly copied from ORC 1.5 DefaultDataReader readRowIndex
  override def readRowIndex(
      stripe: StripeInformation,
      fileSchema: TypeDescription,
      inFooter: OrcProto.StripeFooter,
      ignoreNonUtf8BloomFilter: Boolean,
      included: Array[Boolean],
      inIndexes: Array[OrcProto.RowIndex],
      sargColumns: Array[Boolean],
      version: OrcFile.WriterVersion,
      inBloomFilterKinds: Array[OrcProto.Stream.Kind],
      inBloomFilterIndices: Array[OrcProto.BloomFilterIndex]): OrcIndex = {
    val footer = if (inFooter == null) readStripeFooter(stripe) else inFooter
    val indexes = if (inIndexes == null) new Array[OrcProto.RowIndex](typeCount) else inIndexes
    val bloomFilterKinds = if (inBloomFilterKinds == null) {
      new Array[OrcProto.Stream.Kind](typeCount)
    } else {
      inBloomFilterKinds
    }
    val bloomFilterIndices = if (inBloomFilterIndices == null) {
      new Array[OrcProto.BloomFilterIndex](typeCount)
    } else {
      inBloomFilterIndices
    }
    val ranges = RecordReaderUtils.planIndexReading(fileSchema, footer,
      ignoreNonUtf8BloomFilter, included, sargColumns, version,
      bloomFilterKinds)
    val prev = if (ranges.prev == null) new DiskRangeList.MutateHelper(ranges) else ranges.prev
    readDiskRanges(ranges, stripe.getOffset, new BufferChunkLoader(false))
    var range = prev.next
    var offset: Long = 0
    breakable {
      import scala.collection.JavaConverters._
      for (stream <- footer.getStreamsList.asScala) {
        // advance to find the next range
        while (range != null && range.getEnd <= offset) {
          range = range.next
        }
        // no more ranges, so we are done
        if (range == null) {
          break
        }
        val column = stream.getColumn
        if (stream.hasKind() && range.getOffset <= offset) {
          import OrcProto.Stream.Kind._
          stream.getKind match {
            case ROW_INDEX =>
              if (included == null || included(column)) {
                val bb = range.getData().duplicate()
                bb.position((offset - range.getOffset).toInt)
                bb.limit((bb.position() + stream.getLength).toInt)
                val singleton = new java.util.ArrayList[DiskRange]()
                singleton.add(new BufferChunk(bb, 0))
                indexes(column) = OrcProto.RowIndex.parseFrom(
                  InStream.createCodedInputStream("index", singleton,
                    stream.getLength, codec, bufferSize))
              }
            case BLOOM_FILTER | BLOOM_FILTER_UTF8 =>
              if (sargColumns != null && sargColumns(column)) {
                val bb = range.getData().duplicate()
                bb.position((offset - range.getOffset).toInt)
                bb.limit((bb.position() + stream.getLength).toInt)
                val singleton = new java.util.ArrayList[DiskRange]()
                singleton.add(new BufferChunk(bb, 0))
                bloomFilterIndices(column) = OrcProto.BloomFilterIndex.parseFrom(
                  InStream.createCodedInputStream("bloom_filter", singleton,
                    stream.getLength(), codec, bufferSize))
              }
            case _ =>
          }
        }
        offset += stream.getLength()
      }
    }
    return new OrcIndex(indexes, bloomFilterKinds, bloomFilterIndices)
  }

  override def close(): Unit = {
    if (codec != null) {
      OrcCodecPool.returnCodec(compression, codec)
      codec = null
    }
    super.close()
  }
}

object GpuOrcDataReader {
  // File cache is being used, so we want read ranges that can be cached separately
  val shouldMergeDiskRanges: Boolean = false
}
