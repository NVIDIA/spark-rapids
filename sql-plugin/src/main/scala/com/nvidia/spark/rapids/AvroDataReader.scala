/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.io.{InputStream, IOException}
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileConstants, SeekableInput}
import org.apache.avro.io.{BinaryData, BinaryDecoder, DecoderFactory}
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}

private class SeekableInputStream(in: SeekableInput) extends InputStream with SeekableInput {
  var oneByte = new Array[Byte](1)

  override def read(): Int = {
    val n = read(oneByte, 0, 1)
    if (n == 1) return oneByte(0) & 0xff else return n
  }

  override def read(b: Array[Byte]): Int = in.read(b, 0, b.length)

  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)

  override def seek(p: Long): Unit = {
    if (p < 0) throw new IOException("Illegal seek: " + p)
    in.seek(p)
  }

  override def tell(): Long = in.tell()

  override def length(): Long = in.length()

  override def close(): Unit = {
    in.close()
    super.close()
  }

  override def available(): Int = {
    val remaining = in.length() - in.tell()
    if (remaining > Int.MaxValue) Int.MaxValue else remaining.toInt
  }
}

/**
 * The header information of an Avro file.
 */
class Header private[rapids] {
  private[rapids] val meta = mutable.Map[String, Array[Byte]]()
  private[rapids] val sync = new Array[Byte](DataFileConstants.SYNC_SIZE)
  private[rapids] var headerSize: Option[Long] = None

  def firstBlockStart: Long = headerSize.getOrElse {
    val out = new CountingOutputStream(NullOutputStream.NULL_OUTPUT_STREAM)
    AvroFileWriter(out).writeHeader(this)
    val newSize = out.getByteCount
    headerSize = Some(newSize)
    newSize
  }

  @transient
  lazy val schema: Schema = {
    getMetaString(DataFileConstants.SCHEMA)
      .map(s => new Schema.Parser().setValidateDefaults(false).setValidate(false).parse(s))
      .orNull
  }

  private def getMetaString(key: String): Option[String] = {
    meta.get(key).map(new String(_, StandardCharsets.UTF_8))
  }
}

object Header {
  /**
   * Merge the metadata of the given headers.
   * Note: It does not check the compatibility of the headers.
   * @param headers whose metadata to be merged.
   * @return the first header but having the new merged metadata, or
   *         None if the input is empty.
   */
  def mergeMetadata(headers: Seq[Header]): Option[Header] = {
    if (headers.isEmpty) {
      None
    } else if (headers.size == 1) {
      Some(headers.head)
    } else {
      val mergedHeader = headers.reduce { (merged, h) =>
        merged.meta ++= h.meta
        merged
      }
      // need to re-compute the header size
      mergedHeader.headerSize = None
      Some(mergedHeader)
    }
  }

  /** Test whether the two headers have the same sync marker */
  def hasSameSync(h1: Header, h2: Header): Boolean = h1.sync.sameElements(h2.sync)

  /**
   * Test whether the two headers have conflicts in the metadata.
   * A conflict means a key exists in both of the two headers' metadata,
   * and maps to different values.
   */
  def hasConflictInMetadata(h1: Header, h2: Header): Boolean = h1.meta.exists {
    case (k, v) => h2.meta.contains(k) && !h2.meta.get(k).get.sameElements(v)
  }
}

/**
 * The each Avro block information
 *
 * @param blockStart  the start of block
 * @param blockLength the whole block length = the size between two sync buffers + sync buffer
 * @param blockSize   the block data size
 * @param count       how many entries in this block
 */
case class BlockInfo(blockStart: Long, blockLength: Long, blockDataSize: Long, count: Long)

/**
 * AvroDataFileReader parses the Avro file to get the header and all block information
 */
class AvroDataFileReader(si: SeekableInput) extends AutoCloseable {
  private val sin = new SeekableInputStream(si)
  sin.seek(0) // seek to the start of file and get some meta info.
  private var vin: BinaryDecoder = DecoderFactory.get.binaryDecoder(sin, vin);
  private val header: Header = new Header()
  private var firstBlockStart: Long = 0

  // store all blocks info
  private val blocks: mutable.ArrayBuffer[BlockInfo] = mutable.ArrayBuffer.empty

  initialize()

  def getBlocks(): Seq[BlockInfo] = blocks.toSeq

  def getHeader(): Header = header

  private def initialize() = {
    val magic = new Array[Byte](DataFileConstants.MAGIC.length)
    vin.readFixed(magic)

    magic match {
      case Array(79, 98, 106, 1) => // current avro format
      case Array(79, 98, 106, 0) => // old format
        throw new UnsupportedOperationException("avro 1.2 format is not support by GPU")
      case _ => throw new RuntimeException("Not an Avro data file.")
    }

    var l = vin.readMapStart().toInt
    if (l > 0) {
      do {
        for (i <- 1 to l) {
          val key = vin.readString(null).toString
          val value = vin.readBytes(null)
          val bb = new Array[Byte](value.remaining())
          value.get(bb)
          header.meta += (key -> bb)
        }
        l = vin.mapNext().toInt
      } while (l != 0)
    }
    vin.readFixed(header.sync)
    firstBlockStart = sin.tell - vin.inputStream.available // get the first block Start address
    header.headerSize = Some(firstBlockStart)
    parseBlocks()
  }

  private def seek(position: Long): Unit = {
    sin.seek(position)
    vin = DecoderFactory.get().binaryDecoder(this.sin, vin);
  }

  private def parseBlocks(): Unit = {
    if (firstBlockStart >= sin.length() || vin.isEnd()) {
      // no blocks
      return
    }
    // buf is used for writing long
    val buf = new Array[Byte](12)
    var blockStart = firstBlockStart
    while (blockStart < sin.length()) {
      seek(blockStart)
      if (vin.isEnd()) {
        return
      }
      val blockCount = vin.readLong()
      val blockDataSize = vin.readLong()
      if (blockDataSize > Integer.MAX_VALUE || blockDataSize < 0) {
        throw new IOException("Block size invalid or too large: " + blockDataSize)
      }

      // Get how many bytes used to store the value of count and block data size.
      val blockCountLen = BinaryData.encodeLong(blockCount, buf, 0)
      val blockDataSizeLen: Int = BinaryData.encodeLong(blockDataSize, buf, 0)

      // (len of entries) + (len of block size) + (block size) + (sync size)
      val blockLength = blockCountLen + blockDataSizeLen + blockDataSize +
          DataFileConstants.SYNC_SIZE
      blocks += BlockInfo(blockStart, blockLength, blockDataSize, blockCount)

      // Do we need to check the SYNC BUFFER, or just let cudf do it?
      blockStart += blockLength
    }
  }

  override def close(): Unit = {
    vin.inputStream().close()
  }
}

object AvroDataFileReader {

  def openReader(si: SeekableInput): AvroDataFileReader = {
    new AvroDataFileReader(si)
  }
}
