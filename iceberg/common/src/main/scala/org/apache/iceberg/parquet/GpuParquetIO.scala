/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.parquet

import java.io.EOFException
import java.nio.ByteBuffer

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.parquet.ParquetFooterUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.io.{InputFile => ShadedInputFile, SeekableInputStream => ShadedSeekableInputStream}
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC

object GpuParquetIO {
  def file(file: InputFile): ShadedInputFile = {
    ParquetIO.file(file)
  }

  /**
   * Open a shaded `ParquetFileReader` with its footer tail served from the Rapids file cache when
   * possible. On a cache hit no remote IO happens for the footer; on a miss the tail is read from
   * the Iceberg `InputFile` and handed to `FileCache` for future reads.
   *
   * The cached bytes are wrapped around the real input file so `ParquetFileReader.open` still uses
   * its normal initialization path — no version-specific parquet API is required.
   */
  def openReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    val framedTail = withResource(ParquetFooterUtils.getFooterBuffer(
      inputFile, metrics,
      ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath))) { hmb =>
      ParquetFooterUtils.readBytesFromBuffer(hmb, 0, hmb.getLength.toInt)
    }
    // framedTail is MAGIC + <file tail bytes starting at footerStart>; the leading MAGIC is
    // synthetic so ParquetFileReader.readFooter can operate on it standalone elsewhere.
    val realShadedFile = file(inputFile.getDelegate)
    val wrapped = new FooterCachedShadedInputFile(realShadedFile, framedTail, MAGIC.length)
    ParquetFileReader.open(wrapped, options)
  }
}

/**
 * Shaded `InputFile` wrapper that serves reads in the footer tail region from an in-memory byte
 * array and delegates everything else to the real file. The wrapper is scoped to a single
 * `ParquetFileReader` open call; closing the reader closes the underlying stream.
 *
 * @param realFile      the real shaded input file (used for data reads outside the footer)
 * @param framedTail    bytes of the form `<synthetic prefix> + <real file tail bytes>`; real file
 *                      tail starts at index `tailPrefixLen` within this array
 * @param tailPrefixLen number of synthetic bytes at the start of `framedTail` (not part of the
 *                      real file); usually `MAGIC.length`
 */
private class FooterCachedShadedInputFile(
    realFile: ShadedInputFile,
    framedTail: Array[Byte],
    tailPrefixLen: Int) extends ShadedInputFile {

  private val fileLen: Long = realFile.getLength
  private val cachedBytes: Int = framedTail.length - tailPrefixLen
  private val cachedStart: Long = fileLen - cachedBytes

  override def getLength: Long = fileLen

  override def newStream(): ShadedSeekableInputStream = new ShadedSeekableInputStream {
    private var pos: Long = 0L
    private var realStream: ShadedSeekableInputStream = _

    private def realAt(p: Long): ShadedSeekableInputStream = {
      if (realStream == null) {
        realStream = realFile.newStream()
      }
      realStream.seek(p)
      realStream
    }

    override def getPos: Long = pos

    override def seek(newPos: Long): Unit = {
      pos = newPos
    }

    override def read(): Int = {
      if (pos >= fileLen) {
        -1
      } else if (pos >= cachedStart) {
        val b = framedTail((pos - cachedStart).toInt + tailPrefixLen) & 0xff
        pos += 1
        b
      } else {
        val rs = realAt(pos)
        val b = rs.read()
        if (b >= 0) {
          pos += 1
        }
        b
      }
    }

    override def read(dst: Array[Byte], off: Int, len: Int): Int = {
      if (pos >= fileLen || len == 0) {
        if (len == 0) 0 else -1
      } else if (pos >= cachedStart) {
        val avail = (fileLen - pos).toInt
        val toCopy = math.min(len, avail)
        System.arraycopy(framedTail, (pos - cachedStart).toInt + tailPrefixLen, dst, off, toCopy)
        pos += toCopy
        toCopy
      } else {
        val cap = math.min(len.toLong, cachedStart - pos).toInt
        val rs = realAt(pos)
        val n = rs.read(dst, off, cap)
        if (n > 0) {
          pos += n
        }
        n
      }
    }

    override def readFully(dst: Array[Byte]): Unit = readFully(dst, 0, dst.length)

    override def readFully(dst: Array[Byte], off: Int, len: Int): Unit = {
      var total = 0
      while (total < len) {
        val n = read(dst, off + total, len - total)
        if (n < 0) {
          throw new EOFException(s"Reached end of file at pos $pos while reading $len bytes")
        }
        total += n
      }
    }

    override def read(buf: ByteBuffer): Int = {
      if (pos >= fileLen) {
        return -1
      }
      val want = math.min(buf.remaining.toLong, fileLen - pos).toInt
      val tmp = new Array[Byte](want)
      var total = 0
      while (total < want) {
        val n = read(tmp, total, want - total)
        if (n < 0) {
          if (total == 0) return -1 else return total
        }
        total += n
      }
      buf.put(tmp, 0, total)
      total
    }

    override def readFully(buf: ByteBuffer): Unit = {
      val need = buf.remaining
      val tmp = new Array[Byte](need)
      readFully(tmp, 0, need)
      buf.put(tmp)
    }

    override def close(): Unit = {
      if (realStream != null) {
        realStream.close()
        realStream = null
      }
    }
  }
}
