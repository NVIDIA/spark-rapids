/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet.converter

import java.io.EOFException
import java.nio.ByteBuffer

import org.apache.iceberg.io.{InputFile, SeekableInputStream}
import org.apache.iceberg.shaded.org.apache.parquet.io.{
  InputFile => ShadedInputFile,
  SeekableInputStream => ShadedSeekableInputStream}

object ToIcebergShaded {
  def inputFile(file: InputFile): ShadedInputFile = {
    new ShadedInputFile {
      override def getLength: Long = file.getLength

      override def newStream(): ShadedSeekableInputStream =
        new IcebergShadedSeekableInputStream(file.newStream())
    }
  }
}

private class IcebergShadedSeekableInputStream(stream: SeekableInputStream)
  extends ShadedSeekableInputStream {

  override def getPos: Long = stream.getPos

  override def seek(newPos: Long): Unit = stream.seek(newPos)

  override def read(): Int = stream.read()

  override def read(bytes: Array[Byte], offset: Int, length: Int): Int =
    stream.read(bytes, offset, length)

  override def close(): Unit = stream.close()

  override def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)

  override def readFully(bytes: Array[Byte], start: Int, length: Int): Unit = {
    var offset = start
    var remaining = length
    while (remaining > 0) {
      val bytesRead = read(bytes, offset, remaining)
      if (bytesRead < 0) {
        throw new EOFException()
      }
      offset += bytesRead
      remaining -= bytesRead
    }
  }

  override def read(buffer: ByteBuffer): Int = {
    if (!buffer.hasRemaining) {
      0
    } else if (buffer.hasArray) {
      val bytesRead = stream.read(
        buffer.array(),
        buffer.arrayOffset() + buffer.position(),
        buffer.remaining())
      if (bytesRead > 0) {
        buffer.position(buffer.position() + bytesRead)
      }
      bytesRead
    } else {
      val bytes = new Array[Byte](math.min(buffer.remaining(), 8192))
      val bytesRead = stream.read(bytes, 0, bytes.length)
      if (bytesRead > 0) {
        buffer.put(bytes, 0, bytesRead)
      }
      bytesRead
    }
  }

  override def readFully(buffer: ByteBuffer): Unit = {
    while (buffer.hasRemaining) {
      val bytesRead = read(buffer)
      if (bytesRead < 0) {
        throw new EOFException()
      }
    }
  }
}
