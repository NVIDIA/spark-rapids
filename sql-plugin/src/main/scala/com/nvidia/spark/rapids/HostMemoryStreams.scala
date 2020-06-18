/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import java.io.{InputStream, IOException, OutputStream}

import ai.rapids.cudf.HostMemoryBuffer

/**
  * An implementation of OutputStream that writes to a HostMemoryBuffer.
  *
  * NOTE: Closing this output stream does NOT close the buffer!
  *
  * @param buffer the buffer to receive written data
  */
class HostMemoryOutputStream(buffer: HostMemoryBuffer) extends OutputStream {
  private var pos: Long = 0

  override def write(i: Int): Unit = {
    buffer.setByte(pos, i.toByte)
    pos += 1
  }

  override def write(bytes: Array[Byte]): Unit = {
    buffer.setBytes(pos, bytes, 0, bytes.length)
    pos += bytes.length
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    buffer.setBytes(pos, bytes, offset, len)
    pos += len
  }

  def getPos: Long = pos
}

/**
  * An implementation of InputStream that reads from a HostMemoryBuffer.
  *
  * NOTE: Closing this input stream does NOT close the buffer!
  *
  * @param hmb the buffer from which to read data
  * @param hmbLength the amount of data available in the buffer
  */
class HostMemoryInputStream(
    hmb: HostMemoryBuffer,
    hmbLength: Long) extends InputStream {
  private var pos = 0L
  private var mark = -1L

  override def read(): Int = {
    if (pos >= hmbLength) {
      -1
    } else {
      val result = hmb.getByte(pos)
      pos += 1
      result
    }
  }

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    if (pos >= hmbLength) {
      -1
    } else {
      val numBytes = Math.min(available(), length)
      hmb.getBytes(buffer, offset, pos, numBytes)
      pos += numBytes
      numBytes
    }
  }

  override def skip(count: Long): Long = {
    val oldPos = pos
    pos = Math.min(pos + count, hmbLength)
    pos - oldPos
  }

  override def available(): Int = Math.min(hmbLength - pos, Integer.MAX_VALUE).toInt

  override def mark(ignored: Int): Unit = {
    mark = pos
  }

  override def reset(): Unit = {
    if (mark <= 0) {
      throw new IOException("reset called before mark")
    }
    pos = mark
  }

  override def markSupported(): Boolean = true

  def getPos: Long = pos
}
