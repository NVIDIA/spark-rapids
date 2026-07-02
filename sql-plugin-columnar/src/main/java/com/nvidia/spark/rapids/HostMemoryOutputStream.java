/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * An implementation of OutputStream that writes to a HostMemoryBuffer.
 *
 * NOTE: Closing this output stream does NOT close the buffer!
 */
public class HostMemoryOutputStream extends OutputStream {
  public final HostMemoryBuffer buffer;
  protected long pos = 0;

  public HostMemoryOutputStream(HostMemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public HostMemoryBuffer buffer() {
    return buffer;
  }

  @Override
  public void write(int i) {
    buffer.setByte(pos, (byte) i);
    pos += 1;
  }

  @Override
  public void write(byte[] bytes) {
    buffer.setBytes(pos, bytes, 0, bytes.length);
    pos += bytes.length;
  }

  @Override
  public void write(byte[] bytes, int offset, int len) {
    buffer.setBytes(pos, bytes, offset, len);
    pos += len;
  }

  public void write(ByteBuffer data) {
    int numBytes = data.remaining();
    ByteBuffer outBuffer = buffer.asByteBuffer(pos, numBytes);
    outBuffer.put(data);
    pos += numBytes;
  }

  public ByteBuffer writeAsByteBuffer(int length) {
    ByteBuffer byteBuffer = buffer.asByteBuffer(pos, length);
    pos += length;
    return byteBuffer;
  }

  public long getPos() {
    return pos;
  }

  public void seek(long newPos) {
    pos = newPos;
  }

  public void copyFromChannel(ReadableByteChannel channel, long length) throws IOException {
    long endPos = pos + length;
    if (endPos > buffer.getLength()) {
      throw new AssertionError();
    }
    while (pos != endPos) {
      int bytesToCopy = (int) Math.min(endPos - pos, Integer.MAX_VALUE);
      ByteBuffer byteBuffer = buffer.asByteBuffer(pos, bytesToCopy);
      while (byteBuffer.hasRemaining()) {
        int channelReadBytes = channel.read(byteBuffer);
        if (channelReadBytes < 0) {
          throw new EOFException("Unexpected EOF while reading from byte channel");
        }
      }
      pos += bytesToCopy;
    }
  }
}
