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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * An implementation of InputStream that reads from a HostMemoryBuffer.
 *
 * NOTE: Closing this input stream does NOT close the buffer!
 */
public class HostMemoryInputStream extends InputStream {
  public final HostMemoryBuffer hmb;
  public final long hmbLength;

  protected long pos = 0;
  protected long mark = -1;

  public HostMemoryInputStream(HostMemoryBuffer hmb, long hmbLength) {
    this.hmb = hmb;
    this.hmbLength = hmbLength;
  }

  public HostMemoryBuffer hmb() {
    return hmb;
  }

  public long hmbLength() {
    return hmbLength;
  }

  @Override
  public int read() {
    if (pos >= hmbLength) {
      return -1;
    }
    byte result = hmb.getByte(pos);
    pos += 1;
    // Java bytes are signed, so mask off the upper bits to avoid returning negative EOF values.
    return result & 0xFF;
  }

  @Override
  public int read(byte[] buffer, int offset, int length) {
    if (pos >= hmbLength) {
      return -1;
    }
    int numBytes = Math.min(available(), length);
    hmb.getBytes(buffer, offset, pos, numBytes);
    pos += numBytes;
    return numBytes;
  }

  public ByteBuffer readByteBuffer(int length) {
    ByteBuffer byteBuffer = hmb.asByteBuffer(pos, length);
    pos += length;
    return byteBuffer;
  }

  @Override
  public long skip(long count) {
    long oldPos = pos;
    pos = Math.min(pos + count, hmbLength);
    return pos - oldPos;
  }

  @Override
  public int available() {
    return (int) Math.min(hmbLength - pos, Integer.MAX_VALUE);
  }

  @Override
  public void mark(int ignored) {
    mark = pos;
  }

  @Override
  public void reset() throws IOException {
    if (mark <= 0) {
      throw new IOException("reset called before mark");
    }
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public long getPos() {
    return pos;
  }
}
