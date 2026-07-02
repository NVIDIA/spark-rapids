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

import java.nio.channels.ReadableByteChannel;

/** A HostMemoryOutputStream only counts the written bytes, nothing is actually written. */
public final class NullHostMemoryOutputStream extends HostMemoryOutputStream {
  public NullHostMemoryOutputStream() {
    super(null);
  }

  @Override
  public void write(int i) {
    pos += 1;
  }

  @Override
  public void write(byte[] bytes) {
    pos += bytes.length;
  }

  @Override
  public void write(byte[] bytes, int offset, int len) {
    pos += len;
  }

  @Override
  public void copyFromChannel(ReadableByteChannel channel, long length) {
    long endPos = pos + length;
    while (pos != endPos) {
      long bytesToCopy = Math.min(endPos - pos, Integer.MAX_VALUE);
      pos += bytesToCopy;
    }
  }
}
