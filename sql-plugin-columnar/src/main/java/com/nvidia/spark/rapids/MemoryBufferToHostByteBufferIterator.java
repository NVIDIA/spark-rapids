/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import java.nio.ByteBuffer;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.MemoryBuffer;

/**
 * Create an iterator that will emit ByteBuffer instances sequentially to work around the 2GB
 * ByteBuffer size limitation after copying a MemoryBuffer to a host-backed bounce buffer.
 *
 * NOTE: It is the caller's responsibility to ensure this iterator does not outlive memoryBuffer.
 * The iterator DOES NOT increment the reference count of memoryBuffer to ensure it remains valid.
 */
public class MemoryBufferToHostByteBufferIterator extends AbstractHostByteBufferIterator {
  private final MemoryBuffer memoryBuffer;
  private final HostMemoryBuffer bounceBuffer;
  private final Cuda.Stream stream;
  private final long totalLength;
  private final long limit;

  public MemoryBufferToHostByteBufferIterator(
      MemoryBuffer memoryBuffer,
      HostMemoryBuffer bounceBuffer,
      Cuda.Stream stream) {
    this.memoryBuffer = memoryBuffer;
    this.bounceBuffer = bounceBuffer;
    this.stream = stream;
    this.totalLength = memoryBuffer == null ? 0 : memoryBuffer.getLength();
    this.limit = Math.min(bounceBuffer.getLength(), Integer.MAX_VALUE);
  }

  @Override
  public long totalLength() {
    return totalLength;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  public ByteBuffer getByteBuffer(long offset, long length) {
    bounceBuffer.copyFromMemoryBufferAsync(0, memoryBuffer, offset, length, stream);
    stream.sync();
    return bounceBuffer.asByteBuffer(0, (int) length);
  }
}
