/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.nio.ByteBuffer

import ai.rapids.cudf.HostMemoryBuffer

/**
 * Create an iterator that will emit ByteBuffer instances sequentially
 * to work around the 2GB ByteBuffer size limitation. This allows
 * the entire address range of a >2GB host buffer to be covered
 * by a sequence of ByteBuffer instances.
 * <p>NOTE: It is the caller's responsibility to ensure this iterator
 * does not outlive the host buffer. The iterator DOES NOT increment
 * the reference count of the host buffer to ensure it remains valid.
 *
 * @param hostBuffer host buffer to iterate
 * @return ByteBuffer iterator
 */
class HostByteBufferIterator(hostBuffer: HostMemoryBuffer)
    extends Iterator[ByteBuffer] {
  private[this] var nextBufferStart: Long = 0L

  override def hasNext: Boolean = hostBuffer != null && nextBufferStart < hostBuffer.getLength

  override def next(): ByteBuffer = {
    val offset = nextBufferStart
    val length = Math.min(hostBuffer.getLength - nextBufferStart, Integer.MAX_VALUE)
    nextBufferStart += length
    hostBuffer.asByteBuffer(offset, length.toInt)
  }
}
