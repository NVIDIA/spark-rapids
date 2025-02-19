/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.kudo;

import ai.rapids.cudf.HostMemoryBuffer;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static com.nvidia.spark.rapids.jni.Preconditions.ensure;
import static java.util.Objects.requireNonNull;

/**
 * This class extends {@link ByteArrayOutputStream} to provide some internal methods to save copy.
 */
public class OpenByteArrayOutputStream extends ByteArrayOutputStream {
  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 32;

  /**
   * Creates a new byte array output stream. The buffer capacity is
   * initially 32 bytes, though its size increases if necessary.
   */
  public OpenByteArrayOutputStream() {
    this(32);
  }

  /**
   * Creates a new byte array output stream, with a buffer capacity of
   * the specified size, in bytes.
   *
   * @param   size   the initial size.
   * @exception  IllegalArgumentException if size is negative.
   */
  public OpenByteArrayOutputStream(int size) {
    super(size);
  }

  /**
   * Get underlying byte array.
   */
  public byte[] getBuf() {
    return buf;
  }

  /**
   * Get actual number of bytes that have been written to this output stream.
   * @return Number of bytes written to this output stream. Note that this maybe smaller than length of
   *      {@link OpenByteArrayOutputStream#getBuf()}.
   */
  public int getCount() {
    return count;
  }

  /**
   * Increases the capacity if necessary to ensure that it can hold
   * at least the number of elements specified by the minimum
   * capacity argument.
   *
   * @param capacity the desired minimum capacity
   * @throws IllegalStateException If {@code capacity < 0}  or {@code capacity >= MAX_ARRAY_LENGTH}.
   */
  public void reserve(int capacity) {
    ensure(capacity >= 0, () -> "Requested capacity must be positive, but was " + capacity);
    ensure(capacity < MAX_ARRAY_LENGTH, () -> "Requested capacity is too large: " + capacity);

    if (capacity > buf.length) {
      buf = Arrays.copyOf(buf, capacity);
    }
  }

  /**
   * Copy from {@link HostMemoryBuffer} to this output stream.
   * @param srcBuf {@link HostMemoryBuffer} to copy from.
   * @param offset Start position in source {@link HostMemoryBuffer}.
   * @param length Number of bytes to copy.
   */
  public void write(HostMemoryBuffer srcBuf, long offset, int length) {
    requireNonNull(srcBuf, "Source buf can't be null!");
    reserve(count + length);
    srcBuf.getBytes(buf, count, offset, length);
    count += length;
  }
}
