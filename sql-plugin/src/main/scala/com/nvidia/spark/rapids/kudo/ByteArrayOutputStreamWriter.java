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

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import ai.rapids.cudf.HostMemoryBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Adapter class which helps to save memory copy when shuffle manager uses
 * {@link ByteArrayOutputStream} during serialization.
 */
public class ByteArrayOutputStreamWriter implements DataWriter {
  private static final Method ENSURE_CAPACITY;
  private static final Field BUF;
  private static final Field COUNT;

  static {
    try {
      ENSURE_CAPACITY = ByteArrayOutputStream.class.getDeclaredMethod("ensureCapacity", int.class);
      ENSURE_CAPACITY.setAccessible(true);

      BUF = ByteArrayOutputStream.class.getDeclaredField("buf");
      BUF.setAccessible(true);


      COUNT = ByteArrayOutputStream.class.getDeclaredField("count");
      COUNT.setAccessible(true);
    } catch (NoSuchMethodException | NoSuchFieldException e) {
      throw new RuntimeException("Failed to find ByteArrayOutputStream.ensureCapacity", e);
    }
  }

  private final ByteArrayOutputStream out;

  public ByteArrayOutputStreamWriter(ByteArrayOutputStream bout) {
    requireNonNull(bout, "Byte array output stream can't be null");
    this.out = bout;
  }

  @Override
  public void reserve(int size) throws IOException {
    try {
      ENSURE_CAPACITY.invoke(out, size);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke ByteArrayOutputStream.ensureCapacity", e);
    }
  }

  @Override
  public void writeInt(int v) throws IOException {
    reserve(Integer.BYTES + out.size());
    byte[] bytes = new byte[4];
    bytes[0] = (byte) ((v >>> 24) & 0xFF);
    bytes[1] = (byte) ((v >>> 16) & 0xFF);
    bytes[2] = (byte) ((v >>> 8) & 0xFF);
    bytes[3] = (byte) (v & 0xFF);
    out.write(bytes);
  }

  @Override
  public void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) throws IOException {
    reserve(toIntExact(out.size() + len));

    try {
      byte[] buf = (byte[]) BUF.get(out);
      int count = out.size();

      src.getBytes(buf, count, srcOffset, len);
      COUNT.setInt(out, toIntExact(count + len));
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void write(byte[] arr, int offset, int length) throws IOException {
    out.write(arr, offset, length);
  }
}
