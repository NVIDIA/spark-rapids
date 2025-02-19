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
import java.io.IOException;

/**
 * Adapter class which helps to save memory copy when shuffle manager uses
 * {@link OpenByteArrayOutputStream} during serialization.
 */
public class OpenByteArrayOutputStreamWriter implements DataWriter {
  private final OpenByteArrayOutputStream out;

  public OpenByteArrayOutputStreamWriter(OpenByteArrayOutputStream bout) {
    requireNonNull(bout, "Byte array output stream can't be null");
    this.out = bout;
  }

  @Override
  public void reserve(int size) throws IOException {
    out.reserve(size);
  }

  @Override
  public void writeInt(int v) throws IOException {
    out.reserve(4 + out.size());
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }

  @Override
  public void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) throws IOException {
    out.write(src, srcOffset, toIntExact(len));
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void write(byte[] arr, int offset, int length) throws IOException {
    out.write(arr, offset, length);
  }
}
