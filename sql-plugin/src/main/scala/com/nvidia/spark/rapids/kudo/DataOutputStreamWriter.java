/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Visible for testing
 */
class DataOutputStreamWriter implements DataWriter {
  private final byte[] arrayBuffer = new byte[1024];
  private final DataOutputStream dout;

  public DataOutputStreamWriter(DataOutputStream dout) {
    this.dout = dout;
  }

  @Override
  public void writeInt(int i) throws IOException {
    dout.writeInt(i);
  }

  @Override
  public void copyDataFrom(HostMemoryBuffer src, long srcOffset, long len) throws IOException {
    long dataLeft = len;
    while (dataLeft > 0) {
      int amountToCopy = (int) Math.min(arrayBuffer.length, dataLeft);
      src.getBytes(arrayBuffer, 0, srcOffset, amountToCopy);
      dout.write(arrayBuffer, 0, amountToCopy);
      srcOffset += amountToCopy;
      dataLeft -= amountToCopy;
    }
  }

  @Override
  public void flush() throws IOException {
    dout.flush();
  }

  @Override
  public void write(byte[] arr, int offset, int length) throws IOException {
    dout.write(arr, offset, length);
  }
}
