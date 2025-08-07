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

package com.nvidia.spark.rapids.fileio.iceberg;

import com.nvidia.spark.rapids.fileio.SeekableInputStream;

import java.io.IOException;
import java.util.Objects;

public class IcebergInputStream extends SeekableInputStream {
  private final org.apache.iceberg.io.SeekableInputStream delegate;
  private boolean closed;

  public IcebergInputStream(org.apache.iceberg.io.SeekableInputStream delegate) {
    Objects.requireNonNull(delegate, "delegate can't be null!");
    this.delegate = delegate;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    delegate.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return delegate.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (!closed)  {
      super.close();
      this.closed = true;
    }
  }

}
