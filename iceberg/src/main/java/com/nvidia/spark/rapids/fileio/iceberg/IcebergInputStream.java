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
