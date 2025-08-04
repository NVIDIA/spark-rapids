package com.nvidia.spark.rapids.fileio.iceberg;

import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.fileio.SeekableInputStream;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.util.Objects;

public class IcebergInputFile implements RapidsInputFile {
  private final InputFile delegate;

  public IcebergInputFile(InputFile delegate) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    this.delegate = delegate;
  }

  @Override
  public long getLength() throws IOException {
    return delegate.getLength();
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return new IcebergInputStream(delegate.newStream());
  }
}
