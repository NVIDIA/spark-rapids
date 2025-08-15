package com.nvidia.spark.rapids.fileio.iceberg;

import com.nvidia.spark.rapids.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.util.Objects;

public class IcebergFileIO implements RapidsFileIO {
  private final FileIO delegate;

  public IcebergFileIO(FileIO delegate) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    this.delegate = delegate;
  }


  @Override
  public RapidsInputFile open(String path) throws IOException {
    return new IcebergInputFile(delegate.newInputFile(path));
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
