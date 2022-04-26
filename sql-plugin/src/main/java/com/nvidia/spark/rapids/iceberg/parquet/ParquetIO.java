/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ParquetIO {
  private ParquetIO() {
  }

  static InputFile file(org.apache.iceberg.io.InputFile file) {
    // TODO: use reflection to avoid depending on classes from iceberg-hadoop
    // TODO: use reflection to avoid depending on classes from hadoop
    if (file instanceof HadoopInputFile) {
      HadoopInputFile hfile = (HadoopInputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(hfile.getStat(), hfile.getConf());
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create Parquet input file for " + file, e);
      }
    }
    return new ParquetInputFile(file);
  }

  static SeekableInputStream stream(org.apache.iceberg.io.SeekableInputStream stream) {
    if (stream instanceof DelegatingInputStream) {
      InputStream wrapped = ((DelegatingInputStream) stream).getDelegate();
      if (wrapped instanceof FSDataInputStream) {
        return HadoopStreams.wrap((FSDataInputStream) wrapped);
      }
    }
    return new ParquetInputStreamAdapter(stream);
  }

  private static class ParquetInputStreamAdapter extends DelegatingSeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream delegate;

    private ParquetInputStreamAdapter(org.apache.iceberg.io.SeekableInputStream delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }
  }

  private static class ParquetInputFile implements InputFile {
    private final org.apache.iceberg.io.InputFile file;

    private ParquetInputFile(org.apache.iceberg.io.InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() throws IOException {
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return stream(file.newStream());
    }
  }
}
