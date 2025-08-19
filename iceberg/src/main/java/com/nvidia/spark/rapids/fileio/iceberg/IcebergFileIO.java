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

import com.nvidia.spark.rapids.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link RapidsFileIO} using the Iceberg {@link FileIO}.
 * <br/>
 * This class wraps an Iceberg {@link FileIO} and provides a method to create
 * {@link RapidsInputFile} instances.
 */
public class IcebergFileIO implements RapidsFileIO {
  private final FileIO delegate;

  /**
   * Constructs an IcebergFileIO with the given Iceberg FileIO delegate.
   *
   * @param delegate the Iceberg FileIO to delegate to. It's the caller's responsibility to ensure
   *                 that the delegate is closed when no longer used, e.g., iceberg table/catalog close.
   */
  public IcebergFileIO(FileIO delegate) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    this.delegate = delegate;
  }


  @Override
  public RapidsInputFile newInputFile(String path) throws IOException {
    return new IcebergInputFile(delegate.newInputFile(path));
  }
}
