/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.s3.IcebergS3InputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

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
  private final Configuration hadoopConf;

  /**
   * Constructs an IcebergFileIO with the given Iceberg FileIO delegate.
   *
   * @param delegate   the Iceberg FileIO to delegate to. It's the caller's responsibility to ensure
   *                   that the delegate is closed when no longer used, e.g., iceberg table/catalog close.
   * @param hadoopConf the ambient Spark/Hadoop configuration. Passed through to PerfIO for S3 reads
   *                   so the async S3 client inherits the same credential-provider chain as the rest
   *                   of the job (IRSA on EKS, assumed role, EMRFS defaults, etc.).
   */
  public IcebergFileIO(FileIO delegate, Configuration hadoopConf) {
    Objects.requireNonNull(delegate, "delegate can't be null");
    Objects.requireNonNull(hadoopConf, "hadoopConf can't be null");
    this.delegate = delegate;
    this.hadoopConf = hadoopConf;
  }


  @Override
  public IcebergInputFile newInputFile(String path) throws IOException {
    InputFile inputFile = delegate.newInputFile(path);
    return IcebergS3InputFile.maybeCreate(inputFile, hadoopConf);
  }

  @Override
  public IcebergOutputFile newOutputFile(String path) throws IOException {
    return new IcebergOutputFile(delegate.newOutputFile(path));
  }
}
