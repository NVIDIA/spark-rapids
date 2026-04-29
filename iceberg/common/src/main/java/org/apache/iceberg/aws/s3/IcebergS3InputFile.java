/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.aws.s3;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.PerfIO;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import com.nvidia.spark.rapids.iceberg.IcebergProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that routes byte-range reads through PerfIO. PerfIO
 * picks one of two paths:
 *
 * <ul>
 *   <li>Common case (IRSA / assumed-role / static-key auth): the live Hadoop
 *       {@link Configuration} drives PerfIO's tuned RangeCopier singleton. Fast.</li>
 *   <li>REST-vended credentials present (Iceberg 1.9+ {@code SupportsStorageCredentials}
 *       is non-empty): a per-iceberg-version bridge in spark-rapids-private builds an
 *       {@code S3AsyncClient} from {@code S3FileIOProperties} so the table-scoped creds
 *       are honored. Slower but correct.</li>
 * </ul>
 *
 * <p>This class lives in {@code org.apache.iceberg.aws.s3} for the package-private
 * {@link BaseS3File} access used to extract the bucket/key URI.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private final URI s3Uri;
  private final Configuration hadoopConf;
  private final FileIO fileIO;
  private final String shimPackage;

  private IcebergS3InputFile(
      InputFile delegate,
      URI s3Uri,
      Configuration hadoopConf,
      FileIO fileIO,
      String shimPackage) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.hadoopConf = hadoopConf;
    this.fileIO = fileIO;
    this.shimPackage = shimPackage;
  }

  public static IcebergInputFile maybeCreate(
      InputFile inputFile, FileIO fileIO, Configuration hadoopConf) {
    if (!(inputFile instanceof BaseS3File) || !PerfIO.s3BackendAvailable()) {
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
    S3URI uri = s3File.uri();
    URI s3Uri = URI.create("s3://" + uri.bucket() + "/" + uri.key());
    return new IcebergS3InputFile(
        inputFile, s3Uri, hadoopConf, fileIO, IcebergProvider.shimPackage());
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    PerfIO.readVectoredS3(hadoopConf, fileIO, shimPackage, output, s3Uri, copyRanges);
  }
}
