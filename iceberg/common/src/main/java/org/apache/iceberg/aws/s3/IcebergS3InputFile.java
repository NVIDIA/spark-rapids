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
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that routes byte-range reads through PerfIO. The
 * {@code S3AsyncClient} is borrowed from Iceberg's own {@code S3FileIO} via a per-version
 * shim ({@link ShimUtils#s3AsyncClient}), so the credential chain, endpoint, and CRT
 * transport configured on the FileIO (including REST-catalog vended credentials on 1.9+
 * and per-prefix routing on 1.10+) are honored without spark-rapids manufacturing its
 * own client.
 *
 * <p>Iceberg 1.6.x has no public {@code S3AsyncClient} accessor on {@code S3FileIO}, so
 * {@link #maybeCreate} falls back to a plain {@link IcebergInputFile} on that version
 * and lets the default Iceberg vectored read run.
 *
 * <p>This class lives in {@code org.apache.iceberg.aws.s3} for the package-private
 * {@link BaseS3File} access used to extract the bucket/key URI.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private final URI s3Uri;
  private final Object asyncClient;

  private IcebergS3InputFile(InputFile delegate, URI s3Uri, Object asyncClient) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.asyncClient = asyncClient;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile, FileIO fileIO) {
    if (!(inputFile instanceof BaseS3File)
        || !PerfIO.s3BackendAvailable()
        || !ShimUtils.s3AsyncClientSupported()) {
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
    S3URI uri = s3File.uri();
    URI s3Uri = URI.create("s3://" + uri.bucket() + "/" + uri.key());
    Object asyncClient = ShimUtils.s3AsyncClient(fileIO, s3Uri.toString());
    return new IcebergS3InputFile(inputFile, s3Uri, asyncClient);
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    PerfIO.readVectoredS3(asyncClient, output, s3Uri, copyRanges);
  }
}
