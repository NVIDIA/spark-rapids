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
import org.apache.iceberg.io.InputFile;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that routes byte-range reads through PerfIO using the
 * {@link S3AsyncClient} owned by Iceberg's {@link S3FileIO}. Lives in
 * {@code org.apache.iceberg.aws.s3} for access to the package-private {@link BaseS3File}.
 *
 * <p>Reusing Iceberg's async client keeps the PerfIO read path on the same credential chain
 * (resolved via {@link S3FileIOProperties}, including
 * {@link org.apache.iceberg.io.SupportsStorageCredentials} contributions) as every other S3
 * call the job makes.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private final URI s3Uri;
  private final S3AsyncClient asyncClient;

  private IcebergS3InputFile(InputFile delegate, URI s3Uri, S3AsyncClient asyncClient) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.asyncClient = asyncClient;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile) {
    if (!(inputFile instanceof BaseS3File)) {
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
    S3AsyncClient asyncClient = s3File.asyncClient();
    if (asyncClient == null) {
      // S3FileIO opted out of async (s3.crt.enabled=false and shouldUseAsyncClient()=false).
      // Without an async client we have no zero-copy fast path; fall back to the default reader.
      return new IcebergInputFile(inputFile);
    }
    S3URI uri = s3File.uri();
    URI s3Uri = URI.create("s3://" + uri.bucket() + "/" + uri.key());
    return new IcebergS3InputFile(inputFile, s3Uri, asyncClient);
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    PerfIO.readVectoredS3(asyncClient, output, s3Uri, copyRanges);
  }
}
