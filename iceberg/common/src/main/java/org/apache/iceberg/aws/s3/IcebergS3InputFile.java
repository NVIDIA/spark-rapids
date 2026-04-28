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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that routes byte-range reads through PerfIO using the
 * AWS S3 async client owned by Iceberg's {@link S3FileIO}.
 *
 * <p>Authorization is delegated entirely to Iceberg. {@link S3FileIO} builds and caches its
 * async client from {@link S3FileIOProperties} merged with any
 * {@link org.apache.iceberg.io.SupportsStorageCredentials} contributions the catalog
 * supplied; this class borrows that already-configured client so PerfIO inherits the same
 * credential chain (IRSA / assumed-role / signer / region pinning) as every other S3 call
 * the FileIO makes.
 *
 * <p>The async client is held as {@code Object} (read reflectively from
 * {@link S3FileIO#asyncClient()}) so that {@code iceberg/common} does not pick up a
 * compile-time dependency on the AWS SDK — the SDK is supplied at runtime by Iceberg's
 * bundled spark-runtime jar. {@link PerfIO} re-casts it to its real type in private/core,
 * where aws-sdk is already on the compile classpath.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private static final Method S3_FILEIO_ASYNC_CLIENT;

  static {
    try {
      S3_FILEIO_ASYNC_CLIENT = S3FileIO.class.getMethod("asyncClient");
    } catch (NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final URI s3Uri;
  private final Object asyncClient;

  private IcebergS3InputFile(InputFile delegate, URI s3Uri, Object asyncClient) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.asyncClient = asyncClient;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile, FileIO fileIO) {
    if (!(inputFile instanceof BaseS3File) || !(fileIO instanceof S3FileIO)) {
      return new IcebergInputFile(inputFile);
    }
    Object asyncClient;
    try {
      asyncClient = S3_FILEIO_ASYNC_CLIENT.invoke(fileIO);
    } catch (ReflectiveOperationException e) {
      return new IcebergInputFile(inputFile);
    }
    if (asyncClient == null) {
      // S3FileIO opted out of async (e.g. s3.crt.enabled=false). Without an async client
      // there is no zero-copy fast path; fall back to the default reader.
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
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
