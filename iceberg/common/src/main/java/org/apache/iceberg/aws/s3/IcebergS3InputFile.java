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
import com.nvidia.spark.rapids.IcebergS3RangeCopier;
import com.nvidia.spark.rapids.IcebergS3RangeCopier.IcebergS3Client;
import com.nvidia.spark.rapids.PerfIOConf;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import com.nvidia.spark.rapids.iceberg.ShimUtils;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that delegates byte-range reads to
 * {@link IcebergS3RangeCopier}. The supplied {@link FileIO} is only used for
 * its property map and any per-prefix storage-credential overlays.
 *
 * <p>This class lives in {@code org.apache.iceberg.aws.s3} for the package-private
 * {@link BaseS3File} access used to extract the bucket/key URI.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergS3InputFile.class);

  private final URI s3Uri;
  private final IcebergS3Client icebergS3Client;

  private IcebergS3InputFile(InputFile delegate, URI s3Uri, IcebergS3Client icebergS3Client) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.icebergS3Client = icebergS3Client;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile, FileIO fileIO) {
    // When the gating conf is off (or the file is not an S3 file), return the
    // default IcebergInputFile so the standard Iceberg SeekableInputStream path is used.
    if (!isPerfioS3Enabled() || !(inputFile instanceof BaseS3File)) {
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
    S3URI uri = s3File.uri();
    // Use the 4-arg URI constructor so the (raw, un-percent-encoded) bucket and
    // key are encoded per-component. URI.create / new URI(String) would treat the
    // input as an already-encoded URI string and throw on partition values that
    // contain spaces, '#', or other reserved characters (e.g. partition=hello world).
    URI s3Uri;
    try {
      s3Uri = new URI("s3", uri.bucket(), "/" + uri.key(), null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid S3 URI for bucket=" + uri.bucket() + " key=" + uri.key(), e);
    }
    // Iceberg < 1.7 does not have SupportsStorageCredentials; ShimUtils returns
    // the per-prefix credential overlays (or an empty map on 1.6).
    IcebergS3Client icebergS3Client = IcebergS3RangeCopier.resolveClient(
        s3Uri.toString(),
        fileIO.properties(),
        ShimUtils.storageCredentialOverlays(fileIO));
    LOG.debug("IcebergS3RangeCopier path active for {}", s3Uri);
    return new IcebergS3InputFile(inputFile, s3Uri, icebergS3Client);
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    IcebergS3RangeCopier.copyToHMB(icebergS3Client, output, s3Uri, copyRanges);
  }

  /**
   * Issue a single suffix-range {@code GetObject} ({@code Range: bytes=-N}) for
   * the last {@code length} bytes. Avoids the {@code getLength()} round-trip the
   * default {@link com.nvidia.spark.rapids.jni.fileio.RapidsInputFile#readTail}
   * would make.
   */
  @Override
  public void readTail(long length, HostMemoryBuffer output) throws IOException {
    if (length == 0) {
      return;
    }
    if (length < 0) {
      throw new IllegalArgumentException("length must be non-negative");
    }
    IcebergS3RangeCopier.copyTailToHMB(icebergS3Client, output, s3Uri, length, /*dstOffset*/ 0L);
  }

  private static boolean isPerfioS3Enabled() {
    SparkEnv env = SparkEnv.get();
    if (env == null) {
      return false;
    }
    return env.conf().getBoolean(PerfIOConf.S3PERF_ENABLED().key(), false);
  }
}
