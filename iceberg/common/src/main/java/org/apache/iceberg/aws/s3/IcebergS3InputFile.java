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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * S3-backed {@link IcebergInputFile} that routes byte-range reads through PerfIO's shared
 * async S3 client. Lives in {@code org.apache.iceberg.aws.s3} for access to the
 * package-private {@link S3FileIOProperties}.
 *
 * <p>Uses the ambient Spark/Hadoop {@link Configuration} so PerfIO inherits the same
 * credential-provider chain (IRSA, assumed role, EMRFS defaults, etc.) as the rest of the
 * job. Synthesizing a {@code Configuration} from {@link S3FileIOProperties} would drop
 * everything except static keys, which silently falls back to IMDS on EKS and authenticates
 * as the node instance role instead of the pod's execution role.
 */
public final class IcebergS3InputFile extends IcebergInputFile {
  private final URI s3Uri;
  private final Configuration hadoopConf;

  private IcebergS3InputFile(InputFile delegate, URI s3Uri, Configuration hadoopConf) {
    super(delegate);
    this.s3Uri = s3Uri;
    this.hadoopConf = hadoopConf;
  }

  public static IcebergInputFile maybeCreate(InputFile inputFile, Configuration hadoopConf) {
    if (!(inputFile instanceof BaseS3File) || !PerfIO.s3BackendAvailable()) {
      return new IcebergInputFile(inputFile);
    }
    BaseS3File s3File = (BaseS3File) inputFile;
    S3URI uri = s3File.uri();
    URI s3Uri = URI.create("s3://" + uri.bucket() + "/" + uri.key());
    return new IcebergS3InputFile(inputFile, s3Uri, hadoopConf);
  }

  @Override
  public void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges)
      throws IOException {
    PerfIO.readVectoredS3(hadoopConf, output, s3Uri, copyRanges);
  }
}
