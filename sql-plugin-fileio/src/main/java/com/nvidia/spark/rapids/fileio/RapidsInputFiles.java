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

package com.nvidia.spark.rapids.fileio;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * Static helpers shared by {@link RapidsInputFile} implementations.
 */
public final class RapidsInputFiles {
    private static final S3PerfReader DISABLED_S3_PERF_READER = new S3PerfReader() {
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public boolean readVectored(
                Configuration hadoopConf,
                URI fileUri,
                HostMemoryBuffer output,
                List<RapidsInputFile.CopyRange> copyRanges) {
            return false;
        }

        @Override
        public boolean readTail(
                Configuration hadoopConf,
                URI fileUri,
                HostMemoryBuffer output,
                long length,
                long outputOffset) {
            return false;
        }
    };

    private static volatile S3PerfReader s3PerfReader = DISABLED_S3_PERF_READER;

    private RapidsInputFiles() {}

    /**
     * Java bridge for S3 PerfIO integration. The implementation lives in sql-plugin
     * because it depends on private Scala PerfIO state.
     */
    public interface S3PerfReader {
        boolean isEnabled();

        boolean readVectored(
                Configuration hadoopConf,
                URI fileUri,
                HostMemoryBuffer output,
                List<RapidsInputFile.CopyRange> copyRanges) throws IOException;

        boolean readTail(
                Configuration hadoopConf,
                URI fileUri,
                HostMemoryBuffer output,
                long length,
                long outputOffset) throws IOException;
    }

    public static void setS3PerfReader(S3PerfReader reader) {
        s3PerfReader = Objects.requireNonNull(reader, "reader can't be null");
    }

    public static void resetS3PerfReader() {
        s3PerfReader = DISABLED_S3_PERF_READER;
    }

    /**
     * True iff the active SQL-plugin bridge says the S3 PerfIO path is enabled.
     * Returns false before the bridge is registered so callers default to the
     * non-PerfIO path during early bring-up.
     */
    public static boolean isS3PerfEnabled() {
        return s3PerfReader.isEnabled();
    }

    public static boolean readS3Vectored(
            Configuration hadoopConf,
            URI fileUri,
            HostMemoryBuffer output,
            List<RapidsInputFile.CopyRange> copyRanges) throws IOException {
        return s3PerfReader.readVectored(hadoopConf, fileUri, output, copyRanges);
    }

    public static boolean readS3Tail(
            Configuration hadoopConf,
            URI fileUri,
            HostMemoryBuffer output,
            long length,
            long outputOffset) throws IOException {
        return s3PerfReader.readTail(hadoopConf, fileUri, output, length, outputOffset);
    }
}
