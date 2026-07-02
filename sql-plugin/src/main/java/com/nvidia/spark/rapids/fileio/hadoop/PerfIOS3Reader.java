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

package com.nvidia.spark.rapids.fileio.hadoop;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.IntRangeWithOffset;
import com.nvidia.spark.rapids.PerfIO$;
import com.nvidia.spark.rapids.PerfIOConf;
import com.nvidia.spark.rapids.RangeWithOffset;
import com.nvidia.spark.rapids.SuffixRangeWithOffset;
import com.nvidia.spark.rapids.fileio.RapidsInputFiles.S3PerfReader;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkEnv;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/** SQL-plugin bridge from Java-only file I/O classes to private Scala PerfIO state. */
public final class PerfIOS3Reader implements S3PerfReader {
    public static final PerfIOS3Reader INSTANCE = new PerfIOS3Reader();

    private PerfIOS3Reader() {}

    @Override
    public boolean isEnabled() {
        SparkEnv env = SparkEnv.get();
        if (env == null) {
            return false;
        }
        return env.conf().getBoolean(PerfIOConf.S3PERF_ENABLED().key(), false);
    }

    @Override
    public boolean readVectored(
            Configuration hadoopConf,
            URI fileUri,
            HostMemoryBuffer output,
            List<RapidsInputFile.CopyRange> copyRanges) throws IOException {
        List<RangeWithOffset> ranges = new ArrayList<>(copyRanges.size());
        for (RapidsInputFile.CopyRange range : copyRanges) {
            ranges.add(new IntRangeWithOffset(
                    range.getInputOffset(), range.getLength(), range.getOutputOffset()));
        }
        return readToHostMemory(hadoopConf, fileUri, output, ranges);
    }

    @Override
    public boolean readTail(
            Configuration hadoopConf,
            URI fileUri,
            HostMemoryBuffer output,
            long length,
            long outputOffset) throws IOException {
        List<RangeWithOffset> ranges = new ArrayList<>(1);
        ranges.add(new SuffixRangeWithOffset(length, outputOffset));
        return readToHostMemory(hadoopConf, fileUri, output, ranges);
    }

    private boolean readToHostMemory(
            Configuration hadoopConf,
            URI fileUri,
            HostMemoryBuffer output,
            List<RangeWithOffset> ranges) {
        Option<Object> result = PerfIO$.MODULE$.readToHostMemory(
                hadoopConf,
                output,
                fileUri,
                () -> JavaConverters.asScalaBufferConverter(ranges).asScala().toSeq());
        return result.isDefined();
    }
}
