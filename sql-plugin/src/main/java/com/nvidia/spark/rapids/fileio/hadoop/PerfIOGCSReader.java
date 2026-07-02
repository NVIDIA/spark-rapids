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

import com.nvidia.spark.rapids.PerfIOConf;
import com.nvidia.spark.rapids.fileio.RapidsInputFiles.GCSPerfReader;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkEnv;

import java.io.IOException;

/** SQL-plugin bridge from Java-only file I/O classes to private Scala GCS PerfIO state. */
public final class PerfIOGCSReader implements GCSPerfReader {
    public static final PerfIOGCSReader INSTANCE = new PerfIOGCSReader();

    private PerfIOGCSReader() {}

    @Override
    public boolean isEnabled() {
        SparkEnv env = SparkEnv.get();
        if (env == null) {
            return false;
        }
        return env.conf().getBoolean(PerfIOConf.GCSPERF_ENABLED().key(), false);
    }

    @Override
    public RapidsInputFile createInputFile(Path path, Configuration conf) throws IOException {
        return GCSInputFile$.MODULE$.create(path, conf);
    }
}
