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

import com.nvidia.spark.rapids.fileio.RapidsInputFiles;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** Hadoop input factory that routes S3 paths through the registered PerfIO bridge. */
public final class PerfIOHadoopInputFileFactory implements HadoopInputFileFactory {
    public static final PerfIOHadoopInputFileFactory INSTANCE = new PerfIOHadoopInputFileFactory();

    private PerfIOHadoopInputFileFactory() {}

    @Override
    public RapidsInputFile create(Path path, Configuration conf) throws IOException {
        String scheme = path.toUri().getScheme();
        if (scheme != null && scheme.startsWith("s3") && RapidsInputFiles.isS3PerfEnabled()) {
            return S3InputFile.create(path, conf);
        }
        return HadoopInputFile.create(path, conf);
    }

    private Object readResolve() {
        return INSTANCE;
    }
}
