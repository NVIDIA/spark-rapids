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

import com.nvidia.spark.rapids.PerfIOConf;
import org.apache.spark.SparkEnv;

/**
 * Static helpers shared by {@link com.nvidia.spark.rapids.jni.fileio.RapidsInputFile}
 * implementations.
 */
public final class RapidsInputFiles {
    private RapidsInputFiles() {}

    /**
     * Cached value of {@code spark.rapids.perfio.s3.enabled}. The conf is marked
     * startupOnly, so caching after the first non-null {@link SparkEnv} read is
     * safe and avoids repeated {@link SparkEnv}/conf lookups on every input-file
     * factory call.
     */
    private static volatile Boolean s3PerfEnabledCache;

    /**
     * True iff {@code spark.rapids.perfio.s3.enabled} is set to {@code true} on
     * the active SparkConf. Returns false when no {@link SparkEnv} is initialized
     * (e.g. before driver bring-up) so callers default to the non-PerfIO path.
     */
    public static boolean isS3PerfEnabled() {
        Boolean cached = s3PerfEnabledCache;
        if (cached != null) {
            return cached;
        }
        SparkEnv env = SparkEnv.get();
        if (env == null) {
            return false;
        }
        boolean enabled = env.conf().getBoolean(PerfIOConf.S3PERF_ENABLED().key(), false);
        s3PerfEnabledCache = enabled;
        return enabled;
    }
}
