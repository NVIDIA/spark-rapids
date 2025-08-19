/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation {@link RapidsFileIO} using the hadoop file system.
 * <br/>
 */
public class HadoopFileIO implements RapidsFileIO {
    private final SerializableConfiguration hadoopConf;

    public HadoopFileIO(Configuration hadoopConf) {
        Objects.requireNonNull(hadoopConf, "hadoopConf can't be null");
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    @Override
    public RapidsInputFile newInputFile(String path) throws IOException {
        return this.newInputFile(new Path(path));
    }

    @Override
    public RapidsInputFile newInputFile(Path path) throws IOException {
        return HadoopInputFile.create(path, hadoopConf.value());
    }
}
