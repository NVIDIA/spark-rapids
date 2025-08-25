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

package com.nvidia.spark.rapids.fileio;

import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * An io layer to access different underlying storages such as hdfs, aws s3, etc.
 *<br/>
 */
public interface RapidsFileIO extends Serializable{
    /**
     * Creates a new {@link RapidsInputFile} for the given path.
     * @param path The absolute path to the file.
     * @return a new {@link RapidsInputFile} for the given path
     * @throws IOException If the underlying file system throws IOException
     */
    RapidsInputFile newInputFile(String path) throws IOException;


    /**
     * Creates a new {@link RapidsInputFile} for the given path.
     * @param path The absolute path to the file.
     * @return a new {@link RapidsInputFile} for the given path
     * @throws IOException If the underlying file system throws IOException
     */
    default RapidsInputFile newInputFile(Path path) throws IOException {
        return newInputFile(path.toString());
    }
}
