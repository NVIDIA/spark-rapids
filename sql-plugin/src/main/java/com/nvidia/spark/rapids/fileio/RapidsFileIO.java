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
 * An io layer to access different underlying storages such as hdfs, aws s3.
 *<br/>
 *<br/>
 * This layer is heavily inspired by io abstractions layers like
 * <a href="https://github.com/apache/iceberg/blob/50d310aef17908f03f595d520cd751527483752a/api/src/main/java/org/apache/iceberg/io/FileIO.java#L36">iceberg's FileIO</a>,
 * <a href="https://github.com/apache/parquet-java/tree/master/parquet-common/src/main/java/org/apache/parquet/io">parquet io</a>,
 * <a href="https://github.com/trinodb/trino/blob/master/lib/trino-filesystem/src/main/java/io/trino/filesystem/TrinoFileSystem.java">trino filesystem</a>
 */
public interface RapidsFileIO extends Serializable, Closeable {
    RapidsInputFile open(String path) throws IOException;


    default RapidsInputFile open(Path path) throws IOException {
        return open(path.toString());
    }
}
