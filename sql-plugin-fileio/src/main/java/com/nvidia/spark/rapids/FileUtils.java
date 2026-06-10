/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class FileUtils {
  private FileUtils() {}

  public static final class TempFile {
    private final FSDataOutputStream outputStream;
    private final Path path;

    TempFile(FSDataOutputStream outputStream, Path path) {
      this.outputStream = outputStream;
      this.path = path;
    }

    public FSDataOutputStream getOutputStream() {
      return outputStream;
    }

    public Path getPath() {
      return path;
    }
  }

  public static TempFile createTempFile(
      Configuration conf, String pathPrefix, String pathSuffix) throws IOException {
    FileSystem fs = new Path(pathPrefix).getFileSystem(conf);
    Random rnd = new Random();
    String suffix = pathSuffix != null ? pathSuffix : "";
    while (true) {
      Path path = new Path(pathPrefix + rnd.nextInt(Integer.MAX_VALUE) + suffix);
      if (!fs.exists(path)) {
        try {
          return new TempFile(fs.create(path, false), path);
        } catch (FileAlreadyExistsException e) {
          // Retry if another writer won the race between exists and create.
        }
      }
    }
  }
}
