/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.io.Serializable;
import java.util.Objects;

/**
 * Codec-specific table compression settings.
 */
public class TableCompressionCodecConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long lz4ChunkSize;
  private final long zstdChunkSize;

  public TableCompressionCodecConfig(long lz4ChunkSize, long zstdChunkSize) {
    this.lz4ChunkSize = lz4ChunkSize;
    this.zstdChunkSize = zstdChunkSize;
  }

  public long lz4ChunkSize() {
    return lz4ChunkSize;
  }

  public long zstdChunkSize() {
    return zstdChunkSize;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TableCompressionCodecConfig)) {
      return false;
    }
    TableCompressionCodecConfig that = (TableCompressionCodecConfig) other;
    return lz4ChunkSize == that.lz4ChunkSize && zstdChunkSize == that.zstdChunkSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lz4ChunkSize, zstdChunkSize);
  }

  @Override
  public String toString() {
    return "TableCompressionCodecConfig(" + lz4ChunkSize + "," + zstdChunkSize + ")";
  }
}
