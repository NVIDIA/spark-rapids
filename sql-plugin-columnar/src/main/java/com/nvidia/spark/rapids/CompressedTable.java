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

import java.util.Objects;

import ai.rapids.cudf.DeviceMemoryBuffer;
import com.nvidia.spark.rapids.format.TableMeta;

/**
 * Compressed table descriptor.
 */
public class CompressedTable implements AutoCloseable {
  public final long compressedSize;
  public final TableMeta meta;
  public final DeviceMemoryBuffer buffer;

  public CompressedTable(long compressedSize, TableMeta meta, DeviceMemoryBuffer buffer) {
    this.compressedSize = compressedSize;
    this.meta = meta;
    this.buffer = buffer;
  }

  public long compressedSize() {
    return compressedSize;
  }

  public TableMeta meta() {
    return meta;
  }

  public DeviceMemoryBuffer buffer() {
    return buffer;
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CompressedTable)) {
      return false;
    }
    CompressedTable that = (CompressedTable) other;
    return compressedSize == that.compressedSize &&
        Objects.equals(meta, that.meta) &&
        Objects.equals(buffer, that.buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compressedSize, meta, buffer);
  }

  @Override
  public String toString() {
    return "CompressedTable(" + compressedSize + "," + meta + "," + buffer + ")";
  }
}
