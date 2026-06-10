/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

/** Mutable Avro block metadata for iterator reuse. */
public final class MutableBlockInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private long blockSize;
  private long dataSize;
  private long count;

  public MutableBlockInfo(long blockSize, long dataSize, long count) {
    this.blockSize = blockSize;
    this.dataSize = dataSize;
    this.count = count;
  }

  public long blockSize() {
    return blockSize;
  }

  public void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
  }

  public long dataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public long count() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MutableBlockInfo)) {
      return false;
    }
    MutableBlockInfo other = (MutableBlockInfo) obj;
    return blockSize == other.blockSize && dataSize == other.dataSize && count == other.count;
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockSize, dataSize, count);
  }

  @Override
  public String toString() {
    return "MutableBlockInfo(" + blockSize + "," + dataSize + "," + count + ")";
  }
}
