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

/** Avro block metadata. */
public final class BlockInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long blockStart;
  private final long blockSize;
  private final long dataSize;
  private final long count;

  public BlockInfo(long blockStart, long blockSize, long dataSize, long count) {
    this.blockStart = blockStart;
    this.blockSize = blockSize;
    this.dataSize = dataSize;
    this.count = count;
  }

  public long blockStart() {
    return blockStart;
  }

  public long blockSize() {
    return blockSize;
  }

  public long dataSize() {
    return dataSize;
  }

  public long count() {
    return count;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BlockInfo)) {
      return false;
    }
    BlockInfo other = (BlockInfo) obj;
    return blockStart == other.blockStart &&
        blockSize == other.blockSize &&
        dataSize == other.dataSize &&
        count == other.count;
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockStart, blockSize, dataSize, count);
  }

  @Override
  public String toString() {
    return "BlockInfo(" + blockStart + "," + blockSize + "," + dataSize + "," + count + ")";
  }
}
