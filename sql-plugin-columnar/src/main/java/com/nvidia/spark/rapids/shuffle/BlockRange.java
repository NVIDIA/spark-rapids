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

package com.nvidia.spark.rapids.shuffle;

import java.util.Objects;

/** Byte range for a block. */
public final class BlockRange<T extends BlockWithSize> {
  private final T block;
  private final long rangeStart;
  private final long rangeEnd;

  public BlockRange(T block, long rangeStart, long rangeEnd) {
    if (rangeStart >= rangeEnd) {
      throw new IllegalArgumentException(
          "requirement failed: Instantiated a BlockRange with invalid boundaries: " +
              rangeStart + " to " + rangeEnd);
    }
    this.block = block;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
  }

  public T block() {
    return block;
  }

  public long rangeStart() {
    return rangeStart;
  }

  public long rangeEnd() {
    return rangeEnd;
  }

  public long rangeSize() {
    return rangeEnd - rangeStart;
  }

  public boolean isComplete() {
    return rangeEnd == block.size();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BlockRange)) {
      return false;
    }
    BlockRange<?> other = (BlockRange<?>) obj;
    return rangeStart == other.rangeStart &&
        rangeEnd == other.rangeEnd &&
        Objects.equals(block, other.block);
  }

  @Override
  public int hashCode() {
    return Objects.hash(block, rangeStart, rangeEnd);
  }

  @Override
  public String toString() {
    return "BlockRange(" + block + "," + rangeStart + "," + rangeEnd + ")";
  }
}
