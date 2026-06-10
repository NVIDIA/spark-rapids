/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

public class AutoCloseableTargetSize implements AutoCloseable, Serializable {
  private static final long serialVersionUID = 1L;

  public final long targetSize;
  public final long minSize;
  public final long dataSize;

  public AutoCloseableTargetSize(long targetSize, long minSize) {
    this(targetSize, minSize, 0);
  }

  public AutoCloseableTargetSize(long targetSize, long minSize, long dataSize) {
    this.targetSize = targetSize;
    this.minSize = minSize;
    this.dataSize = dataSize;
  }

  public long targetSize() {
    return targetSize;
  }

  public long minSize() {
    return minSize;
  }

  public long dataSize() {
    return dataSize;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AutoCloseableTargetSize)) {
      return false;
    }
    AutoCloseableTargetSize that = (AutoCloseableTargetSize) other;
    return targetSize == that.targetSize &&
        minSize == that.minSize &&
        dataSize == that.dataSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetSize, minSize, dataSize);
  }

  @Override
  public String toString() {
    return "AutoCloseableTargetSize(" + targetSize + "," + minSize + "," + dataSize + ")";
  }
}
