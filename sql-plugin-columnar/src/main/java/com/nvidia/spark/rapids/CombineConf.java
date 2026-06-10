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

public class CombineConf implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long combineThresholdSize;
  private final int combineWaitTime;

  public CombineConf(long combineThresholdSize, int combineWaitTime) {
    this.combineThresholdSize = combineThresholdSize;
    this.combineWaitTime = combineWaitTime;
  }

  public long combineThresholdSize() {
    return combineThresholdSize;
  }

  public int combineWaitTime() {
    return combineWaitTime;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CombineConf)) {
      return false;
    }
    CombineConf that = (CombineConf) other;
    return combineThresholdSize == that.combineThresholdSize &&
        combineWaitTime == that.combineWaitTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(combineThresholdSize, combineWaitTime);
  }

  @Override
  public String toString() {
    return "CombineConf(" + combineThresholdSize + "," + combineWaitTime + ")";
  }
}
