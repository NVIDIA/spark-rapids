/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async;

import java.io.Serializable;
import java.util.Objects;

/**
 * Mutable throttling counters updated by ThrottlingExecutor.
 */
public class ThrottlingExecutorStats implements Serializable {
  private static final long serialVersionUID = 1L;

  public int numTasksScheduled;
  public long accumulatedThrottleTimeNs;
  public long minThrottleTimeNs;
  public long maxThrottleTimeNs;

  public ThrottlingExecutorStats(
      int numTasksScheduled,
      long accumulatedThrottleTimeNs,
      long minThrottleTimeNs,
      long maxThrottleTimeNs) {
    this.numTasksScheduled = numTasksScheduled;
    this.accumulatedThrottleTimeNs = accumulatedThrottleTimeNs;
    this.minThrottleTimeNs = minThrottleTimeNs;
    this.maxThrottleTimeNs = maxThrottleTimeNs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ThrottlingExecutorStats)) {
      return false;
    }
    ThrottlingExecutorStats that = (ThrottlingExecutorStats) other;
    return numTasksScheduled == that.numTasksScheduled
        && accumulatedThrottleTimeNs == that.accumulatedThrottleTimeNs
        && minThrottleTimeNs == that.minThrottleTimeNs
        && maxThrottleTimeNs == that.maxThrottleTimeNs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        numTasksScheduled, accumulatedThrottleTimeNs, minThrottleTimeNs, maxThrottleTimeNs);
  }

  @Override
  public String toString() {
    return "ThrottlingExecutorStats(" + numTasksScheduled + ","
        + accumulatedThrottleTimeNs + "," + minThrottleTimeNs + ","
        + maxThrottleTimeNs + ")";
  }
}
