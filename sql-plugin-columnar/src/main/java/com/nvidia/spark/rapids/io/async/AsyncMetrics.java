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
 * Scheduling and execution timings for an async task.
 */
public class AsyncMetrics implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long scheduleTimeMs;
  private final long executionTimeMs;

  public AsyncMetrics(long scheduleTimeMs, long executionTimeMs) {
    this.scheduleTimeMs = scheduleTimeMs;
    this.executionTimeMs = executionTimeMs;
  }

  public long scheduleTimeMs() {
    return scheduleTimeMs;
  }

  public long executionTimeMs() {
    return executionTimeMs;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AsyncMetrics)) {
      return false;
    }
    AsyncMetrics that = (AsyncMetrics) other;
    return scheduleTimeMs == that.scheduleTimeMs
        && executionTimeMs == that.executionTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheduleTimeMs, executionTimeMs);
  }

  @Override
  public String toString() {
    return "AsyncMetrics(" + scheduleTimeMs + "," + executionTimeMs + ")";
  }
}
