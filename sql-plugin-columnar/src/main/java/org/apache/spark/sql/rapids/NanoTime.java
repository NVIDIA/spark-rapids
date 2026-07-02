/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class NanoTime implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Long value;

  public NanoTime(Long value) {
    this.value = value;
  }

  public Long value() {
    return value;
  }

  @Override
  public String toString() {
    long hours = TimeUnit.NANOSECONDS.toHours(value);
    long remaining = value - TimeUnit.HOURS.toNanos(hours);
    long minutes = TimeUnit.NANOSECONDS.toMinutes(remaining);
    remaining -= TimeUnit.MINUTES.toNanos(minutes);
    double seconds = ((double) remaining) / TimeUnit.SECONDS.toNanos(1);
    return String.format(Locale.US, "%02d:%02d:%06.3f", hours, minutes, seconds);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof NanoTime)) {
      return false;
    }
    NanoTime other = (NanoTime) obj;
    return Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
