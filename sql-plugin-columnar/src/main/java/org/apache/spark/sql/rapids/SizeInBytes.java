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
import java.util.Objects;

public final class SizeInBytes implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final String[] SIZE_UNIT_NAMES = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};

  private final Long value;

  public SizeInBytes(Long value) {
    this.value = value;
  }

  public Long value() {
    return value;
  }

  @Override
  public String toString() {
    long unitVal = value;
    long remainVal = 0;
    int unitIndex = 0;
    while (unitIndex < SIZE_UNIT_NAMES.length && unitVal >= 1024) {
      long nextUnitVal = unitVal >> 10;
      remainVal = unitVal - (nextUnitVal << 10);
      unitVal = nextUnitVal;
      unitIndex += 1;
    }
    String finalVal = String.format("%.2f", unitVal + (remainVal / 1024.0));
    return finalVal + SIZE_UNIT_NAMES[unitIndex] + " (" + value + " bytes)";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SizeInBytes)) {
      return false;
    }
    SizeInBytes other = (SizeInBytes) obj;
    return Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
