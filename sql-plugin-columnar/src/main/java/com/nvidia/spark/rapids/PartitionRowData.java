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

package com.nvidia.spark.rapids;

import java.util.Objects;

import org.apache.spark.sql.catalyst.InternalRow;

/** Partition value and replication count. */
public final class PartitionRowData {
  private final InternalRow rowValue;
  private final int rowNum;

  public PartitionRowData(InternalRow rowValue, int rowNum) {
    this.rowValue = rowValue;
    this.rowNum = rowNum;
  }

  public InternalRow rowValue() {
    return rowValue;
  }

  public int rowNum() {
    return rowNum;
  }

  public static PartitionRowData[] from(InternalRow[] rowValues, int[] rowNums) {
    int length = Math.min(rowValues.length, rowNums.length);
    PartitionRowData[] result = new PartitionRowData[length];
    for (int i = 0; i < length; i++) {
      result[i] = new PartitionRowData(rowValues[i], rowNums[i]);
    }
    return result;
  }

  public static PartitionRowData[] from(InternalRow[] rowValues, long[] rowNums) {
    int length = Math.min(rowValues.length, rowNums.length);
    PartitionRowData[] result = new PartitionRowData[length];
    for (int i = 0; i < length; i++) {
      long rowNum = rowNums[i];
      if (rowNum > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "Row number " + rowNum + " exceeds max value of an integer.");
      }
      result[i] = new PartitionRowData(rowValues[i], (int) rowNum);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PartitionRowData)) {
      return false;
    }
    PartitionRowData other = (PartitionRowData) obj;
    return rowNum == other.rowNum && Objects.equals(rowValue, other.rowValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowValue, rowNum);
  }

  @Override
  public String toString() {
    return "PartitionRowData(" + rowValue + "," + rowNum + ")";
  }
}
