/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.kudo;

public class SliceInfo {
  final int offset;
  final int rowCount;
  private final SlicedValidityBufferInfo validityBufferInfo;

  SliceInfo(int offset, int rowCount) {
    this.offset = offset;
    this.rowCount = rowCount;
    this.validityBufferInfo = SlicedValidityBufferInfo.calc(offset, rowCount);
  }

  SlicedValidityBufferInfo getValidityBufferInfo() {
    return validityBufferInfo;
  }

  public int getOffset() {
    return offset;
  }

  public int getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "SliceInfo{" +
        "offset=" + offset +
        ", rowCount=" + rowCount +
        ", validityBufferInfo=" + validityBufferInfo +
        '}';
  }
}
