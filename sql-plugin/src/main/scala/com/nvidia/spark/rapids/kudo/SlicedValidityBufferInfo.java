/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/**
 * A simple utility class to hold information about serializing/deserializing sliced validity buffer.
 */
class SlicedValidityBufferInfo {
  private final int bufferOffset;
  private final int bufferLength;
  /// The bit offset within the buffer where the slice starts
  private final int beginBit;

  SlicedValidityBufferInfo(int bufferOffset, int bufferLength, int beginBit) {
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
    this.beginBit = beginBit;
  }

  @Override
  public String toString() {
    return "SlicedValidityBufferInfo{" + "bufferOffset=" + bufferOffset + ", bufferLength=" + bufferLength +
        ", beginBit=" + beginBit + '}';
  }

  public int getBufferOffset() {
    return bufferOffset;
  }

  public int getBufferLength() {
    return bufferLength;
  }

  public int getBeginBit() {
    return beginBit;
  }

  static SlicedValidityBufferInfo calc(int rowOffset, int numRows) {
    if (rowOffset < 0) {
      throw new IllegalArgumentException("rowOffset must be >= 0, but was " + rowOffset);
    }
    if (numRows < 0) {
      throw new IllegalArgumentException("numRows must be >= 0, but was " + numRows);
    }
    int bufferOffset = rowOffset / 8;
    int beginBit = rowOffset % 8;
    int bufferLength = 0;
    if (numRows > 0) {
      bufferLength = (rowOffset + numRows - 1) / 8 - bufferOffset + 1;
    }
    return new SlicedValidityBufferInfo(bufferOffset, bufferLength, beginBit);
  }
}
