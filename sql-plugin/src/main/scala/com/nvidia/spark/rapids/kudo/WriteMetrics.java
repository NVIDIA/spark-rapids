/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
 * This class contains metrics for serializing table using kudo format.
 */
public class WriteMetrics {
  private long copyBufferTime;
  private long writtenBytes;


  public WriteMetrics() {
    this.copyBufferTime = 0;
    this.writtenBytes = 0;
  }

  /**
   * Get the time spent on copying the buffer.
   */
  public long getCopyBufferTime() {
    return copyBufferTime;
  }

  public void addCopyBufferTime(long time) {
    copyBufferTime += time;
  }

  /**
   * Get the number of bytes written.
   */
  public long getWrittenBytes() {
    return writtenBytes;
  }

  public void addWrittenBytes(long bytes) {
    writtenBytes += bytes;
  }
}
