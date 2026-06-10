/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.kudo.KudoTable;
import com.nvidia.spark.rapids.jni.kudo.KudoTableHeader;

public class SpillableKudoTable implements AutoCloseable {
  public final KudoTableHeader header;
  public final long length;
  private final SpillableHostBuffer shb;

  public SpillableKudoTable(KudoTableHeader header, long length, SpillableHostBuffer shb) {
    this.header = header;
    this.length = length;
    this.shb = shb;
  }

  public static SpillableKudoTable from(KudoTableHeader header, HostMemoryBuffer buffer) {
    if (buffer == null) {
      return new SpillableKudoTable(header, 0, null);
    } else {
      return new SpillableKudoTable(
          header,
          buffer.getLength(),
          SpillableHostBuffer.apply(
              buffer,
              buffer.getLength(),
              SpillPriorities.ACTIVE_BATCHING_PRIORITY));
    }
  }

  public KudoTable makeKudoTable() {
    if (shb == null) {
      return new KudoTable(header, null);
    } else {
      return new KudoTable(header, shb.getHostBuffer());
    }
  }

  @Override
  public String toString() {
    return "SpillableKudoTable{header=" + header + ", shb=" + shb + '}';
  }

  @Override
  public void close() {
    if (shb != null) {
      shb.close();
    }
  }
}
