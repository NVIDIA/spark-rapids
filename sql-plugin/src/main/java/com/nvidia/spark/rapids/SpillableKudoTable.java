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
package com.nvidia.spark.rapids;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.kudo.KudoTable;
import com.nvidia.spark.rapids.jni.kudo.KudoTableHeader;

public class SpillableKudoTable extends KudoTable {
  private SpillableHostBuffer shb;

  public SpillableKudoTable(KudoTableHeader header, HostMemoryBuffer buffer) {
    super(header, null);
    this.length = buffer.getLength();
    this.shb = SpillableHostBuffer.apply(buffer, buffer.getLength(),
        SpillPriorities$.MODULE$.ACTIVE_BATCHING_PRIORITY());
  }

  // NOT GOOD: The current class is "Spillable", but not always spillable, we have to call this to
  // guarantee that it is spillable
  public void guaranteeSpillable() {
    if (buffer != null) {
      // NOT GOOD: it's dangerous to close this buffer without knowing if it's being used elsewhere.
      // Remember, the caller of SpillableKudoTable.getBuffer() did not inc the ref count, so it's
      // really going to be closed!
      buffer.close();
      buffer = null;
    }
  }

  @Override
  public HostMemoryBuffer getBuffer() {
    if (buffer == null) {
      buffer = shb.getHostBuffer();
    }
    return buffer;
  }

  @Override
  public String toString() {
    return "SpillableKudoTable{header=" + this.header + ", shb=" + this.shb + '}';
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (shb != null) {
      shb.close();
    }
  }
}