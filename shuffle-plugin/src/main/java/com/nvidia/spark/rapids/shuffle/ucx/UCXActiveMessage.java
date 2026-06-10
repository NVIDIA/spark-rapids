/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle.ucx;

import java.util.Objects;

/** Active message id and dynamic header used by UCX request/response handlers. */
public final class UCXActiveMessage {
  private final int activeMessageId;
  private final long header;
  private final boolean forceRndv;

  public UCXActiveMessage(int activeMessageId, long header, boolean forceRndv) {
    this.activeMessageId = activeMessageId;
    this.header = header;
    this.forceRndv = forceRndv;
  }

  public int activeMessageId() {
    return activeMessageId;
  }

  public long header() {
    return header;
  }

  public boolean forceRndv() {
    return forceRndv;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof UCXActiveMessage)) {
      return false;
    }
    UCXActiveMessage other = (UCXActiveMessage) obj;
    return activeMessageId == other.activeMessageId &&
        header == other.header &&
        forceRndv == other.forceRndv;
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeMessageId, header, forceRndv);
  }

  @Override
  public String toString() {
    return "[amId=" + String.format("0x%08X", activeMessageId) +
        ", hdr=" + String.format("0x%016X", header) + "]";
  }
}
