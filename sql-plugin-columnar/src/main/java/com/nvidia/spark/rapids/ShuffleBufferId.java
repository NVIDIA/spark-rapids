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

package com.nvidia.spark.rapids;

import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.storage.ShuffleBlockId;

/** Identifier for a shuffle buffer that holds the data for a table. */
public final class ShuffleBufferId implements Serializable {
  private static final long serialVersionUID = 0L;

  private final ShuffleBlockId blockId;
  private final int tableId;
  private final int shuffleId;
  private final long mapId;

  public ShuffleBufferId(ShuffleBlockId blockId, int tableId) {
    this.blockId = blockId;
    this.tableId = tableId;
    this.shuffleId = blockId.shuffleId();
    this.mapId = blockId.mapId();
  }

  public ShuffleBlockId blockId() {
    return blockId;
  }

  public int tableId() {
    return tableId;
  }

  public int shuffleId() {
    return shuffleId;
  }

  public long mapId() {
    return mapId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ShuffleBufferId)) {
      return false;
    }
    ShuffleBufferId that = (ShuffleBufferId) other;
    return tableId == that.tableId && Objects.equals(blockId, that.blockId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockId, tableId);
  }

  @Override
  public String toString() {
    return "ShuffleBufferId(" + blockId + "," + tableId + ")";
  }
}
