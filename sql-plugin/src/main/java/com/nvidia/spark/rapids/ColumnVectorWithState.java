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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * Base class that extends ColumnVector and includes batch state management.
 * This class provides state tracking for batch processing, specifically
 * for tracking whether a batch is final or a sub-partition of the final batch.
 */
public abstract class ColumnVectorWithState extends ColumnVector {

  private volatile boolean isFinalBatch = false;
  // If it is a sub-partition of the only batch of current task
  private volatile boolean isSubPartitionOfOnlyBatch = false;

  protected ColumnVectorWithState(DataType type) {
    super(type);
  }

  /**
   * Set if this is a part of the final batch for this partition or not.
   * @param isFinal true if this is part of the final batch or false if unknown.
   */
  public final void setFinalBatch(boolean isFinal) {
    isFinalBatch = isFinal;
  }

  /**
   * Is this the final batch or is it unknown.
   * @return true if it is known to be a part of the final batch, else false if it is unknown
   */
  public boolean isKnownFinalBatch() {
    return isFinalBatch;
  }

  /**
   * Set if this is a sub-partition of the only batch for this task or not.
   */
  public final void setSubPartitionOfOnlyBatch(boolean isFinal) {
    isSubPartitionOfOnlyBatch = isFinal;
  }

  /**
   * Is this a sub-partition of the only batch or is it unknown.
   */
  public boolean isKnownSubPartitionOfOnlyBatch() {
    return isSubPartitionOfOnlyBatch;
  }
}
