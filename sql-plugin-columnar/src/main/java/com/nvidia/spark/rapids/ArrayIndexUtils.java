/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Table;

public final class ArrayIndexUtils {
  private ArrayIndexUtils() {}

  public static final class IndexAndNumElement {
    private final int index;
    private final int numElements;

    IndexAndNumElement(int index, int numElements) {
      this.index = index;
      this.numElements = numElements;
    }

    public int getIndex() {
      return index;
    }

    public int getNumElements() {
      return numElements;
    }
  }

  /**
   * Return the first int value (should be valid) in {@code indices} and
   * {@code numElements} where the corresponding row in {@code mask} is true.
   * Null rows in {@code mask} are skipped.
   *
   * <p>{@code indices} and {@code numElements} should be int columns with the
   * same row count. {@code mask} should be a boolean column with the same row
   * count. Otherwise, behavior is undefined.
   */
  public static IndexAndNumElement firstIndexAndNumElementUnchecked(
      ColumnView mask, ColumnVector indices, ColumnVector numElements) {
    try (Table indexTable = new Table(indices, numElements);
         Table filteredTable = indexTable.filter(mask)) {
      assert filteredTable.getRowCount() > 0;
      int index;
      try (HostColumnVector indicesH = filteredTable.getColumn(0).copyToHost()) {
        assert !indicesH.isNull(0);
        index = indicesH.getInt(0);
      }
      int numElement;
      try (HostColumnVector numElemsH = filteredTable.getColumn(1).copyToHost()) {
        assert !numElemsH.isNull(0);
        numElement = numElemsH.getInt(0);
      }
      return new IndexAndNumElement(index, numElement);
    }
  }
}
