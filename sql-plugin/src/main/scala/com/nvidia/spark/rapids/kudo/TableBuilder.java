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

import ai.rapids.cudf.*;
import com.nvidia.spark.rapids.jni.Arms;
import com.nvidia.spark.rapids.jni.schema.SchemaVisitor;

import java.util.Arrays;
import java.util.List;

import static com.nvidia.spark.rapids.jni.Preconditions.ensure;
import static java.util.Objects.requireNonNull;

/**
 * This class is used to build a cudf table from a list of column view info, and a device buffer.
 */
class TableBuilder implements SchemaVisitor<ColumnView, ColumnViewInfo, Table>, AutoCloseable {
  private int curViewInfoIdx;
  private int curViewIdx;
  private final DeviceMemoryBuffer buffer;
  private final ColumnViewInfo[] colViewInfoList;
  private final ColumnView[] columnViewList;

  public TableBuilder(ColumnViewInfo[] colViewInfoList, DeviceMemoryBuffer buffer) {
    requireNonNull(colViewInfoList, "colViewInfoList cannot be null");
    ensure(colViewInfoList.length != 0, "colViewInfoList cannot be empty");
    requireNonNull(buffer, "Device buffer can't be null!");

    this.curViewInfoIdx = 0;
    this.curViewIdx = 0;
    this.buffer = buffer;
    this.colViewInfoList = colViewInfoList;
    this.columnViewList = new ColumnView[colViewInfoList.length];
  }

  @Override
  public Table visitTopSchema(Schema schema, List<ColumnView> children) {
    // When this method is called, the ownership of the column views in `columnViewList` has been transferred to
    // `children`, so we need to clear `columnViewList`.
    Arrays.fill(columnViewList, null);
    try {
      try (CloseableArray<ColumnVector> arr = CloseableArray.wrap(new ColumnVector[children.size()])) {
        for (int i = 0; i < children.size(); i++) {
          ColumnView colView = children.set(i, null);
          arr.set(i, ColumnVector.fromViewWithContiguousAllocation(colView.getNativeView(), buffer));
        }

        return new Table(arr.getArray());
      }
    } finally {
      Arms.closeAll(columnViewList);
    }
  }

  @Override
  public ColumnView visitStruct(Schema structType, List<ColumnView> children) {
    ColumnViewInfo colViewInfo = getCurrentColumnViewInfo();

    ColumnView[] childrenView = children.toArray(new ColumnView[0]);
    ColumnView columnView = colViewInfo.buildColumnView(buffer, childrenView);
    columnViewList[curViewIdx] = columnView;
    curViewIdx += 1;
    curViewInfoIdx += 1;
    return columnView;
  }

  @Override
  public ColumnViewInfo preVisitList(Schema listType) {
    ColumnViewInfo colViewInfo = getCurrentColumnViewInfo();

    curViewInfoIdx += 1;
    return colViewInfo;
  }

  @Override
  public ColumnView visitList(Schema listType, ColumnViewInfo colViewInfo, ColumnView childResult) {

    ColumnView[] children = new ColumnView[]{childResult};

    ColumnView view = colViewInfo.buildColumnView(buffer, children);
    columnViewList[curViewIdx] = view;
    curViewIdx += 1;
    return view;
  }

  @Override
  public ColumnView visit(Schema primitiveType) {
    ColumnViewInfo colViewInfo = getCurrentColumnViewInfo();

    ColumnView columnView = colViewInfo.buildColumnView(buffer, null);
    columnViewList[curViewIdx] = columnView;
    curViewIdx += 1;
    curViewInfoIdx += 1;
    return columnView;
  }

  private ColumnViewInfo getCurrentColumnViewInfo() {
    return colViewInfoList[curViewInfoIdx];
  }

  @Override
  public void close() throws Exception {
    Arms.closeAll(columnViewList);
  }
}
