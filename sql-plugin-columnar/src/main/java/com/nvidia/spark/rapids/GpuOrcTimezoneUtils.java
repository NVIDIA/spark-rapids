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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;

public final class GpuOrcTimezoneUtils {
  private static final ZoneId UTC = ZoneId.of("UTC");

  private GpuOrcTimezoneUtils() {
  }

  /**
   * Get the offset in microseconds for 2015-01-01 between JVM timezone and UTC timezone.
   *
   * @param jvmTz the JVM timezone to calculate the offset for
   * @return the offset in microseconds between the JVM timezone and UTC timezone
   */
  private static long getOffsetForJanuaryFirst2015(ZoneId jvmTz) {
    long t1 = LocalDateTime.of(2015, 1, 1, 0, 0, 0).atZone(jvmTz).toInstant()
        .getEpochSecond();
    long t2 = LocalDateTime.of(2015, 1, 1, 0, 0, 0).atZone(UTC).toInstant()
        .getEpochSecond();
    return (t2 - t1) * 1000000L;
  }

  private static <T extends ColumnView> T addToClose(List<ColumnView> toClose, T view) {
    toClose.add(view);
    return view;
  }

  /**
   * Recursively rebase timestamp columns in an input column view to the target timezone.
   * This handles nested list and struct types.
   */
  private static ColumnView rebaseTimestampRecursively(
      ColumnView col,
      List<ColumnView> toClose,
      long diffMicros) {
    DType dType = col.getType();
    if (dType.hasTimeResolution()) {
      assert dType.equals(DType.TIMESTAMP_MICROSECONDS) :
          "Only TIMESTAMP_MICROSECONDS is supported, but got " + dType;

      try (ColumnView longs = col.bitCastTo(DType.INT64);
           Scalar offsetScalar = Scalar.fromLong(diffMicros);
           ColumnVector rebased = longs.sub(offsetScalar)) {
        return rebased.castTo(DType.TIMESTAMP_MICROSECONDS);
      }
    } else if (DType.LIST.equals(dType)) {
      ColumnView child = addToClose(toClose, col.getChildColumnView(0));
      ColumnView newChild = rebaseTimestampRecursively(child, toClose, diffMicros);
      if (newChild != child) {
        return col.replaceListChild(addToClose(toClose, newChild));
      }
      return col;
    } else if (DType.STRUCT.equals(dType)) {
      ColumnView[] newViews = new ColumnView[col.getNumChildren()];
      for (int i = 0; i < newViews.length; i++) {
        ColumnView child = addToClose(toClose, col.getChildColumnView(i));
        ColumnView newChild = rebaseTimestampRecursively(child, toClose, diffMicros);
        if (newChild != child) {
          addToClose(toClose, newChild);
        }
        newViews[i] = newChild;
      }
      return new ColumnView(col.getType(), col.getRowCount(), Optional.of(col.getNullCount()),
          col.getValid(), col.getOffsets(), newViews);
    }
    return col;
  }

  /**
   * Rebase timestamp columns in the input table to the system default timezone. If the system's
   * default timezone is UTC, this returns the input table as-is. Otherwise the input table is
   * closed before returning.
   *
   * @param input the input table
   * @return a table with timestamp columns rebased
   */
  public static Table rebaseTimeZone(Table input) {
    ZoneId toZoneId = ZoneId.systemDefault();

    if (UTC.equals(toZoneId)) {
      return input;
    }

    long diffMicros = getOffsetForJanuaryFirst2015(toZoneId);
    try (Table ignored = input) {
      ColumnVector[] newColumns = new ColumnVector[input.getNumberOfColumns()];
      try {
        for (int colIdx = 0; colIdx < newColumns.length; colIdx++) {
          ColumnVector col = input.getColumn(colIdx);
          List<ColumnView> toClose = new ArrayList<>();
          try {
            ColumnView rebased = rebaseTimestampRecursively(col, toClose, diffMicros);
            if (col == rebased) {
              newColumns[colIdx] = col.incRefCount();
            } else {
              toClose.add(rebased);
              newColumns[colIdx] = rebased.copyToColumnVector();
            }
          } finally {
            closeAll(toClose);
          }
        }
        return new Table(newColumns);
      } finally {
        closeAll(newColumns);
      }
    }
  }

  private static void closeAll(ColumnView[] views) {
    for (ColumnView view : views) {
      if (view != null) {
        view.close();
      }
    }
  }

  private static void closeAll(List<ColumnView> views) {
    RuntimeException firstException = null;
    for (ColumnView view : views) {
      try {
        view.close();
      } catch (RuntimeException e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }
}
