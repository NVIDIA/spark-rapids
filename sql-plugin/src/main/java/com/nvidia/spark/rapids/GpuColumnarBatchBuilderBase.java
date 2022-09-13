/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.ArrowColumnBuilder;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.shims.GpuTypeShims;
import org.apache.arrow.memory.ReferenceManager;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public abstract class GpuColumnarBatchBuilderBase implements AutoCloseable {
  protected StructField[] fields;

  public abstract void close();
  public abstract void copyColumnar(ColumnVector cv, int colNum, boolean nullable, int rows);

  protected abstract ColumnVector buildAndPutOnDevice(int builderIndex);
  protected abstract int buildersLength();

  public ColumnarBatch build(int rows) {
    int buildersLen = buildersLength();
    ColumnVector[] vectors = new ColumnVector[buildersLen];
    boolean success = false;
    try {
      for (int i = 0; i < buildersLen; i++) {
        vectors[i] = buildAndPutOnDevice(i);
      }
      ColumnarBatch ret = new ColumnarBatch(vectors, rows);
      success = true;
      return ret;
    } finally {
      if (!success) {
        for (ColumnVector vec: vectors) {
          if (vec != null) {
            vec.close();
          }
        }
      }
    }
  }
}
