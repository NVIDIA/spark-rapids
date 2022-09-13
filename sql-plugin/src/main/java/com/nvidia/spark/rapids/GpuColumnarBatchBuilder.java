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

public class GpuColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
  private final ai.rapids.cudf.HostColumnVector.ColumnBuilder[] builders;

  /**
    * A collection of builders for building up columnar data.
    * @param schema the schema of the batch.
    * @param rows the maximum number of rows in this batch.
    */
  public GpuColumnarBatchBuilder(StructType schema, int rows) {
    fields = schema.fields();
    int len = fields.length;
    builders = new ai.rapids.cudf.HostColumnVector.ColumnBuilder[len];
    boolean success = false;
    try {
      for (int i = 0; i < len; i++) {
        StructField field = fields[i];
        builders[i] = new HostColumnVector.ColumnBuilder(
          GpuColumnVector.convertFrom(field.dataType(), field.nullable()), rows);
      }
      success = true;
    } finally {
      if (!success) {
        for (ai.rapids.cudf.HostColumnVector.ColumnBuilder b: builders) {
          if (b != null) {
            b.close();
          }
        }
      }
    }
  }

  public void copyColumnar(ColumnVector cv, int colNum, boolean nullable, int rows) {
    HostColumnarToGpu.columnarCopy(cv, builder(colNum), rows);
  }

  public ai.rapids.cudf.HostColumnVector.ColumnBuilder builder(int i) {
    return builders[i];
  }

  protected int buildersLength() {
    return builders.length;
  }

  protected ColumnVector buildAndPutOnDevice(int builderIndex) {
    ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
    GpuColumnVector gcv = new GpuColumnVector(fields[builderIndex].dataType(), cv);
    builders[builderIndex] = null;
    return gcv;
  }

  public HostColumnVector[] buildHostColumns() {
    HostColumnVector[] vectors = new HostColumnVector[builders.length];
    try {
      for (int i = 0; i < builders.length; i++) {
        vectors[i] = builders[i].build();
        builders[i] = null;
      }
      HostColumnVector[] result = vectors;
      vectors = null;
      return result;
    } finally {
      if (vectors != null) {
        for (HostColumnVector v : vectors) {
          if (v != null) {
            v.close();
          }
        }
      }
    }
  }

  @Override
  public void close() {
    for (ai.rapids.cudf.HostColumnVector.ColumnBuilder b: builders) {
      if (b != null) {
        b.close();
      }
    }
  }
}