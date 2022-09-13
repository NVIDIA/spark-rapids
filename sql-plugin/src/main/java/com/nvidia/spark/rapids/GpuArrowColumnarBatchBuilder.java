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

public final class GpuArrowColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
  private final ai.rapids.cudf.ArrowColumnBuilder[] builders;

  private final ArrowBufReferenceHolder[] referenceHolders;

  /**
    * A collection of builders for building up columnar data from Arrow data.
    * @param schema the schema of the batch.
    */
  public GpuArrowColumnarBatchBuilder(StructType schema) {
    fields = schema.fields();
    int len = fields.length;
    builders = new ai.rapids.cudf.ArrowColumnBuilder[len];
    referenceHolders = new ArrowBufReferenceHolder[len];
    boolean success = false;

    try {
      for (int i = 0; i < len; i++) {
        StructField field = fields[i];
        builders[i] = new ai.rapids.cudf.ArrowColumnBuilder(
          GpuColumnVector.convertFrom(field.dataType(), field.nullable()));
        referenceHolders[i] = new ArrowBufReferenceHolder();
      }
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
  }

  protected int buildersLength() {
    return builders.length;
  }

  protected ColumnVector buildAndPutOnDevice(int builderIndex) {
    ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
    GpuColumnVector gcv = new GpuColumnVector(fields[builderIndex].dataType(), cv);
    referenceHolders[builderIndex].releaseReferences();
    builders[builderIndex] = null;
    return gcv;
  }

  public void copyColumnar(ColumnVector cv, int colNum, boolean ignored, int rows) {
    referenceHolders[colNum].addReferences(
      HostColumnarToGpu.arrowColumnarCopy(cv, builder(colNum), rows)
    );
  }

  public ai.rapids.cudf.ArrowColumnBuilder builder(int i) {
    return builders[i];
  }

  @Override
  public void close() {
    for (ai.rapids.cudf.ArrowColumnBuilder b: builders) {
      if (b != null) {
        b.close();
      }
    }
    for (ArrowBufReferenceHolder holder: referenceHolders) {
      holder.releaseReferences();
    }
  }

  private static final class ArrowBufReferenceHolder {
    private final List<ReferenceManager> references = new ArrayList<>();

    public void addReferences(List<ReferenceManager> refs) {
      references.addAll(refs);
      refs.forEach(ReferenceManager::retain);
    }

    public void releaseReferences() {
      if (references.isEmpty()) {
        return;
      }
      for (ReferenceManager ref: references) {
        ref.release();
      }
      references.clear();
    }
  }
}