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

package com.nvidia.spark.rapids.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import ai.rapids.cudf.Scalar;
import com.nvidia.spark.rapids.GpuCast;
import com.nvidia.spark.rapids.GpuColumnVector;
import com.nvidia.spark.rapids.GpuScalar;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Takes a partition reader output and adds any constant columns and deletion filters
 * that need to be applied after the data is loaded from the raw data files.
 */
public class GpuIcebergReader implements CloseableIterator<ColumnarBatch> {
  private final Schema expectedSchema;
  private final PartitionReader<ColumnarBatch> partReader;
  private final GpuDeleteFilter deleteFilter;
  private final Map<Integer, ?> idToConstant;
  private boolean needNext = true;
  private boolean isBatchPending;

  public GpuIcebergReader(Schema expectedSchema,
                          PartitionReader<ColumnarBatch> partReader,
                          GpuDeleteFilter deleteFilter,
                          Map<Integer, ?> idToConstant) {
    this.expectedSchema = expectedSchema;
    this.partReader = partReader;
    this.deleteFilter = deleteFilter;
    this.idToConstant = idToConstant;
  }

  @Override
  public void close() throws IOException {
    partReader.close();
  }

  @Override
  public boolean hasNext() {
    if (needNext) {
      try {
        isBatchPending = partReader.next();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      needNext = false;
    }
    return isBatchPending;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches to iterate");
    }
    isBatchPending = false;
    needNext = true;
    try (ColumnarBatch batch = partReader.get()) {
      if (deleteFilter != null) {
        throw new UnsupportedOperationException("Delete filter is not supported");
      }
      ColumnarBatch updatedBatch = addConstantColumns(batch);
      return addUpcastsIfNeeded(updatedBatch);
    }
  }

  private ColumnarBatch addConstantColumns(ColumnarBatch batch) {
    ColumnVector[] columns = new ColumnVector[expectedSchema.columns().size()];
    ColumnarBatch result = null;
    final ConstantDetector constantDetector = new ConstantDetector(idToConstant);
    try {
      int inputIdx = 0;
      int outputIdx = 0;
      for (Types.NestedField field : expectedSchema.columns()) {
        // need to check for key presence since associated value could be null
        if (idToConstant.containsKey(field.fieldId())) {
          DataType type = SparkSchemaUtil.convert(field.type());
          try (Scalar scalar = GpuScalar.from(idToConstant.get(field.fieldId()), type)) {
            columns[outputIdx++] = GpuColumnVector.from(scalar, batch.numRows(), type);
          }
        } else {
          if (TypeUtil.visit(field.type(), constantDetector)) {
            throw new UnsupportedOperationException("constants not implemented for nested field");
          }
          GpuColumnVector gpuColumn = (GpuColumnVector) batch.column(inputIdx++);
          columns[outputIdx++] = gpuColumn.incRefCount();
        }
      }
      if (inputIdx != batch.numCols()) {
        throw new IllegalStateException("Did not consume all input batch columns");
      }
      result = new ColumnarBatch(columns, batch.numRows());
    } finally {
      if (result == null) {
        // TODO: Update safeClose to be reusable by Java code
        for (ColumnVector c : columns) {
          if (c != null) {
            c.close();
          }
        }
      }
    }
    return result;
  }

  private ColumnarBatch addUpcastsIfNeeded(ColumnarBatch batch) {
    GpuColumnVector[] columns = null;
    try {
      List<Types.NestedField> expectedColumnTypes = expectedSchema.columns();
      Preconditions.checkState(expectedColumnTypes.size() == batch.numCols(),
          "Expected to load " + expectedColumnTypes.size() + " columns, found " + batch.numCols());
      columns = GpuColumnVector.extractColumns(batch);
      for (int i = 0; i < batch.numCols(); i++) {
        DataType expectedSparkType = SparkSchemaUtil.convert(expectedColumnTypes.get(i).type());
        GpuColumnVector oldColumn = columns[i];
        columns[i] = GpuColumnVector.from(
            GpuCast.doCast(oldColumn.getBase(), oldColumn.dataType(), expectedSparkType, false, false, false),
            expectedSparkType);
      }
      ColumnarBatch newBatch = new ColumnarBatch(columns, batch.numRows());
      columns = null;
      return newBatch;
    } finally {
      batch.close();
      if (columns != null) {
        for (ColumnVector c : columns) {
          c.close();
        }
      }
    }
  }

  private static class ConstantDetector extends TypeUtil.SchemaVisitor<Boolean> {
    private final Map<Integer, ?> idToConstant;

    ConstantDetector(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public Boolean schema(Schema schema, Boolean structResult) {
      return structResult;
    }

    @Override
    public Boolean struct(Types.StructType struct, List<Boolean> fieldResults) {
      return fieldResults.stream().anyMatch(b -> b);
    }

    @Override
    public Boolean field(Types.NestedField field, Boolean fieldResult) {
      return idToConstant.containsKey(field.fieldId());
    }

    @Override
    public Boolean list(Types.ListType list, Boolean elementResult) {
      return list.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
    }

    @Override
    public Boolean map(Types.MapType map, Boolean keyResult, Boolean valueResult) {
      return map.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
    }

    @Override
    public Boolean primitive(Type.PrimitiveType primitive) {
      return false;
    }
  }
}
