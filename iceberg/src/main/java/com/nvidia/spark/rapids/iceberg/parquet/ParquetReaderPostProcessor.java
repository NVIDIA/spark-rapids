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

package com.nvidia.spark.rapids.iceberg.parquet;

import ai.rapids.cudf.Scalar;
import com.nvidia.spark.rapids.CastOptions$;
import com.nvidia.spark.rapids.GpuCast;
import com.nvidia.spark.rapids.GpuColumnVector;
import com.nvidia.spark.rapids.GpuScalar;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument;

public class ParquetReaderPostProcessor extends TypeUtil.SchemaVisitor<Object> {
  private final MessageType fileReadSchema;
  private final Map<Integer, ?> idToConstant;
  private final Schema expectedSchema;
  private ColumnarBatch batch;
  private Types.NestedField currentField;

  public ParquetReaderPostProcessor(MessageType fileReadSchema, Map<Integer, ?> idToConstant,
      Schema expectedSchema) {
    Objects.requireNonNull(fileReadSchema, "fileReadSchema cannot be null");
    Objects.requireNonNull(expectedSchema, "expectedSchema cannot be null");
    Objects.requireNonNull(idToConstant, "idToConstant cannot be null");
    this.fileReadSchema = fileReadSchema;
    this.idToConstant = idToConstant;
    this.expectedSchema = expectedSchema;
  }

  @Override
  public Object schema(Schema schema, Object structResult) {
    return structResult;
  }

  @Override
  public Object struct(Types.StructType struct, List<Object> fieldResults) {
    ColumnVector[] columns = new ColumnVector[fieldResults.size()];
    for (int i = 0; i < fieldResults.size(); i++) {
      columns[i] = (ColumnVector) fieldResults.get(i);
    }
    return new ColumnarBatch(columns, batch.numRows());
  }

  @Override
  public Object field(Types.NestedField field, Object fieldResult) {
    if (!field.type().isPrimitiveType()) {
      throw new UnsupportedOperationException("Unsupported type for iceberg scan: " + field.type());
    }
    return fieldResult;
  }

  @Override
  public void beforeField(Types.NestedField field) {
    currentField = field;
  }

  @Override
  public Object primitive(Type.PrimitiveType fieldType) {
    int curFieldId = currentField.fieldId();
    DataType type = SparkSchemaUtil.convert(fieldType);
    // need to check for key presence since associated value could be null
    if (idToConstant.containsKey(curFieldId)) {
      try (Scalar scalar = GpuScalar.from(idToConstant.get(curFieldId), type)) {
        return GpuColumnVector.from(scalar, batch.numRows(), type);
      }
    }

    if (curFieldId == MetadataColumns.ROW_POSITION.fieldId()) {
      throw new UnsupportedOperationException("ROW_POSITION meta column is not supported yet");
    }

    if (curFieldId == MetadataColumns.IS_DELETED.fieldId()) {
      throw new UnsupportedOperationException("IS_DELETED meta column is not supported yet");
    }

    if (curFieldId == MetadataColumns.IS_DELETED.fieldId()) {
      throw new UnsupportedOperationException("IS_DELETED meta column is not supported yet");
    }

    for (int i=0; i<fileReadSchema.getFieldCount(); i++) {
      org.apache.parquet.schema.Type t = fileReadSchema.getType(i);
      if (t.getId() != null && t.getId().intValue() == curFieldId) {
        return doUpCastIfNeeded((GpuColumnVector) batch.column(i), fieldType);
      }
    }

    if (currentField.isOptional()) {
      return GpuColumnVector.fromNull(batch.numRows(), type);
    }

    throw new IllegalArgumentException("Missing required field: " + currentField.fieldId());
  }

  private ColumnVector doUpCastIfNeeded(GpuColumnVector oldColumn, Type.PrimitiveType targetType) {
    DataType expectedSparkType = SparkSchemaUtil.convert(targetType);
    return GpuColumnVector.from(
        GpuCast.doCast(oldColumn.getBase(), oldColumn.dataType(), expectedSparkType,
            CastOptions$.MODULE$.DEFAULT_CAST_OPTIONS()), expectedSparkType);
  }

  public ColumnarBatch process(ColumnarBatch originalBatch) {
    checkArgument(fileReadSchema.getFieldCount() == originalBatch.numCols(),
        "File read schema field count %s does not match expected schema field count %s",
        fileReadSchema.getFieldCount(),
        originalBatch.numCols());
    this.currentField = null;
    this.batch = originalBatch;
    return (ColumnarBatch) TypeUtil.visit(this.expectedSchema, this);
  }
}
