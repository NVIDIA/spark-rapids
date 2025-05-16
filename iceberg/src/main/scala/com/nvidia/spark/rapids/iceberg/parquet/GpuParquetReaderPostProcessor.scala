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

package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{CastOptions, GpuCast, GpuColumnVector, GpuScalar, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor.{doUpCastIfNeeded, HandlerResult}
import java.util
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.{Type, Types, TypeUtil}
import org.apache.iceberg.types.Types.NestedField
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/** Processes columnar batch after reading from parquet file.
 *
 * Apache iceberg uses a lazy approach to deal with schema evolution, e.g. when you
 * add/remove/rename a column, it's essentially just metadata operation, without touching data
 * files. So after reading from parquet, we need to deal with missing column, type promotion,
 * etc. And these are all handled in [[GpuParquetReaderPostProcessor]].
 *
 * For details of schema evolution, please refer to
 * [[https://iceberg.apache.org/spec/#schema-evolution iceberg spec]].
 *
 * @param fileReadSchema Schema passed to actual parquet reader.
 * @param idToConstant   Constant fields.
 * @param expectedSchema Iceberg schema required by reader.
 */
class GpuParquetReaderPostProcessor(
    private[parquet] val fileReadSchema: MessageType,
    private[parquet] val idToConstant: JMap[Integer, _],
    private[parquet] val expectedSchema: Schema
) {
  require(fileReadSchema != null, "fileReadSchema cannot be null")
  require(idToConstant != null, "idToConstant cannot be null")
  require(expectedSchema != null, "expectedSchema cannot be null")

  /**
   * Process columnar batch to match expected schema.
   *
   * @param originalBatch Columnar batch read from parquet.Note that this method will take
   *                      ownership of this batch, and should not be used anymore.
   * @return Processed columnar batch.
   */
  def process(originalBatch: ColumnarBatch): ColumnarBatch = {
    require(originalBatch != null, "Columnar batch can't be null")

    withRetryNoSplit(SpillableColumnarBatch(originalBatch, ACTIVE_ON_DECK_PRIORITY)) { batch =>
      withResource(new ColumnarBatchHandler(this, batch)) { handler =>
        TypeUtil.visit(expectedSchema, handler).left.get
      }
    }
  }
}


private class ColumnarBatchHandler(private val processor: GpuParquetReaderPostProcessor,
    private val spillableBatch: SpillableColumnarBatch
) extends TypeUtil.SchemaVisitor[HandlerResult] with AutoCloseable {
  private val batch = spillableBatch.getColumnarBatch()
  private var currentField: NestedField = _
  // This is used to hold the column vectors that are created during the travel of the batch, so
  // that if exception happens, we can close them all at once.
  private val vectorBuffer = new ArrayBuffer[GpuColumnVector](batch.numCols())


  override def schema(schema: Schema, structResult: HandlerResult): HandlerResult  = structResult

  override def struct(struct: Types.StructType, fieldResults: util.List[HandlerResult])
  : HandlerResult = {
    val columns = new Array[ColumnVector](fieldResults.size)
    for (i <- 0 until fieldResults.size) {
      columns(i) = fieldResults.get(i).right.get
    }
    // Ownership has transferred to columnar batch, so it should be cleared
    vectorBuffer.clear()
    Left(new ColumnarBatch(columns, batch.numRows()))
  }

  override def field(field: Types.NestedField, fieldResult: HandlerResult): HandlerResult = {
    if (!field.`type`.isPrimitiveType) {
      throw new UnsupportedOperationException("Unsupported type" +
        " for iceberg scan: " + field.`type`)
    }
    fieldResult
  }


  override def beforeField(field: Types.NestedField): Unit = {
    currentField = field
  }

  override def primitive(fieldType: Type.PrimitiveType): HandlerResult = {
    val gpuColVector = doProcessPrimitive(fieldType)
    vectorBuffer += gpuColVector
    Right(gpuColVector)
  }

  private def doProcessPrimitive(fieldType: Type.PrimitiveType): GpuColumnVector = {
    val curFieldId = currentField.fieldId
    val sparkType = SparkSchemaUtil.convert(fieldType)
    // need to check for key presence since associated value could be null
    if (processor.idToConstant.containsKey(curFieldId)) {
      withResource(GpuScalar.from(processor.idToConstant.get(curFieldId), sparkType)) { scalar =>
        return GpuColumnVector.from(scalar, batch.numRows(), sparkType)
      }
    }
    if (curFieldId == MetadataColumns.ROW_POSITION.fieldId) {
      throw new UnsupportedOperationException("ROW_POSITION meta column is not supported yet")
    }
    if (curFieldId == MetadataColumns.IS_DELETED.fieldId) {
      throw new UnsupportedOperationException("IS_DELETED meta column is not supported yet")
    }

    for (i <- 0 until processor.fileReadSchema.getFieldCount) {
      val t = processor.fileReadSchema.getType(i)
      if (t.getId != null && t.getId.intValue == curFieldId) {
        return doUpCastIfNeeded(batch.column(i).asInstanceOf[GpuColumnVector],
          fieldType)
      }
    }
    if (currentField.isOptional) {
      return GpuColumnVector.fromNull(batch.numRows(), sparkType)
    }

    throw new IllegalArgumentException("Missing required field: " + currentField.fieldId)
  }

  override def close(): Unit = {
    batch.close()
    withResource(vectorBuffer) { _ => }
  }
}

object GpuParquetReaderPostProcessor {
  private[parquet] type HandlerResult = Either[ColumnarBatch, GpuColumnVector]

  private[parquet] def doUpCastIfNeeded(oldColumn: GpuColumnVector,
      targetType: Type.PrimitiveType): GpuColumnVector = {
    val expectedSparkType = SparkSchemaUtil.convert(targetType)
    if (DataType.equalsStructurally(oldColumn.dataType, expectedSparkType)) {
      oldColumn.incRefCount()
    } else {
      GpuColumnVector.from(
        GpuCast.doCast(oldColumn.getBase,
          oldColumn.dataType,
          expectedSparkType,
          CastOptions.DEFAULT_CAST_OPTIONS),
        expectedSparkType)
    }
  }
}