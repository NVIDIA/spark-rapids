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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.{CastOptions, GpuCast, GpuColumnVector, GpuScalar, ParquetFileInfoWithBlockMeta}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor.{doUpCastIfNeeded, HandlerResult}
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil
import java.util
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.types.{Type, Types, TypeUtil}
import org.apache.iceberg.types.Types.NestedField
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/** Processes columnar batch after reading from parquet file.
 *
 * Apache iceberg uses a lazy approach to deal with schema evolution, e.g. when you
 * add/remove/rename a column, it's essentially just metadata operation, without touching data
 * files. So after reading from parquet, we need to deal with missing column, type promotion,
 * etc. And these are all handled in [[GpuParquetReaderPostProcessor]].
 *
 * @param parquetInfo    Parquet file info with block metadata.
 * @param idToConstant   Constant fields.
 * @param expectedSchema Iceberg schema required by reader.
 */
class GpuParquetReaderPostProcessor(
    private[parquet] val parquetInfo: ParquetFileInfoWithBlockMeta,
    private[parquet] val idToConstant: JMap[Integer, _],
    private[parquet] val expectedSchema: Schema
) {
  require(parquetInfo != null, "parquetInfo cannot be null")
  require(parquetInfo.blocks.size == parquetInfo.blocksFirstRowIndices.size,
    s"Parquet info block count ${parquetInfo.blocks.size} not matching parquet info block first " +
      s"row index count ${parquetInfo.blocksFirstRowIndices.size}")
  require(idToConstant != null, "idToConstant cannot be null")
  require(expectedSchema != null, "expectedSchema cannot be null")

  private[parquet] val fileReadSchema: MessageType = parquetInfo.schema
  private[parquet] val filePath: String = parquetInfo.filePath.toString
  private[parquet] var processedBlockRowCounts = 0L
  private[parquet] var processedRowCount = 0L
  private[parquet] var curBlockIndex = 0

  /**
   * Process columnar batch to match expected schema.
   *
   * @param originalBatch Columnar batch read from parquet. Note that <b>the caller</b> should take
   *                      care of managing ownership of it.
   * @return Processed columnar batch.
   */
  def process(originalBatch: ColumnarBatch): ColumnarBatch = {
    require(originalBatch != null, "Columnar batch can't be null")

    withRetryNoSplit(GpuColumnVector.incRefCounts(originalBatch)) { batch =>
      withResource(new ColumnarBatchHandler(this, batch)) { handler =>
        TypeUtil.visit(expectedSchema, handler).left.get
      }
    }
  }
}


private class ColumnarBatchHandler(private val processor: GpuParquetReaderPostProcessor,
    private val batch: ColumnarBatch
) extends TypeUtil.SchemaVisitor[HandlerResult] with AutoCloseable {
  private var currentField: NestedField = _
  private val vectorBuffer: ArrayBuffer[GpuColumnVector] = new ArrayBuffer[GpuColumnVector](
    batch.numCols())

  override def schema(schema: Schema, structResult: HandlerResult): HandlerResult  = structResult

  override def struct(struct: Types.StructType, fieldResults: util.List[HandlerResult])
  : HandlerResult = {
    val columns = new Array[ColumnVector](fieldResults.size)
    for (i <- 0 until fieldResults.size) {
      columns(i) = fieldResults.get(i).right.get
    }
    // Ownership has transferred to columnar batch, so it should be cleared
    vectorBuffer.clear()
    Left(new ColumnarBatch(columns, batch.numRows))
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
    val `type` = SparkSchemaUtil.convert(fieldType)
    // need to check for key presence since associated value could be null
    if (processor.idToConstant.containsKey(curFieldId)) {
      withResource(GpuScalar.from(processor.idToConstant.get(curFieldId), `type`)) { scalar =>
        return GpuColumnVector.from(scalar, batch.numRows, `type`)
      }
    }

    if (curFieldId == MetadataColumns.FILE_PATH.fieldId) {
      withResource(GpuScalar.from(processor.filePath, StringType)) { scalar =>
        return GpuColumnVector.from(scalar, batch.numRows, StringType)
      }
    }

    if (curFieldId == MetadataColumns.ROW_POSITION.fieldId) {
      return processRowPos(batch.numRows)
    }

    if (curFieldId == MetadataColumns.IS_DELETED.fieldId) {
      throw new UnsupportedOperationException("IS_DELETED meta column is not supported yet")
    }

    for (i <- 0 until processor.fileReadSchema.getFieldCount) {
      val t = processor.fileReadSchema.getType(i)
      if (t.getId != null && t.getId.intValue == curFieldId) {
        return doUpCastIfNeeded(batch.column(i).asInstanceOf[GpuColumnVector], fieldType)
      }
    }
    if (currentField.isOptional) {
      return GpuColumnVector.fromNull(batch.numRows, `type`)
    }

    throw new IllegalArgumentException("Missing required field: " + currentField.fieldId)
  }

  override def close(): Unit = {
    withResource(vectorBuffer) { _ => }
  }

  private def processRowPos(numRows: Int): GpuColumnVector = {
    val rowPoses = new Array[Long](numRows)
    var curBlockRowCount = processor.parquetInfo.blocks(processor.curBlockIndex).getRowCount
    var curBlockRowStart = processor.parquetInfo.blocksFirstRowIndices(processor.curBlockIndex)
    var curBlockRowEnd = curBlockRowStart + curBlockRowCount
    var curRowPos = curBlockRowStart + processor.processedRowCount -
      processor.processedBlockRowCounts
    for (i <- 0 until numRows) {
      if (curRowPos >= curBlockRowEnd) {
        // switch to next block
        processor.curBlockIndex += 1
        processor.processedBlockRowCounts += curBlockRowCount
        curRowPos = processor.parquetInfo.blocksFirstRowIndices(processor.curBlockIndex)

        curBlockRowCount = processor.parquetInfo.blocks(processor.curBlockIndex).getRowCount
        curBlockRowStart = processor.parquetInfo.blocksFirstRowIndices(processor.curBlockIndex)
        curBlockRowEnd = curBlockRowStart + curBlockRowCount
      }

      rowPoses(i) = curRowPos
      curRowPos += 1
      processor.processedRowCount += 1
    }

    closeOnExcept(CudfColumnVector.fromLongs(rowPoses: _*)) { rowPosesCV =>
      GpuColumnVector.fromChecked(rowPosesCV, LongType)
    }
  }
}

object GpuParquetReaderPostProcessor {
  private[parquet] type HandlerResult = Either[ColumnarBatch, GpuColumnVector]

  private[parquet] def doUpCastIfNeeded(oldColumn: GpuColumnVector, targetType: Type.PrimitiveType)
  = {
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
