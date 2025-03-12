package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{CastOptions, GpuCast, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor.{doUpCastIfNeeded, HandlerResult}
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil
import java.util
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.types.{Type, Types, TypeUtil}
import org.apache.iceberg.types.Types.NestedField
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/** Processes columnar batch after reading from parquet file.
 *
 * Apache iceberg uses a lazy approach to deal with schema evolution, e.g. when you
 * add/remove/rename a column, it's essentially just metadata operation, without touching data
 * files. So after reading from parquet, we need to deal with missing column, type promotion,
 * etc. And these are all handled in [[GpuParquetReaderPostProcessor]].
 *
 * @param fileReadSchema Schema passed to actual parquet reader.
 * @param idToConstant Constant fields.
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

  def process(originalBatch: ColumnarBatch): ColumnarBatch = {
    require(originalBatch != null, "Columnar batch can't be null")
    require(fileReadSchema.getFieldCount == originalBatch.numCols(),
      s"File read schema field count ${fileReadSchema.getFieldCount} doesn't match expected " +
        s"columnar batch columns ${originalBatch.numCols()}")

    withResource(originalBatch) { _ =>
      withResource(new ColumnarBatchHandler(this, originalBatch)) { handler =>
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

  override def struct(struct: Types.StructType, fieldResults: util.List[HandlerResult])
  : HandlerResult = {
    val columns: Array[ColumnVector] = new Array[ColumnVector](fieldResults.size)
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
    if (curFieldId == MetadataColumns.ROW_POSITION.fieldId) {
      throw new UnsupportedOperationException("ROW_POSITION meta column is not supported yet")
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
}

object GpuParquetReaderPostProcessor {
  private[parquet] type HandlerResult = Either[ColumnarBatch, GpuColumnVector]

  private[parquet] def doUpCastIfNeeded(oldColumn: GpuColumnVector, targetType: Type.PrimitiveType)
  = {
    val expectedSparkType = SparkSchemaUtil.convert(targetType)
    GpuColumnVector.from(
      GpuCast.doCast(oldColumn.getBase,
        oldColumn.dataType,
        expectedSparkType,
        CastOptions.DEFAULT_CAST_OPTIONS),
      expectedSparkType)
  }
}
