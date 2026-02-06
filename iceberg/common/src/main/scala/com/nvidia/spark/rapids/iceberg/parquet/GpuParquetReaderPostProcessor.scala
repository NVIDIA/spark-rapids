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

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.CastOptions
import com.nvidia.spark.rapids.GpuCast
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.GpuScalar
import com.nvidia.spark.rapids.NoopMetric
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillableColumnarBatch
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.schema.SchemaWithPartnerVisitor
import org.apache.iceberg.shaded.org.apache.parquet.schema.{MessageType => ShadedMessageType}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Pre-computed action tree for processing columnar batches.
 * Built once at construction time by comparing expected schema with file schema.
 */
private[iceberg] sealed trait ColumnAction {
  def execute(ctx: ColumnActionContext): CudfColumnVector

  /**
   * Display this action as a tree structure for debugging and testing.
   * @param indent The indentation level (number of spaces).
   * @return A string representation of the action tree.
   */
  def display(indent: Int = 0): String
}

/**
 * Context for executing column actions.
 *
 * @param processor The processor instance for accessing metadata, constants, and numRows.
 * @param column    The current column to operate on (None for constant/null actions).
 */
private[iceberg] class ColumnActionContext(
    val processor: GpuParquetReaderPostProcessor,
    val column: Option[CudfColumnVector]
) {
  def withColumn(col: CudfColumnVector): ColumnActionContext = {
    new ColumnActionContext(processor, Some(col))
  }

  def numRows: Int = column.map(_.getRowCount.toInt).getOrElse(processor.currentNumRows)
}

/** Pass through column directly (schemas match exactly). */
private[iceberg] case object PassThrough extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    ctx.column.get.incRefCount()
  }

  override def display(indent: Int): String = {
    " " * indent + "PassThrough"
  }
}

/** Upcast column to target type. */
private[iceberg] case class UpCast(
    fromType: DataType,
    toType: DataType
) extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    val col = ctx.column.get
    if (DataType.equalsStructurally(fromType, toType)) {
      col.incRefCount()
    } else {
      GpuCast.doCast(col, fromType, toType, CastOptions.DEFAULT_CAST_OPTIONS)
    }
  }

  override def display(indent: Int): String = {
    " " * indent + s"UpCast(${fromType.simpleString} -> ${toType.simpleString})"
  }
}

/** Fetch constant value from idToConstant map. */
private[iceberg] case class FetchConstant(
    fieldId: Int,
    sparkType: DataType
) extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    val value = ctx.processor.idToConstant.get(fieldId)
    withResource(GpuScalar.from(value, sparkType)) { scalar =>
      CudfColumnVector.fromScalar(scalar, ctx.numRows)
    }
  }

  override def display(indent: Int): String = {
    " " * indent + s"FetchConstant(fieldId=$fieldId, ${sparkType.simpleString})"
  }
}

/** Fill with null values for missing optional column. */
private[iceberg] case class FillNull(sparkType: DataType) extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    GpuColumnVector.columnVectorFromNull(ctx.numRows, sparkType)
  }

  override def display(indent: Int): String = {
    " " * indent + s"FillNull(${sparkType.simpleString})"
  }
}

/** Fetch FILE_PATH metadata column. */
private[iceberg] case object FetchFilePath extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    withResource(GpuScalar.from(ctx.processor.filePath, StringType)) { scalar =>
      CudfColumnVector.fromScalar(scalar, ctx.numRows)
    }
  }

  override def display(indent: Int): String = {
    " " * indent + "FetchFilePath"
  }
}

/** Fetch ROW_POSITION metadata column. */
private[iceberg] case object FetchRowPosition extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    val numRows = ctx.numRows
    val rowPoses = new Array[Long](numRows)
    val processor = ctx.processor
    var curBlockRowCount = processor.parquetInfo.blocks(processor.curBlockIndex).getRowCount
    var curBlockRowStart =
      processor.parquetInfo.blocksFirstRowIndices(processor.curBlockIndex)
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
        curBlockRowStart =
          processor.parquetInfo.blocksFirstRowIndices(processor.curBlockIndex)
        curBlockRowEnd = curBlockRowStart + curBlockRowCount
      }

      rowPoses(i) = curRowPos
      curRowPos += 1
      processor.processedRowCount += 1
    }

    CudfColumnVector.fromLongs(rowPoses: _*)
  }

  override def display(indent: Int): String = {
    " " * indent + "FetchRowPosition"
  }
}

/** Process list column, applying action to elements if needed. */
private[iceberg] case class ProcessList(
    elementAction: ColumnAction
) extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    val col = ctx.column.get
    if (elementAction == PassThrough) {
      col.incRefCount()
    } else {
      val elementCol = withResource(col.getChildColumnView(0)) { childView =>
        childView.copyToColumnVector()
      }
      withResource(elementCol) { _ =>
        withResource(elementAction.execute(ctx.withColumn(elementCol))) { transformed =>
          withResource(col.replaceListChild(transformed)) { view =>
            view.copyToColumnVector()
          }
        }
      }
    }
  }

  override def display(indent: Int): String = {
    val sb = new StringBuilder
    sb.append(" " * indent).append("ProcessList\n")
    sb.append(" " * (indent + 2)).append("element:\n")
    sb.append(elementAction.display(indent + 4))
    sb.toString()
  }
}

/** Process map column, applying actions to key/value if needed. */
private[iceberg] case class ProcessMap(
    keyAction: ColumnAction,
    valueAction: ColumnAction
) extends ColumnAction {
  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    val col = ctx.column.get
    if (keyAction == PassThrough && valueAction == PassThrough) {
      col.incRefCount()
    } else {
      // Map in cuDF is list<struct<key,value>>
      val structCol = withResource(col.getChildColumnView(0)) { childView =>
        childView.copyToColumnVector()
      }
      withResource(structCol) { _ =>
        val childCols = Seq(0, 1).safeMap { idx =>
          withResource(structCol.getChildColumnView(idx)) { childView =>
            childView.copyToColumnVector()
          }
        }
        withResource(childCols) { cols =>
          val keyCol = cols(0)
          val valueCol = cols(1)
          withResource(keyAction.execute(ctx.withColumn(keyCol))) { newKey =>
            withResource(valueAction.execute(ctx.withColumn(valueCol))) { newValue =>
              withResource(CudfColumnVector.makeStruct(newKey, newValue)) { kvStruct =>
                withResource(col.replaceListChild(kvStruct)) { view =>
                  view.copyToColumnVector()
                }
              }
            }
          }
        }
      }
    }
  }

  override def display(indent: Int): String = {
    val sb = new StringBuilder
    sb.append(" " * indent).append("ProcessMap\n")
    sb.append(" " * (indent + 2)).append("key:\n")
    sb.append(keyAction.display(indent + 4))
    sb.append("\n")
    sb.append(" " * (indent + 2)).append("value:\n")
    sb.append(valueAction.display(indent + 4))
    sb.toString()
  }
}

/**
 * Process struct column, applying actions to fields if needed.
 * @param fieldActions Actions for each expected field (in expected schema order)
 * @param inputIndices Mapping from expected field position to input child position.
 *                     inputIndices(i) = Some(j) means expected field i maps to input child j.
 *                     inputIndices(i) = None means field doesn't exist in input (needs generation).
 */
private[iceberg] case class ProcessStruct(
    fieldActions: Seq[ColumnAction],
    inputIndices: Seq[Option[Int]]
) extends ColumnAction {
  require(fieldActions.size == inputIndices.size,
    s"fieldActions size ${fieldActions.size} must match inputIndices size ${inputIndices.size}")

  override def execute(ctx: ColumnActionContext): CudfColumnVector = {
    // Handle case where entire struct is missing from file (ctx.column is None)
    // In this case, all inputIndices must be None and we generate all children
    ctx.column match {
      case None =>
        // Struct is missing from file - all fields must be generated
        require(inputIndices.forall(_.isEmpty),
          "When struct column is missing, all inputIndices must be None")
        val childCols = fieldActions.safeMap(action => action.execute(ctx))
        withResource(childCols) { cols =>
          CudfColumnVector.makeStruct(ctx.numRows, cols: _*)
        }
      
      case Some(col) =>
        val childCols = fieldActions.zip(inputIndices).safeMap { case (action, inputIdx) =>
          inputIdx match {
            case Some(idx) =>
              // Field exists in input - get child at the correct index
              val childCol = withResource(col.getChildColumnView(idx)) { childView =>
                childView.copyToColumnVector()
              }
              if (action == PassThrough) {
                childCol
              } else {
                withResource(childCol) { _ =>
                  action.execute(ctx.withColumn(childCol))
                }
              }
            case None =>
              // Field doesn't exist in input - action must generate the column
              // (FillNull, FetchConstant, etc.)
              action.execute(ctx)
          }
        }
        withResource(childCols) { cols =>
          CudfColumnVector.makeStruct(ctx.numRows, cols: _*)
        }
    }
  }

  override def display(indent: Int): String = {
    val sb = new StringBuilder
    sb.append(" " * indent).append("ProcessStruct\n")
    fieldActions.zip(inputIndices).zipWithIndex.foreach { case ((action, inputIdx), i) =>
      val inputInfo = inputIdx.map(idx => s"input[$idx]").getOrElse("generated")
      sb.append(" " * (indent + 2)).append(s"field[$i] ($inputInfo):\n")
      sb.append(action.display(indent + 4))
      if (i < fieldActions.size - 1) sb.append("\n")
    }
    sb.toString()
  }
}


/**
 * Helper object for building column actions when field is missing from file schema.
 * Shared between ActionBuildingVisitor and GpuParquetReaderPostProcessor.
 */
private[iceberg] object MissingFieldActionBuilder {
  /**
   * Build action for a field missing from file schema.
   * Checks constants, metadata columns, and optionality.
   */
  def buildAction(
      fieldId: Int,
      sparkType: DataType,
      isOptional: Boolean,
      idToConstant: JMap[Integer, _]): ColumnAction = {
    // 1. Check constant map
    if (idToConstant.containsKey(fieldId)) {
      return FetchConstant(fieldId, sparkType)
    }

    // 2. Check metadata columns
    if (fieldId == MetadataColumns.FILE_PATH.fieldId) {
      return FetchFilePath
    }
    if (fieldId == MetadataColumns.ROW_POSITION.fieldId) {
      return FetchRowPosition
    }
    if (fieldId == MetadataColumns.IS_DELETED.fieldId) {
      throw new UnsupportedOperationException("IS_DELETED meta column is not supported yet")
    }

    // 3. Check if optional - fill null
    if (isOptional) {
      FillNull(sparkType)
    } else {
      // 4. Required field missing - throw error
      throw new IllegalArgumentException(s"Missing required field: $fieldId")
    }
  }
}

/**
 * Visitor to build column action tree by comparing expected schema with file schema.
 * Uses SchemaWithPartnerVisitor where partner is the corresponding file schema type.
 * Tracks current field context to handle missing primitives properly.
 */
private class ActionBuildingVisitor(
    idToConstant: JMap[Integer, _]
) extends SchemaWithPartnerVisitor[Type, ColumnAction] {

  // Track current field using a list as stack (for nested struct handling)
  private var fieldStack: List[Types.NestedField] = Nil
  private def currentField: Types.NestedField = fieldStack.headOption.orNull

  override def schema(
      schema: Schema,
      partner: Type,
      structResult: ColumnAction): ColumnAction = {
    throw new IllegalStateException("Visiting schema not supported in column action builder")
  }

  override def struct(
      struct: Types.StructType,
      partner: Type,
      fieldResults: JList[ColumnAction]): ColumnAction = {
    // Check if the entire struct is a constant (e.g., partition struct)
    // This must be checked BEFORE processing children because the constant
    // is for the whole struct, not individual fields
    if (currentField != null && idToConstant.containsKey(currentField.fieldId())) {
      val sparkType = SparkSchemaUtil.convert(struct)
      return FetchConstant(currentField.fieldId(), sparkType)
    }
    
    val actions = fieldResults.asScala.toSeq
    val expectedFields = struct.fields().asScala

    // Build input indices mapping from expected field ID to partner (file) field position
    val partnerFieldIdToIndex: Map[Int, Int] = if (partner != null) {
      partner.asStructType().fields().asScala.zipWithIndex.map { case (f, i) =>
        f.fieldId() -> i
      }.toMap
    } else {
      // Partner is null - struct doesn't exist in file, all fields must be generated
      Map.empty
    }

    // Map each expected field to its input index (if exists in partner/file)
    val inputIndices = expectedFields.map { f =>
      partnerFieldIdToIndex.get(f.fieldId())
    }.toSeq

    // Check if all PassThrough AND indices are sequential - can simplify to PassThrough
    // Note: must have at least one field and all must come from input to pass through
    val canPassThrough = actions.nonEmpty && 
      actions.forall(_ == PassThrough) &&
      inputIndices.forall(_.isDefined) &&
      inputIndices.zipWithIndex.forall { case (optIdx, i) => optIdx.contains(i) }

    if (canPassThrough) {
      PassThrough
    } else {
      ProcessStruct(actions, inputIndices)
    }
  }

  override def beforeField(field: Types.NestedField, partner: Type): Unit = {
    fieldStack = field :: fieldStack
  }

  override def afterField(field: Types.NestedField, partner: Type): Unit = {
    fieldStack = fieldStack.tail
  }

  override def field(
      field: Types.NestedField,
      partner: Type,
      fieldResult: ColumnAction): ColumnAction = fieldResult

  override def list(
      list: Types.ListType,
      partner: Type,
      elementResult: ColumnAction): ColumnAction = {
    // If partner is null, the entire list is missing from the file - fill with null
    if (partner == null) {
      val sparkType = SparkSchemaUtil.convert(list)
      return FillNull(sparkType)
    }
    
    if (elementResult == PassThrough) {
      PassThrough
    } else {
      ProcessList(elementResult)
    }
  }

  override def map(
      map: Types.MapType,
      partner: Type,
      keyResult: ColumnAction,
      valueResult: ColumnAction): ColumnAction = {
    // If partner is null, the entire map is missing from the file - fill with null
    if (partner == null) {
      val sparkType = SparkSchemaUtil.convert(map)
      return FillNull(sparkType)
    }
    
    if (keyResult == PassThrough && valueResult == PassThrough) {
      PassThrough
    } else {
      ProcessMap(keyResult, valueResult)
    }
  }

  override def primitive(
      primitive: Type.PrimitiveType,
      partner: Type): ColumnAction = {
    val expectedType = SparkSchemaUtil.convert(primitive)

    if (partner != null) {
      // Partner exists - check for type promotion
      val fileType = SparkSchemaUtil.convert(partner.asPrimitiveType())
      if (DataType.equalsStructurally(expectedType, fileType)) {
        PassThrough
      } else {
        UpCast(fileType, expectedType)
      }
    } else {
      // Partner missing - use shared logic
      MissingFieldActionBuilder.buildAction(
        currentField.fieldId(),
        expectedType,
        currentField.isOptional,
        idToConstant)
    }
  }
}

/**
 * Partner accessors to navigate file schema alongside expected schema.
 */
private class FileSchemaAccessors extends SchemaWithPartnerVisitor.PartnerAccessors[Type] {

  override def fieldPartner(partnerStruct: Type, fieldId: Int, name: String): Type = {
    if (partnerStruct == null) return null
    val structType = partnerStruct.asStructType()
    val field = structType.field(fieldId)
    if (field == null) null else field.`type`()
  }

  override def listElementPartner(partnerList: Type): Type = {
    if (partnerList == null) return null
    partnerList.asListType().elementType()
  }

  override def mapKeyPartner(partnerMap: Type): Type = {
    if (partnerMap == null) return null
    partnerMap.asMapType().keyType()
  }

  override def mapValuePartner(partnerMap: Type): Type = {
    if (partnerMap == null) return null
    partnerMap.asMapType().valueType()
  }
}

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
 * The processor builds an action tree at construction time by comparing the expected schema
 * with the file schema using SchemaWithPartnerVisitor. This avoids repeated schema
 * comparisons during batch processing.
 *
 * @param parquetInfo           Parquet file info with block metadata.
 * @param idToConstant          Constant fields.
 * @param expectedSchema        Iceberg schema required by reader.
 * @param shadedFileReadSchema  Shaded parquet file read schema (to avoid conversion overhead).
 */
class GpuParquetReaderPostProcessor(
    private[iceberg] val parquetInfo: ParquetFileInfoWithBlockMeta,
    private[iceberg] val idToConstant: JMap[Integer, _],
    private[iceberg] val expectedSchema: Schema,
    shadedFileReadSchema: ShadedMessageType,
    metrics: Map[String, GpuMetric]
) {
  private val buildActionTimeMetric: GpuMetric =
    metrics.getOrElse(GpuMetric.ICEBERG_BUILD_ACTION_TIME, NoopMetric)
  private val postProcessTimeMetric: GpuMetric =
    metrics.getOrElse(GpuMetric.ICEBERG_POST_PROCESS_TIME, NoopMetric)
  require(parquetInfo != null, "parquetInfo cannot be null")
  require(parquetInfo.blocks.size == parquetInfo.blocksFirstRowIndices.size,
    s"Parquet info block count ${parquetInfo.blocks.size} not matching parquet info block " +
      s"first row index count ${parquetInfo.blocksFirstRowIndices.size}")
  require(idToConstant != null, "idToConstant cannot be null")
  require(expectedSchema != null, "expectedSchema cannot be null")
  require(shadedFileReadSchema != null, "shadedFileReadSchema cannot be null")

  private[iceberg] val fileReadSchema = parquetInfo.schema
  private[iceberg] val filePath: String = parquetInfo.filePath.toString
  private[iceberg] var processedBlockRowCounts = 0L
  private[iceberg] var processedRowCount = 0L
  private[iceberg] var curBlockIndex = 0
  // Set during process() for actions that don't have a column
  private[iceberg] var currentNumRows = 0

  // Convert shaded parquet schema to Iceberg schema for comparison
  private val fileIcebergSchema: Schema = ParquetSchemaUtil.convert(shadedFileReadSchema)

  // Build field ID to batch index mapping using the UNSHADED schema from parquetInfo.
  // The batch returned by parquet reader is in FILE order, and the schema stored in
  // parquetInfo is the one that was actually used for reading.
  // Map field ID to file position (batch index).
  private val fieldIdToBatchIndex: Map[Int, Int] = {
    (0 until fileReadSchema.getFieldCount).flatMap { i =>
      Option(fileReadSchema.getType(i).getId).map(id => id.intValue() -> i)
    }.toMap
  }

  // Pre-compute action tree by visiting expected schema with file schema as partner
  private val rootAction: ColumnAction = buildActionTimeMetric.ns {
    val visitor = new ActionBuildingVisitor(idToConstant)
    val accessors = new FileSchemaAccessors()
    SchemaWithPartnerVisitor.visit(
      expectedSchema.asStruct(),
      fileIcebergSchema.asStruct(),
      visitor,
      accessors)
  }

  private val expectedFields = expectedSchema.asStruct().fields().asScala
  private val expectedSparkTypes = expectedFields.map(f => SparkSchemaUtil.convert(f.`type`()))

  // Check if we can pass through the entire batch without any processing.
  private val canPassThroughBatch: Boolean = rootAction == PassThrough

  // Expose for testing - displays the action tree
  private[iceberg] def displayActionPlan(): String = {
    rootAction match {
      case ProcessStruct(fieldActions, inputIndices) =>
        val fields = expectedFields
        val sb = new StringBuilder
        sb.append("ProcessStruct")
        fieldActions.zip(inputIndices).zip(fields).foreach { case ((action, inputIdx), field) =>
          sb.append("\n")
          val inputInfo = inputIdx.map(idx => s"input[$idx]").getOrElse("generated")
          sb.append(s"  ${field.name()} ($inputInfo):\n")
          sb.append(action.display(4))
        }
        sb.toString()
      case PassThrough =>
        "PassThrough"
      case other =>
        throw new IllegalStateException(
          "Root action must be ProcessStruct or PassThrough, but got: " +
            other.getClass.getSimpleName)
    }
  }

  /**
   * Process columnar batch to match expected schema.
   *
   * @param originalBatch Columnar batch read from parquet. This method takes ownership
   *                      of the batch, which should not be used afterward.
   * @return Processed columnar batch.
   */
  def process(originalBatch: ColumnarBatch): ColumnarBatch = {
    require(originalBatch != null, "Columnar batch can't be null")

    // Fast path: if schemas match exactly, pass through without processing
    if (canPassThroughBatch) {
      currentNumRows = originalBatch.numRows()
      return originalBatch
    }

    postProcessTimeMetric.ns {
      withRetryNoSplit(SpillableColumnarBatch(originalBatch, ACTIVE_ON_DECK_PRIORITY)) { scb =>
        // getColumnarBatch() returns a batch with refcounts incremented.
        // We MUST close it to balance the refcounts, even if an exception occurs.
        withResource(scb.getColumnarBatch()) { batch =>
          currentNumRows = batch.numRows()

          val fields = expectedFields

          // Execute actions on batch (rootAction must be ProcessStruct here since
          // PassThrough is handled by canPassThroughBatch early return)
          val fieldActions = rootAction match {
            case ProcessStruct(actions, _) => actions
            case _ => throw new IllegalStateException(
              s"Root action must be ProcessStruct, but got: ${rootAction.getClass.getSimpleName}")
          }

          // For root-level processing, use fieldIdToBatchIndex to map field IDs to batch columns.
          // The batch is in readSchema order (expected order with missing fields skipped).
          // For PassThrough columns, we incRefCount so they survive when batch is closed.
          // For transformed columns, we create new column vectors.
          val columns: Seq[ColumnVector] = fieldActions.zip(fields).zipWithIndex.safeMap {
            case ((action, field), idx) =>
              val batchIdx = fieldIdToBatchIndex.get(field.fieldId())
              val col = batchIdx.map(i => batch.column(i).asInstanceOf[GpuColumnVector].getBase)
              val ctx = new ColumnActionContext(this, col)
              val result = action.execute(ctx)
              closeOnExcept(result) { _ =>
                GpuColumnVector.from(result, expectedSparkTypes(idx)).asInstanceOf[ColumnVector]
              }
          }
          new ColumnarBatch(columns.toArray[ColumnVector], currentNumRows)
        }
      }
    }
  }
}
