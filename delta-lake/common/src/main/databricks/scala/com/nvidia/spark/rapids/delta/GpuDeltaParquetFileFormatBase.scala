/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector => CudfColmnVector, Scalar}
import com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaColumnMappingMode, NoMapping}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuMetric, GpuParquetMultiFilePartitionReaderFactory, GpuReadParquetFileFormat}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

abstract class GpuDeltaParquetFileFormatBase extends GpuReadParquetFileFormat {
  val columnMappingMode: DeltaColumnMappingMode
  val referenceSchema: StructType

  def prepareSchema(inputSchema: StructType): StructType = {
    DeltaColumnMapping.createPhysicalSchema(inputSchema, referenceSchema, columnMappingMode)
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val preparedRequiredSchema = prepareSchema(fileScan.requiredSchema)

    GpuParquetMultiFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      preparedRequiredSchema,
      prepareSchema(fileScan.readPartitionSchema),
      pushedFilters,
      fileScan.rapidsConf,
      fileScan.allMetrics,
      fileScan.queryUsesInputFile,
      fileScan.alluxioPathsMap)
  }

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric],
      alluxioPathReplacementMap: Option[Map[String, String]])
  : PartitionedFile => Iterator[InternalRow] = {
    val preparedDataSchema = prepareSchema(dataSchema)
    val preparedRequiredSchema = prepareSchema(requiredSchema)
    val preparedPartitionSchema = prepareSchema(partitionSchema)

    logDebug(s"""Schema passed to GpuDeltaParquetFileFormat:
               |dataSchema=$dataSchema
               |partitionSchema=$partitionSchema
               |requiredSchema=$requiredSchema
               |preparedDataSchema=$preparedDataSchema
               |preparedPartitionSchema=$preparedPartitionSchema
               |preparedRequiredSchema=$preparedRequiredSchema
               |referenceSchema=$referenceSchema
               |""".stripMargin)

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      preparedDataSchema,
      preparedPartitionSchema,
      preparedRequiredSchema,
      filters,
      options,
      hadoopConf,
      metrics,
      alluxioPathReplacementMap)

    (file: PartitionedFile) => {
      val input = dataReader(file)
      GpuDeltaParquetFileFormatBase.addMetadataColumnToIterator(preparedRequiredSchema,
        input.asInstanceOf[Iterator[ColumnarBatch]])
        .asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }

}

object GpuDeltaParquetFileFormatBase {
  val METADATA_ROW_IDX_COLUMN: String = "__metadata_row_index"
  val METADATA_ROW_IDX_FIELD: StructField = StructField(METADATA_ROW_IDX_COLUMN, LongType,
    nullable = false)


  private def addMetadataColumnToIterator(
      schema: StructType,
      input: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    val metadataRowIndexCol = schema.fieldNames.indexOf(METADATA_ROW_IDX_COLUMN)
    if (metadataRowIndexCol == -1) {
      return input
    }
    var rowIndex = 0L
    input.map { batch =>
      val newBatch = addRowIdxColumn(metadataRowIndexCol, rowIndex, batch)
      rowIndex += batch.numRows()
      newBatch
    }
  }

  private def addRowIdxColumn(
      rowIdxPos: Int,
      rowIdxStart: Long,
      batch: ColumnarBatch): ColumnarBatch = {
    val rowIdxCol = {
      val cv = CudfColmnVector.sequence(Scalar.fromLong(rowIdxStart), batch.numRows())
      GpuColumnVector.from(cv.incRefCount(), METADATA_ROW_IDX_FIELD.dataType)
    }

    // Replace row_idx column
    val columns = ArrayBuffer[ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      if (i == rowIdxPos) {
        columns += rowIdxCol
      } else {
        columns += batch.column(i)
      }
    }

    new ColumnarBatch(columns.toArray, batch.numRows())
  }
}