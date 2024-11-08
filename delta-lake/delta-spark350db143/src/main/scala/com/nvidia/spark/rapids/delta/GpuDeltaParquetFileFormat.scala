/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.net.URI

import com.databricks.sql.transaction.tahoe.{DeltaColumnMappingMode, DeltaParquetFileFormat, IdMapping}
import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat._
import com.nvidia.spark.rapids.{GpuMetric, RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.addMetadataColumnToIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuDeltaParquetFileFormat(
    override val columnMappingMode: DeltaColumnMappingMode,
    override val referenceSchema: StructType,
    isSplittable: Boolean,
    disablePushDowns: Boolean,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]],
    tablePath: Option[String] = None,
    broadcastHadoopConf: Option[Broadcast[SerializableConfiguration]] = None
) extends GpuDeltaParquetFileFormatBase {

  if (hasDeletionVectorMap) {
    require(tablePath.isDefined && !isSplittable && disablePushDowns,
      "Wrong arguments for Delta table scan with deletion vectors")
  }

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  def hasDeletionVectorMap: Boolean = {
    broadcastDvMap.isDefined && broadcastHadoopConf.isDefined
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = isSplittable

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

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf,
      metrics,
      alluxioPathReplacementMap)

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val results = schemaWithIndices.filter(_._1.name == name)
      if (results.length > 1) {
        throw new IllegalArgumentException(
          s"There are more than one column with name=`$name` requested in the reader output")
      }
      results.headOption.map(e => ColumnMetadata(e._2, e._1))
    }

    val isRowDeletedColumn = findColumn(IS_ROW_DELETED_COLUMN_NAME)
    val rowIndexColumn = findColumn(ROW_INDEX_COLUMN_NAME)

    // We don't have any additional columns to generate, just return the original reader as is.
    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) return dataReader
    require(!isSplittable, "Cannot generate row index related metadata with file splitting")
    require(disablePushDowns, "Cannot generate row index related metadata with filter pushdown")
    if (hasDeletionVectorMap && isRowDeletedColumn.isEmpty) {
      throw new IllegalArgumentException(s"Expected a column ${IS_ROW_DELETED_COLUMN_NAME} in the schema")
    }

//    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    val delVecs = broadcastDvMap
    val maxDelVecScatterBatchSize = RapidsConf
      .DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE
      .get(sparkSession.sessionState.conf)

    val delVecScatterTimeMetric = metrics(GpuMetric.DELETION_VECTOR_SCATTER_TIME)
    val delVecSizeMetric = metrics(GpuMetric.DELETION_VECTOR_SIZE)

    (file: PartitionedFile) => {
      val input = dataReader(file)
      val dv = delVecs.flatMap(_.value.get(new URI(file.filePath.toString())))
        .map { dv =>
          delVecSizeMetric += dv.descriptor.inlineData.length
          RoaringBitmapWrapper.deserializeFromBytes(dv.descriptor.inlineData).inner
        }
//      val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
      addMetadataColumnToIterator(prepareSchema(requiredSchema),
        dv,
        input.asInstanceOf[Iterator[ColumnarBatch]],
        maxDelVecScatterBatchSize,
        delVecScatterTimeMetric
      ).asInstanceOf[Iterator[InternalRow]]
    }
  }

  /**
   * Modifies the data read from underlying Parquet reader by populating one or both of the
   * following metadata columns.
   *   - [[IS_ROW_DELETED_COLUMN_NAME]] - row deleted status from deletion vector corresponding
   *   to this file
   *   - [[ROW_INDEX_COLUMN_NAME]] - index of the row within the file. Note, this column is only
   *     populated when we are not using _metadata.row_index column.
   */
//  private def iteratorWithAdditionalMetadataColumns(
//                                                     partitionedFile: PartitionedFile,
//                                                     iterator: Iterator[Object],
//                                                     isRowDeletedColumnOpt: Option[ColumnMetadata],
//                                                     rowIndexColumnOpt: Option[ColumnMetadata],
//                                                     useOffHeapBuffers: Boolean,
//                                                     serializableHadoopConf: SerializableConfiguration,
//                                                     useMetadataRowIndex: Boolean): Iterator[Object] = {
//    require(!useMetadataRowIndex || rowIndexColumnOpt.isDefined,
//      "useMetadataRowIndex is enabled but rowIndexColumn is not defined.")
//
//    val rowIndexFilterOpt = isRowDeletedColumnOpt.map { col =>
//      // Fetch the DV descriptor from the broadcast map and create a row index filter
//      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
//        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED)
//      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
//        .get(FILE_ROW_INDEX_FILTER_TYPE)
//      if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
//        val rowIndexFilter = filterTypeOpt.get match {
//          case RowIndexFilterType.IF_CONTAINED => DropMarkedRowsFilter
//          case RowIndexFilterType.IF_NOT_CONTAINED => KeepMarkedRowsFilter
//          case unexpectedFilterType => throw new IllegalStateException(
//            s"Unexpected row index filter type: ${unexpectedFilterType}")
//        }
//        rowIndexFilter.createInstance(
//          DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get.asInstanceOf[String]),
//          serializableHadoopConf.value,
//          tablePath.map(new Path(_)))
//      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
//        throw new IllegalStateException(
//          s"Both ${FILE_ROW_INDEX_FILTER_ID_ENCODED} and ${FILE_ROW_INDEX_FILTER_TYPE} " +
//            "should either both have values or no values at all.")
//      } else {
//        KeepAllRowsFilter
//      }
//    }
//
//    // We only generate the row index column when predicate pushdown is not enabled.
//    val rowIndexColumnToWriteOpt = if (useMetadataRowIndex) None else rowIndexColumnOpt
//    val metadataColumnsToWrite =
//      Seq(isRowDeletedColumnOpt, rowIndexColumnToWriteOpt).filter(_.nonEmpty).map(_.get)
//
//    // When metadata.row_index is not used there is no way to verify the Parquet index is
//    // starting from 0. We disable the splits, so the assumption is ParquetFileFormat respects
//    // that.
//    var rowIndex: Long = 0
//
//    // Used only when non-column row batches are received from the Parquet reader
//    val tempVector = new OnHeapColumnVector(1, ByteType)
//
//    iterator.map { row =>
//      row match {
//        case batch: ColumnarBatch => // When vectorized Parquet reader is enabled.
//          val size = batch.numRows()
//          // Create vectors for all needed metadata columns.
//          // We can't use the one from Parquet reader as it set the
//          // [[WritableColumnVector.isAllNulls]] to true and it can't be reset with using any
//          // public APIs.
//          trySafely(useOffHeapBuffers, size, metadataColumnsToWrite) { writableVectors =>
//            val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]
//
//            // When predicate pushdown is enabled we use _metadata.row_index. Therefore,
//            // we only need to construct the isRowDeleted column.
//            var index = 0
//            isRowDeletedColumnOpt.foreach { columnMetadata =>
//              val isRowDeletedVector = writableVectors(index)
//              if (useMetadataRowIndex) {
//                rowIndexFilterOpt.get.materializeIntoVectorWithRowIndex(
//                  size, batch.column(rowIndexColumnOpt.get.index), isRowDeletedVector)
//              } else {
//                rowIndexFilterOpt.get
//                  .materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
//              }
//              indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
//              index += 1
//            }
//
//            rowIndexColumnToWriteOpt.foreach { columnMetadata =>
//              val rowIndexVector = writableVectors(index)
//              // populate the row index column value.
//              for (i <- 0 until size) {
//                rowIndexVector.putLong(i, rowIndex + i)
//              }
//
//              indexVectorTuples += (columnMetadata.index -> rowIndexVector)
//              index += 1
//            }
//
//            val newBatch = replaceVectors(batch, indexVectorTuples.toSeq: _*)
//            rowIndex += size
//            newBatch
//          }
//
//        case columnarRow: ColumnarBatchRow =>
//          // When vectorized reader is enabled but returns immutable rows instead of
//          // columnar batches [[ColumnarBatchRow]]. So we have to copy the row as a
//          // mutable [[InternalRow]] and set the `row_index` and `is_row_deleted`
//          // column values. This is not efficient. It should affect only the wide
//          // tables. https://github.com/delta-io/delta/issues/2246
//          throw new RuntimeException(s"Parquet reader returned a ColumnarBatchRow row type")
//        case rest: InternalRow => // When vectorized Parquet reader is disabled
//          // Temporary vector variable used to get DV values from RowIndexFilter
//          // Currently the RowIndexFilter only supports writing into a columnar vector
//          // and doesn't have methods to get DV value for a specific row index.
//          // TODO: This is not efficient, but it is ok given the default reader is vectorized
//          throw new RuntimeException(s"Parquet reader returned an InternalRow row type")
//        case others =>
//          throw new RuntimeException(
//            s"Parquet reader returned an unknown row type: ${others.getClass.getName}")
//      }
//    }
//  }
}

object GpuDeltaParquetFileFormat {
  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
  }

  def convertToGpu(fmt: DeltaParquetFileFormat): GpuDeltaParquetFileFormat = {
    GpuDeltaParquetFileFormat(fmt.columnMappingMode, fmt.referenceSchema, fmt.isSplittable,
      fmt.disablePushDowns, fmt.broadcastDvMap, fmt.tablePath, fmt.broadcastHadoopConf)
  }
}
