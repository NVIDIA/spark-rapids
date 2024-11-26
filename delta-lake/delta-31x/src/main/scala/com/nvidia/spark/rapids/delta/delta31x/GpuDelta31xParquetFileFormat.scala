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

package com.nvidia.spark.rapids.delta.delta31x

import java.net.URI

import com.nvidia.spark.rapids.{GpuMetric, RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.{GpuDeltaParquetFileFormat, RoaringBitmapWrapper}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.addMetadataColumnToIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, IdMapping, RowIndexFilterType}
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.deletionvectors.{DropMarkedRowsFilter, KeepAllRowsFilter, KeepMarkedRowsFilter}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchRow, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

case class GpuDelta31xParquetFileFormat(
    override val columnMappingMode: DeltaColumnMappingMode,
    override val referenceSchema: StructType,
    isSplittable: Boolean,
    disablePushDowns: Boolean,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]],
    tablePath: Option[String] = None,
    broadcastHadoopConf: Option[Broadcast[SerializableConfiguration]] = None
  ) extends GpuDeltaParquetFileFormat {

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

    val pushdownFilters = if (disablePushDowns) Seq.empty else filters

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      pushdownFilters,
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
      throw new IllegalArgumentException("Expected a column " +
        s"${IS_ROW_DELETED_COLUMN_NAME} in the schema")
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
      val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
      try {
        val iter = addMetadataColumnToIterator(prepareSchema(requiredSchema),
          dv,
          input.asInstanceOf[Iterator[ColumnarBatch]],
          maxDelVecScatterBatchSize,
          delVecScatterTimeMetric
        ).asInstanceOf[Iterator[InternalRow]]
        iteratorWithAdditionalMetadataColumns(file, iter, isRowDeletedColumn, isRowDeletedColumn,
          useOffHeapBuffers).asInstanceOf[Iterator[InternalRow]]
      } catch {
        case NonFatal(e) =>
          dataReader match {
            case resource: AutoCloseable => GpuDelta31xParquetFileFormat.closeQuietly(resource)
            case _ => // do nothing
          }
          throw e
      }
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
  private def iteratorWithAdditionalMetadataColumns(partitionedFile: PartitionedFile,
    iterator: Iterator[Any],
    isRowDeletedColumn: Option[ColumnMetadata],
    rowIndexColumn: Option[ColumnMetadata],
    useOffHeapBuffers: Boolean): Iterator[Any] = {
    val pathUri = partitionedFile.pathUri
    val rowIndexFilter = isRowDeletedColumn.map { col =>
      broadcastDvMap.get.value.get(pathUri).map { descriptorWithFilterType =>
        val dvDescriptor = descriptorWithFilterType.descriptor
        val filterType = descriptorWithFilterType.filterType
        filterType match {
          case RowIndexFilterType.IF_CONTAINED =>
            DropMarkedRowsFilter.createInstance(
              dvDescriptor,
              broadcastHadoopConf.get.value.value,
              tablePath.map(new Path(_)))
          case RowIndexFilterType.IF_NOT_CONTAINED =>
            KeepMarkedRowsFilter.createInstance(
              dvDescriptor,
              broadcastHadoopConf.get.value.value,
              tablePath.map(new Path(_)))
          case _ =>
            throw new IllegalArgumentException(s"Unexpected filter type ${filterType}")
        }
      }.getOrElse(KeepAllRowsFilter)
    }

    val metadataColumns = List(isRowDeletedColumn, rowIndexColumn).flatten

    var rowIndex: Long = 0L

    iterator.map {
      case batch: ColumnarBatch =>
        val size = batch.numRows()
        GpuDelta31xParquetFileFormat.trySafely(useOffHeapBuffers, size, metadataColumns) {
          writableVectors =>
            val indexVectorTuples = new ArrayBuffer[(Int, ColumnVector)]()
            var index = 0

            isRowDeletedColumn.foreach { columnMetadata =>
              val isRowDeletedVector = writableVectors(index)
              rowIndexFilter.get.materializeIntoVector(rowIndex,
                rowIndex + size, isRowDeletedVector)
              indexVectorTuples += (columnMetadata.index -> isRowDeletedVector)
              index += 1
            }

            rowIndexColumn.foreach { columnMetadata =>
              val rowIndexVector = writableVectors(index)
              // populate the row index column value.
              for (i <- 0 until size) {
                rowIndexVector.putLong(i, rowIndex + i)
              }

              indexVectorTuples += (columnMetadata.index -> rowIndexVector)
              index += 1
            }

            val newBatch =
              GpuDelta31xParquetFileFormat.replaceVectors(batch, indexVectorTuples.toSeq)
            rowIndex += size
            newBatch
        }

      case _: ColumnarBatchRow =>
        throw new RuntimeException("Received invalid type ColumnarBatchRow")
      case _: InternalRow =>
        throw new RuntimeException("Received invalid type InternalRow")

      case other =>
        throw new RuntimeException("Parquet reader returned an unknown row type: " +
          s"${other.getClass.getName}")
    }
  }
}

object GpuDelta31xParquetFileFormat {
  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
  }

  /** Utility method to create a new writable vector */
  private def newVector(useOffHeapBuffers: Boolean,
    size: Int, dataType: StructField): WritableColumnVector = {
    if (useOffHeapBuffers) {
      OffHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    } else {
      OnHeapColumnVector.allocateColumns(size, Seq(dataType).toArray)(0)
    }
  }

  /** Try the operation, if the operation fails release the created resource */
  private def trySafely[R <: WritableColumnVector, T](useOffHeapBuffers: Boolean,
     size: Int,
     columns: Seq[ColumnMetadata])(f: Seq[WritableColumnVector] => T): T = {
    val resources = new ArrayBuffer[WritableColumnVector](columns.size)
    try {
      columns.foreach(col => resources.append(newVector(useOffHeapBuffers, size, col.structField)))
      f(resources.toSeq)
    } catch {
      case NonFatal(e) =>
        resources.foreach(closeQuietly(_))
        throw e
    }
  }

  /** Utility method to quietly close an [[AutoCloseable]] */
  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try {
        closeable.close()
      } catch {
        case NonFatal(_) => // ignore
      }
    }
  }

  def replaceVectors(
    batch: ColumnarBatch,
    indexVectorTuples: Seq[(Int, ColumnVector)]): ColumnarBatch = {
    val vectors = new ArrayBuffer[ColumnVector]()

    for (i <- 0 until batch.numCols()) {
      indexVectorTuples.find(_._1 == i) match {
        case Some((_, newVector)) => vectors += newVector
        case None => vectors += batch.column(i)
      }
    }

    new ColumnarBatch(vectors.toArray, batch.numRows())
  }
}
