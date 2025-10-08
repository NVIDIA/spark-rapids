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

package com.nvidia.spark.rapids.delta.common

import ai.rapids.cudf._
import ai.rapids.cudf.HostColumnVector._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormat
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.deletionvectors.StoredBitmap
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class GpuDeltaParquetFileFormatCommon(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormat with Logging {

  // Validate either we have all arguments for DV enabled read or none of them.
  if (hasTablePath) {
    SparkSession.getActiveSession.map { session =>
      val useMetadataRowIndex =
        session.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
      require(useMetadataRowIndex == optimizationsEnabled,
        "Wrong arguments for Delta table scan with deletion vectors")
    }
  }

  if (SparkSession.getActiveSession.isDefined) {
    val session = SparkSession.getActiveSession.get
    TypeWidening.assertTableReadable(session.sessionState.conf, protocol, metadata)
  }

  val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  override def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else schema
  }

  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!optimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(quoteIfNeeded).mkString("."),
              physicalName.map(quoteIfNeeded).mkString("."))
        }
      filters.flatMap(translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(sparkSession: SparkSession,
     options: Map[String, String],
     path: Path): Boolean = optimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {

    val useMetadataRowIndexConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = sparkSession.sessionState.conf.getConf(useMetadataRowIndexConf)

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)

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
    val rowIndexColumnName = ROW_INDEX_COLUMN_NAME

    val rowIndexColumn = findColumn(rowIndexColumnName)

    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) return dataReader
    if (isRowDeletedColumn.isEmpty) return dataReader

    require(useMetadataRowIndex || !optimizationsEnabled,
      "Cannot generate row index related metadata with file splitting or predicate pushdown")

    if (hasTablePath && isRowDeletedColumn.isEmpty) {
      throw new IllegalArgumentException(
        s"Expected a column $IS_ROW_DELETED_COLUMN_NAME in the schema")
    }
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    (file: PartitionedFile) => {
      val iter = dataReader(file)
      RapidsDeletionVectorUtils.iteratorWithAdditionalMetadataColumns(
        file,
        iter,
        isRowDeletedColumn,
        rowIndexColumn,
        tablePath,
        serializableHadoopConf,
        metrics).asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {

    if (fileScan.rapidsConf.isParquetCoalesceFileReadEnabled) {
      logWarning("Coalescing is not supported when `delta.enableDeletionVectors=true`, " +
        "using the multi-threaded reader. For more details on the Parquet reader types " +
        "please look at 'spark.rapids.sql.format.parquet.reader.type' config at " +
        "https://nvidia.github.io/spark-rapids/docs/additional-functionality/advanced_configs.html")
    }

    new DeltaMultiFileReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      prepareSchema(fileScan.requiredSchema),
      prepareSchema(fileScan.readPartitionSchema),
      pushedFilters,
      fileScan.rapidsConf,
      fileScan.allMetrics,
      useMetadataRowIndex = false,
      tablePath)
  }

  private def translateFilterForColumnMapping(
     filter: Filter,
     physicalNameMap: Map[String, String]): Option[Filter] = {
    object PhysicalAttribute {
      def unapply(attribute: String): Option[String] = {
        physicalNameMap.get(attribute)
      }
    }

    filter match {
      case EqualTo(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualTo(physicalAttribute, value))
      case EqualNullSafe(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualNullSafe(physicalAttribute, value))
      case GreaterThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThan(physicalAttribute, value))
      case GreaterThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThanOrEqual(physicalAttribute, value))
      case LessThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThan(physicalAttribute, value))
      case LessThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThanOrEqual(physicalAttribute, value))
      case In(PhysicalAttribute(physicalAttribute), values) =>
        Some(In(physicalAttribute, values))
      case IsNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNull(physicalAttribute))
      case IsNotNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNotNull(physicalAttribute))
      case And(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None) => Some(l)
          case (_, _) => newRight
        }
      case Or(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case (_, _) => None
        }
      case Not(child) =>
        translateFilterForColumnMapping(child, physicalNameMap).map(Not)
      case StringStartsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringStartsWith(physicalAttribute, value))
      case StringEndsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringEndsWith(physicalAttribute, value))
      case StringContains(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringContains(physicalAttribute, value))
      case AlwaysTrue() => Some(AlwaysTrue())
      case AlwaysFalse() => Some(AlwaysFalse())
      case _ =>
        logError(s"Failed to translate filter ${MDC(DeltaLogKeys.FILTER, filter)}")
        None
    }
  }
}

class DeltaMultiFileReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    useMetadataRowIndex: Boolean,
    tablePath: Option[String]
    ) extends GpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
      dataSchema, readDataSchema, partitionSchema,
      filters, rapidsConf, metrics, queryUsesInputFile = true) {

  private val schemaWithIndices = readDataSchema.fields.zipWithIndex
  def findColumn(name: String): Option[ColumnMetadata] = {
    val results = schemaWithIndices.filter(_._1.name == name)
    require(results.length <= 1,
      s"There are more than one column with name=`$name` requested in the reader output")
    results.headOption.map(e => ColumnMetadata(e._2, e._1))
  }

  private val isRowDeletedColumn = findColumn(IS_ROW_DELETED_COLUMN_NAME)
  private val rowIndexColumnName = ROW_INDEX_COLUMN_NAME

  private val rowIndexColumn = findColumn(rowIndexColumnName)

  override def createColumnarReader(p: InputPartition): PartitionReader[ColumnarBatch] = {
    val files = p.asInstanceOf[FilePartition].files
    val reader = super.createColumnarReader(p)
    new DeltaMultiFileParquetPartitionReader(files, reader,
      isRowDeletedColumn, rowIndexColumn, broadcastedConf.value, tablePath, metrics)
  }
}

class DeltaMultiFileParquetPartitionReader(
    files: Array[PartitionedFile],
    reader: PartitionReader[ColumnarBatch],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexColumnOpt: Option[ColumnMetadata],
    serializableConf: SerializableConfiguration,
    tablePath: Option[String],
    metrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] {

  private val filesMap = files.map(f => f.filePath.toString() -> f).toMap
  private var file: PartitionedFile = null
  private var rowIndex: Long = 0L
  private var rowIndexFilterOpt: Option[RapidsRowIndexFilterBase] = None

  override def next(): Boolean = {
    reader.next()
  }

  override def close(): Unit = {
    reader.close()
  }

  private def compareFile(file: PartitionedFile): Boolean = {
    InputFileUtils.getCurInputFilePath() == file.urlEncodedPath &&
      InputFileUtils.getCurInputFileStartOffset == file.start &&
      InputFileUtils.getCurInputFileLength == file.length
  }

  override def get(): ColumnarBatch = {
    val batch = reader.get()
    if (isRowDeletedColumnOpt.isEmpty) {
      return batch
    } else if (file == null || !compareFile(file)) {
      file = filesMap(InputFileUtils.getCurInputFilePath())
      rowIndex = 0
      rowIndexFilterOpt = RapidsDeletionVectorUtils
        .getRowIndexFilter(file, isRowDeletedColumnOpt, serializableConf, tablePath)
    }

    val newBatch = RapidsDeletionVectorUtils.processBatchWithDeletionVector(
      batch,
      rowIndex,
      isRowDeletedColumnOpt,
      rowIndexFilterOpt,
      rowIndexColumnOpt,
      metrics
    )
    rowIndex += batch.numRows()
    newBatch
  }
}

object RapidsDeletionVectorUtils {
  def processBatchWithDeletionVector(
    batch: ColumnarBatch,
    rowIndex: Long,
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexFilterOpt: Option[RapidsRowIndexFilterBase],
    rowIndexColumnOpt: Option[ColumnMetadata],
    metrics: Map[String, GpuMetric]): ColumnarBatch = replaceBatch(rowIndex,
      batch,
      batch.numRows(),
      rowIndexColumnOpt,
      isRowDeletedColumnOpt,
      rowIndexFilterOpt,
      metrics)

  def iteratorWithAdditionalMetadataColumns(
    partitionedFile: PartitionedFile,
    iterator: Iterator[Any],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexColumnOpt: Option[ColumnMetadata],
    tablePath: Option[String],
    serializableConf: SerializableConfiguration,
    metrics: Map[String, GpuMetric]): Iterator[Any] = {

    val rowIndexFilterOpt =
      getRowIndexFilter(partitionedFile, isRowDeletedColumnOpt, serializableConf, tablePath)

    var rowIndex = 0L

    iterator.map {
      case cb: ColumnarBatch =>
        val size = cb.numRows()
        val newBatch = replaceBatch(rowIndex, cb, size, rowIndexColumnOpt, isRowDeletedColumnOpt,
          rowIndexFilterOpt, metrics)
        rowIndex += size
        newBatch

      case other =>
        throw new RuntimeException("Parquet reader returned an unknown row type: " +
          s"${other.getClass.getName}")
    }
  }

  private def getRowIndexPosSimple(start: Long, end: Long): GpuColumnVector = {
    val size = (end - start).toInt
    withResource(Scalar.fromLong(start)) { startScalar =>
      GpuColumnVector.from(ColumnVector.sequence(startScalar, size), LongType)
    }
  }

  private def replaceVectors(
    batch: ColumnarBatch,
    indexVectorTuples: (Int, org.apache.spark.sql.vectorized.ColumnVector) *): ColumnarBatch = {
    val vectors = ArrayBuffer[org.apache.spark.sql.vectorized.ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced: Boolean = false
      for (indexVectorTuple <- indexVectorTuples) {
        val (index, vector) = indexVectorTuple
        if (index == i) {
          vectors += vector
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }

  def getRowIndexFilter(partitionedFile: PartitionedFile,
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    serializableHadoopConf: SerializableConfiguration,
    tablePath: Option[String]): Option[RapidsRowIndexFilterBase] = {
    isRowDeletedColumnOpt.map { _ =>
      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED)
      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE)
      if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
        val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(
          dvDescriptorOpt.get.asInstanceOf[String])
        val tp = tablePath.getOrElse(throw new IllegalStateException(
          "Table path is required for non-empty deletion vectors"))
        val dvStore = new HadoopFileSystemDVStore(serializableHadoopConf.value)
        val bitmap = StoredBitmap.create(dvDesc, new Path(tp)).load(dvStore)
        filterTypeOpt.get match {
          case RowIndexFilterType.IF_CONTAINED => new DropMarkedRowsFilterBase(bitmap)
          case RowIndexFilterType.IF_NOT_CONTAINED => new KeepMarkedRowsFilterBase(bitmap)
          case unexpectedFilterType => throw new IllegalStateException(
            s"Unexpected row index filter type: ${unexpectedFilterType}")
        }
      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
        throw new IllegalStateException(
          s"Both ${FILE_ROW_INDEX_FILTER_ID_ENCODED} and ${FILE_ROW_INDEX_FILTER_TYPE} " +
            "should either both have values or no values at all.")
      } else {
        new KeepAllRowsFilterBase
      }
    }
  }

  private def replaceBatch(rowIndex: Long,
    batch: ColumnarBatch,
    size: Int,
    rowIndexColumnOpt: Option[ColumnMetadata],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexFilterOpt: Option[RapidsRowIndexFilterBase],
    metrics: Map[String, GpuMetric]): ColumnarBatch = {

    var startTime = System.nanoTime()
    withResource(getRowIndexPosSimple(rowIndex, rowIndex + size)) { rowIndexGpuCol =>
      metrics("rowIndexColumnGenTime") += System.nanoTime() - startTime
      val indexVectorTuples = new ArrayBuffer[(Int, org.apache.spark.sql.vectorized.ColumnVector)]
      try {
        rowIndexColumnOpt.foreach { rowIndexCol =>
          indexVectorTuples += (rowIndexCol.index -> rowIndexGpuCol.incRefCount())
        }
        startTime = System.nanoTime()
        val isRowDeletedVector = rowIndexFilterOpt.get.materializeIntoVector(rowIndexGpuCol)
        metrics("isRowDeletedColumnGenTime") += System.nanoTime() - startTime
        indexVectorTuples += (isRowDeletedColumnOpt.get.index -> isRowDeletedVector)
        replaceVectors(batch, indexVectorTuples.toSeq: _*)
      } catch {
        case e: Throwable =>
          indexVectorTuples.map(_._2).safeClose(e)
          throw e
      }
    }
  }
}


