/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.io.IOException

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor, Metadata, Protocol}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * This is the version 2 of the Delta Parquet file format implementation, which uses the
 * new deletion vector APIs in cuDF. Unlike the previous version where deletion vectors
 * are materialized into boolean columns and processed in a FilterExec, the deletion vectors
 * are passed to the Parquet reader and applied during reading with no materialization.
 *
 * Note that we do not support the DataSourceV2 API for Delta Lake tables yet.
 */
class GpuDeltaParquetFileFormatBase2(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
) extends com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormat with Logging {

  // Validate either we have all arguments for DV enabled read or none of them.

  if (hasTablePath) {
    SparkSession.getActiveSession.map { session =>
      val useMetadataRowIndex =
        session.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

      // We currently support the 'useMetadataRowIndex' mode only.
      // It should fall back to 'GpuDeltaParquetFileFormatBase' when 'useMetadataRowIndex = false'.
      require(useMetadataRowIndex,
        "useMetadataRowIndex must be enabled to support Delta table scan with deletion vectors")

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

  /**
   * This function is overridden as Delta 3.3+ has an extra `PARQUET_FIELD_NESTED_IDS_METADATA_KEY`
   * key to remove from the metadata, which does not exist in earlier versions.
   */
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

  /**
   * Prepares filters so that they can be pushed down into the Parquet reader.
   *
   * If column mapping is enabled, then logical column names in the filters will be replaced with
   * their corresponding physical column names. This is necessary as the Parquet files will use
   * physical column names, and the requested schema pushed down in the Parquet reader will also use
   * physical column names.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!optimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(QuotingUtils.quoteIfNeeded).mkString("."),
              physicalName.map(QuotingUtils.quoteIfNeeded).mkString("."))
        }
      filters.flatMap(RapidsDeletionVectors.translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = optimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  /////////////////////////////////
  //
  // Extensions for PERFILE reader
  //
  /////////////////////////////////

  override def createPartitionReaderFactory(sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Seq[Filter],
      rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      options: Map[String, String]) : GpuParquetPartitionReaderFactoryBase = {
    GpuDeltaParquetPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      partitionSchema,
      filters.toArray,
      rapidsConf,
      metrics,
      options)
  }

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
    super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      // Prepare filters for pushdown
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)
  }

  case class GpuDeltaParquetPartitionReaderFactory(
      @transient sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Array[Filter],
      @transient rapidsConf: RapidsConf,
      metrics: Map[String, GpuMetric],
      params: Map[String, String]
  ) extends GpuParquetPartitionReaderFactoryBase(sqlConf, broadcastedConf, dataSchema,
    readDataSchema, partitionSchema, rapidsConf, metrics = metrics, params = params) {

    protected override def buildBaseColumnarParquetReader(
        file: PartitionedFile): PartitionReader[ColumnarBatch] = {
      // we need to copy the Hadoop Configuration because filter push down can mutate it,
      // which can affect other tasks.
      val conf = new Configuration(broadcastedConf.value.value)
      val startTime = System.nanoTime()
      val singleFileInfo = filterHandler.filterBlocks(fileIO, footerReadType, file, conf, filters,
        readDataSchema)
      metrics.get(FILTER_TIME).foreach {
        _ += (System.nanoTime() - startTime)
      }
      new DeltaParquetPartitionReader(fileIO, conf, file, singleFileInfo.filePath,
        singleFileInfo.blocks, singleFileInfo.schema, isCaseSensitive, readDataSchema,
        debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
        targetSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, compressCfg,
        metrics, singleFileInfo.dateRebaseMode, singleFileInfo.timestampRebaseMode,
        singleFileInfo.hasInt96Timestamps, readUseFieldId)
    }
  }

  class DeltaParquetPartitionReader(
      override val fileIO: RapidsFileIO,
      override val conf: Configuration,
      split: PartitionedFile,
      filePath: Path,
      clippedBlocks: Iterable[BlockMetaData],
      clippedParquetSchema: MessageType,
      override val isSchemaCaseSensitive: Boolean,
      readDataSchema: StructType,
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      maxReadBatchSizeRows: Integer,
      maxReadBatchSizeBytes: Long,
      targetBatchSizeBytes: Long,
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      override val compressCfg: CpuCompressionConfig,
      override val execMetrics: Map[String, GpuMetric],
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      useFieldId: Boolean) extends AbstractParquetPartitionReader(
    fileIO, conf, split, filePath, clippedBlocks, clippedParquetSchema, isSchemaCaseSensitive,
    readDataSchema, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    compressCfg, execMetrics, useFieldId) {

    override protected def readBuffer(
        parquetOpts: ParquetOptions,
        colTypes: Array[DataType],
        chunkedBlocks: Seq[BlockMetaData],
        dataBuffer: SpillableHostBuffer
    ): Iterator[ColumnarBatch] = {
      if (dataBuffer.length == 0) {
        dataBuffer.close()
        CachedGpuBatchIterator(EmptyTableReader, colTypes)
      } else {
        RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
          // Load deletion vectors into host memory if any.
          val dvDescriptorOpt = split.otherConstantMetadataColumnValues
            .get(FILE_ROW_INDEX_FILTER_ID_ENCODED).asInstanceOf[Option[String]]
          val filterTypeOpt = split.otherConstantMetadataColumnValues
            .get(FILE_ROW_INDEX_FILTER_TYPE).asInstanceOf[Option[RowIndexFilterType]]
          val maybeSerializedDV = tablePath.map(tp =>
            RapidsDeletionVectors.loadDeletionVector(fileIO, dvDescriptorOpt, filterTypeOpt, tp))
          closeOnExcept(maybeSerializedDV) { _ =>
            val (rowGroupOffsets, rowGroupNumRows) =
              RapidsDeletionVectors.getRowGroupMetadata(chunkedBlocks)
            val maybeDvInfo = maybeSerializedDV.map(serializedDV =>
              new DeletionVector.DeletionVectorInfo(serializedDV, rowGroupOffsets, rowGroupNumRows))

            val hostBuf = dataBuffer.getDataHostBuffer()
            // Duplicate request is ok, and start to use the GPU just after the host
            // buffer is ready to not block CPU things.
            GpuSemaphore.acquireIfNecessary(TaskContext.get())
            val producer = if (maybeDvInfo.isDefined) {
              // MakeParquetTableWithDVProducer will try to close the hostBuf and dvInfo
              MakeParquetTableWithDVProducer(
                useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes, conf,
                targetBatchSizeBytes, parquetOpts,
                Array(hostBuf), metrics,
                dateRebaseMode, timestampRebaseMode,
                isSchemaCaseSensitive,
                useFieldId, readDataSchema,
                clippedParquetSchema, Array(split),
                debugDumpPrefix, debugDumpAlways,
                deletionVectorInfos = Array(maybeDvInfo.get)
              )
            } else {
              // MakeParquetTableProducer will try to close the hostBuf
              MakeParquetTableProducer(useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes, conf,
                targetBatchSizeBytes, parquetOpts,
                Array(hostBuf), metrics,
                dateRebaseMode, timestampRebaseMode,
                hasInt96Timestamps, isSchemaCaseSensitive,
                useFieldId, readDataSchema,
                clippedParquetSchema, Array(split),
                debugDumpPrefix, debugDumpAlways
              )
            }
            CachedGpuBatchIterator(producer, colTypes)
          }
        }
      }
    }
  }

  ///////////////////////////////////////
  //
  // Extensions for multi-threaded reader
  //
  ///////////////////////////////////////

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val poolConf = ThreadPoolConfBuilder(fileScan.rapidsConf)
    GpuDeltaParquetMultiFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      prepareSchema(fileScan.relation.dataSchema),
      prepareSchema(fileScan.requiredSchema),
      prepareSchema(fileScan.readPartitionSchema),
      prepareFiltersForRead(pushedFilters).toArray,
      fileScan.rapidsConf,
      poolConf,
      fileScan.allMetrics,
      fileScan.queryUsesInputFile)
  }

  case class GpuDeltaParquetMultiFilePartitionReaderFactory(
      @transient sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Array[Filter],
      @transient rapidsConf: RapidsConf,
      poolConfBuilder: ThreadPoolConfBuilder,
      metrics: Map[String, GpuMetric],
      queryUsesInputFile: Boolean)
    extends AbstractGpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
      dataSchema, readDataSchema, partitionSchema, filters, rapidsConf, poolConfBuilder,
      metrics, queryUsesInputFile) with Logging {

    logDebug("Using GpuDeltaParquetMultiFilePartitionReaderFactory for multi-threaded Parquet " +
      "reading with deletion vectors")

    override protected def createBaseMultiFileCloudReader(
        fileIO: RapidsFileIO,
        conf: Configuration,
        files: Array[PartitionedFile],
        filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
        isSchemaCaseSensitive: Boolean,
        debugDumpPrefix: Option[String],
        debugDumpAlways: Boolean,
        maxReadBatchSizeRows: Integer,
        maxReadBatchSizeBytes: Long,
        targetBatchSizeBytes: Long,
        maxGpuColumnSizeBytes: Long,
        useChunkedReader: Boolean,
        maxChunkedReaderMemoryUsageSizeBytes: Long,
        compressCfg: CpuCompressionConfig,
        execMetrics: Map[String, GpuMetric],
        partitionSchema: StructType,
        poolConf: ThreadPoolConf,
        maxNumFileProcessed: Int,
        ignoreMissingFiles: Boolean,
        ignoreCorruptFiles: Boolean,
        useFieldId: Boolean,
        queryUsesInputFile: Boolean,
        keepReadsInOrder: Boolean,
        combineConf: CombineConf
    ): AbstractMultiFileCloudParquetPartitionReader = {
      new MultiFileCloudDeltaParquetPartitionReader(
        fileIO,
        conf,
        files,
        filterFunc,
        isSchemaCaseSensitive,
        debugDumpPrefix,
        debugDumpAlways,
        maxReadBatchSizeRows,
        maxReadBatchSizeBytes,
        targetBatchSizeBytes,
        maxGpuColumnSizeBytes,
        useChunkedReader,
        maxChunkedReaderMemoryUsageSizeBytes,
        compressCfg,
        execMetrics,
        partitionSchema,
        poolConf,
        maxNumFileProcessed,
        ignoreMissingFiles,
        ignoreCorruptFiles,
        useFieldId,
        queryUsesInputFile,
        keepReadsInOrder,
        combineConf
      )
    }
  }

  class MultiFileCloudDeltaParquetPartitionReader(
      override val fileIO: RapidsFileIO,
      override val conf: Configuration,
      files: Array[PartitionedFile],
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      override val isSchemaCaseSensitive: Boolean,
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      maxReadBatchSizeRows: Integer,
      maxReadBatchSizeBytes: Long,
      targetBatchSizeBytes: Long,
      maxGpuColumnSizeBytes: Long,
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      override val compressCfg: CpuCompressionConfig,
      override val execMetrics: Map[String, GpuMetric],
      partitionSchema: StructType,
      poolConf: ThreadPoolConf,
      maxNumFileProcessed: Int,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      useFieldId: Boolean,
      queryUsesInputFile: Boolean,
      keepReadsInOrder: Boolean,
      combineConf: CombineConf)
    extends AbstractMultiFileCloudParquetPartitionReader(fileIO, conf, files, filterFunc,
      isSchemaCaseSensitive, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes, compressCfg, execMetrics, partitionSchema,
      poolConf, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles, useFieldId,
      queryUsesInputFile, keepReadsInOrder, combineConf) {

    override protected def readBufferToBatches(
        buffer: HostMemoryBuffersWithMetaData): Iterator[ColumnarBatch] = {
      val deltaBuffer = buffer.asInstanceOf[DeltaParquetHostMemoryBuffersWithMetaData]
      val memBuffersAndSize = deltaBuffer.memBuffersAndSizes
      val hmbAndInfo = memBuffersAndSize.head

      val dateRebaseMode: DateTimeRebaseMode = deltaBuffer.dateRebaseMode
      val timestampRebaseMode: DateTimeRebaseMode = deltaBuffer.timestampRebaseMode
      val hasInt96Timestamps: Boolean = deltaBuffer.hasInt96Timestamps
      val clippedSchema: MessageType = deltaBuffer.clippedSchema
      val readDataSchema: StructType = deltaBuffer.readSchema
      val partedFile: PartitionedFile = deltaBuffer.partitionedFile
      val hostBuffers = hmbAndInfo.hmbs
      val allPartValues: Option[Array[(Long, InternalRow)]] = deltaBuffer.allPartValues
      val dvMetadata: DeletionVectorMetadata = deltaBuffer.dvMetadata.head

      val parseOpts = closeOnExcept(hostBuffers) { _ =>
        getParquetOptions(readDataSchema, clippedSchema, useFieldId)
      }
      val colTypes = readDataSchema.fields.map(f => f.dataType)

      withResource(hostBuffers) { _ =>
        RmmRapidsRetryIterator.withRetryNoSplit {
          closeOnExcept(hostBuffers.safeMap(_.getDataHostBuffer())) { hostBufs =>
            val dvInfo: Array[DeletionVector.DeletionVectorInfo] = if (hasTablePath) {
              dvMetadata.metadatas.map {
                singleDVMeta =>
                  val serializedDV = RapidsDeletionVectors.loadDeletionVector(
                    fileIO, singleDVMeta.dvDescriptorOpt, singleDVMeta.filterTypeOpt, tablePath.get)
                  new DeletionVector.DeletionVectorInfo(serializedDV,
                    singleDVMeta.rowGroupOffsets, singleDVMeta.rowGroupNumRows)
              }
            } else {
              Array()
            }
            closeOnExcept(dvInfo.map(_.serializedBitmap)) { _ =>
              // Duplicate request is ok, and start to use the GPU just after the host
              // buffer is ready to not block CPU things.
              GpuSemaphore.acquireIfNecessary(TaskContext.get())

              val tableReader = if (hasTablePath) {
                // The MakeParquetTableWithDVProducer will close the input buffers
                MakeParquetTableWithDVProducer(
                  useChunkedReader,
                  maxChunkedReaderMemoryUsageSizeBytes,
                  conf, targetBatchSizeBytes,
                  parseOpts,
                  hostBufs, metrics,
                  dateRebaseMode, timestampRebaseMode,
                  isSchemaCaseSensitive, useFieldId, readDataSchema, clippedSchema, files,
                  debugDumpPrefix, debugDumpAlways,
                  dvInfo)
              } else {
                // The MakeParquetTableProducer will close the input buffers
                MakeParquetTableProducer(
                  useChunkedReader,
                  maxChunkedReaderMemoryUsageSizeBytes,
                  conf, targetBatchSizeBytes,
                  parseOpts, hostBufs, metrics,
                  dateRebaseMode, timestampRebaseMode,
                  hasInt96Timestamps, isSchemaCaseSensitive,
                  useFieldId, readDataSchema, clippedSchema,
                  files, debugDumpPrefix, debugDumpAlways
                )
              }

              val batchIter = CachedGpuBatchIterator(tableReader, colTypes)

              if (allPartValues.isDefined) {
                val allPartInternalRows = allPartValues.get.map(_._2)
                // rowsPerPartition has been adjusted already to account only the alive rows.
                val rowsPerPartition = allPartValues.get.map(_._1)
                new GpuColumnarBatchWithPartitionValuesIterator(batchIter, allPartInternalRows,
                  rowsPerPartition, partitionSchema, maxGpuColumnSizeBytes)
              } else {
                // this is a bit weird, we don't have number of rows when allPartValues isn't
                // filled in so can't use GpuColumnarBatchWithPartitionValuesIterator
                batchIter.flatMap { batch =>
                  // we have to add partition values here for this batch, we already verified that
                  // its not different for all the blocks in this batch
                  BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
                    partedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
                }
              }
            }
          }
        }
      }
    }

    /**
     * Deletion vector metadata for a single host memory buffer containing a part of data.
     */
    private case class SingleBufferDVMetadata(
        dvDescriptorOpt: Option[String],
        filterTypeOpt: Option[RowIndexFilterType],
        rowGroupOffsets: Array[Long],
        rowGroupNumRows: Array[Int]
    )

    private case class DeletionVectorMetadata(
        metadatas: Array[SingleBufferDVMetadata]
    )

    private object DeletionVectorMetadata {
      def forSingleBuffer(
          dvDescriptorOpt: Option[String],
          filterTypeOpt: Option[RowIndexFilterType],
          rowGroupOffsets: Array[Long],
          rowGroupNumRows: Array[Int]
      ) = {
        DeletionVectorMetadata(
          Array(
            SingleBufferDVMetadata(dvDescriptorOpt, filterTypeOpt, rowGroupOffsets, rowGroupNumRows)
          )
        )
      }

      def combine(metadatas: Array[DeletionVectorMetadata]): DeletionVectorMetadata = {
        DeletionVectorMetadata(metadatas.flatMap(_.metadatas))
      }
    }

    private case class DeltaParquetHostMemoryEmptyMetaData(
        override val partitionedFile: PartitionedFile,
        bufferSize: Long,
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        numRows: Long,
        override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
      extends HostMemoryEmptyMetaData {}

    private case class DeltaParquetHostMemoryBuffersWithMetaData(
        override val partitionedFile: PartitionedFile,
        override val memBuffersAndSizes: Array[SingleHMBAndMeta],
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        override val allPartValues: Option[Array[(Long, InternalRow)]],
        // deletion vector metadata. should be aligned with memBuffersAndSizes if deletion vectors
        // are present.
        dvMetadata: Array[DeletionVectorMetadata]
    ) extends HostMemoryBuffersWithMetaData {

      override def consumeHeadBuffer(): HostMemoryBuffersWithMetaData = {
        require(memBuffersAndSizes.nonEmpty,
          "consumeHeadBuffer called on HostMemoryBuffersWithMetaData with no buffers")
        require(memBuffersAndSizes.length == dvMetadata.length,
          "memBuffersAndSizes and dvMetadata should have the same length")
        val (remainingBuffers, newDvMetadata) = if (memBuffersAndSizes.length > 1) {
          (memBuffersAndSizes.drop(1), dvMetadata.drop(1))
        } else {
          (Array.empty[SingleHMBAndMeta], Array.empty[DeletionVectorMetadata])
        }
        this.copy(memBuffersAndSizes = remainingBuffers, dvMetadata = newDvMetadata)
      }
    }

    override protected def newHMEmptyMetadataForChunks(
        partitionedFile: PartitionedFile,
        bufferSize: Long,
        bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        numRows: Long
    ): HostMemoryEmptyMetaData = {
      DeltaParquetHostMemoryEmptyMetaData(
        partitionedFile,
        bufferSize,
        bytesRead,
        dateRebaseMode,
        timestampRebaseMode,
        hasInt96Timestamps,
        clippedSchema,
        readSchema,
        numRows
      )
    }

    override protected def newCombinedHMEmptyMetadata(emptyMeta: CombinedEmptyMeta,
        nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData = {
      val metaForEmpty = emptyMeta.metaForEmpty
      DeltaParquetHostMemoryEmptyMetaData(
        metaForEmpty.partitionedFile, // just pick one since not used
        emptyMeta.emptyBufferSize,
        emptyMeta.emptyTotalBytesRead,
        metaForEmpty.dateRebaseMode, // these shouldn't matter since data is empty
        metaForEmpty.timestampRebaseMode, // these shouldn't matter since data is empty
        metaForEmpty.hasInt96Timestamps, // these shouldn't matter since data is empty
        metaForEmpty.clippedSchema,
        metaForEmpty.readSchema,
        emptyMeta.emptyNumRows,
        Some(nonEmptyMeta.allPartValues)
      )
    }

    override protected def newHMBWithMetaDataForChunks(
        partitionedFile: PartitionedFile,
        memBuffersAndSize: Array[SingleHMBAndMeta],
        bytesRead: Long,
        fileBlockMeta: ParquetFileInfoWithBlockMeta
    ): HostMemoryBuffersWithMetaData = {
      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED).asInstanceOf[Option[String]]
      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE).asInstanceOf[Option[RowIndexFilterType]]
      val dvMetadataArray = memBuffersAndSize.map { singleHMBAndMeta =>
        val dataBlock = singleHMBAndMeta.blockMeta.map(_.asInstanceOf[ParquetDataBlock].dataBlock)
        val (rowGroupOffsets, rowGroupNumRows) = RapidsDeletionVectors
          .getRowGroupMetadata(dataBlock)
        DeletionVectorMetadata.forSingleBuffer(
          dvDescriptorOpt, filterTypeOpt, rowGroupOffsets, rowGroupNumRows)
      }

      DeltaParquetHostMemoryBuffersWithMetaData(
        partitionedFile,
        memBuffersAndSize,
        bytesRead,
        fileBlockMeta.dateRebaseMode,
        fileBlockMeta.timestampRebaseMode,
        fileBlockMeta.hasInt96Timestamps,
        fileBlockMeta.schema,
        fileBlockMeta.readSchema,
        None,
        dvMetadataArray
      )
    }

    override protected def newCombinedHMBWithMetaData(
        combinedMeta: CombinedMeta,
        newHmbBufferInfo: SingleHMBAndMeta,
        offset: Long
    ): HostMemoryBuffersWithMetaData = {
      val metaToUse = combinedMeta.firstNonEmpty
      val toCombine = combinedMeta.toCombine
        .collect { case hmb: DeltaParquetHostMemoryBuffersWithMetaData => hmb }
      val combinedDVMeta = DeletionVectorMetadata.combine(toCombine.flatMap(_.dvMetadata))

      DeltaParquetHostMemoryBuffersWithMetaData(
        metaToUse.partitionedFile,
        Array(newHmbBufferInfo),
        offset,
        metaToUse.dateRebaseMode,
        metaToUse.timestampRebaseMode,
        metaToUse.hasInt96Timestamps,
        metaToUse.clippedSchema,
        metaToUse.readSchema,
        Some(combinedMeta.allPartValues),
        Array(combinedDVMeta)
      )
    }

    override protected def computeNumRowsAlive(
        totalNumRows: Long,
        file: PartitionedFile
    ): Int = {
      val dvDescriptorOpt = file.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED).asInstanceOf[Option[String]]
      val filterTypeOpt = file.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE).asInstanceOf[Option[RowIndexFilterType]]

      val numDeletedRows = if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
        val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get)
        dvDesc.cardinality
      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
        throw new IllegalStateException(
          "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
      } else {
        0
      }

      require(numDeletedRows <= totalNumRows,
        s"Deletion vector cardinality ($numDeletedRows) exceeds file row count ($totalNumRows)")
      Math.toIntExact(totalNumRows - numDeletedRows)
    }
  }
}

/**
 * A simple wrapper to adapt the DeletionVector.ParquetChunkedReader to the ChunkedReader interface
 * expected by AbstractParquetTableReader.
 */
case class DeltaParquetChunkedReader(delegate: DeletionVector.ParquetChunkedReader)
  extends ChunkedReader {
  override def hasNext: Boolean = delegate.hasNext
  override def next: Table = delegate.readChunk()
  override def close(): Unit = delegate.close()
}

/**
 * A chunked reader for Parquet files with deletion vectors.
 */
case class DeltaParquetTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    opts: ParquetOptions,
    buffers: Array[HostMemoryBuffer],
    metrics : Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    isSchemaCaseSensitive: Boolean,
    useFieldId: Boolean,
    readDataSchema: StructType,
    clippedParquetSchema: MessageType,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    dvInfos: Array[DeletionVector.DeletionVectorInfo]) extends AbstractParquetTableReader(
  conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, metrics,
  dateRebaseMode, timestampRebaseMode, isSchemaCaseSensitive, useFieldId, readDataSchema,
  clippedParquetSchema, splits, debugDumpPrefix, debugDumpAlways
) {

  logDebug("Using DeltaParquetTableReader for reading Parquet with deletion vectors")

  override protected val reader = DeltaParquetChunkedReader(
    DeletionVector.newParquetChunkedReader(chunkSizeByteLimit,
      maxChunkedReaderMemoryUsageSizeBytes, opts, buffers, dvInfos)
  )

  override protected val resources: Seq[AutoCloseable] =
    Seq(reader) ++ buffers ++ dvInfos.map(_.serializedBitmap)

  override protected def postProcessChunk(chunk: Table): Table = {
    // The cuDF reader prepends an extra index column in the output table.
    // We need to drop it before returning as we don't use it.
    RapidsDeletionVectors.dropFirstColumn(chunk)
  }
}

object MakeParquetTableWithDVProducer extends Logging {
  def apply(
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      conf: Configuration,
      chunkSizeByteLimit: Long,
      opts: ParquetOptions,
      buffers: Array[HostMemoryBuffer],
      metrics : Map[String, GpuMetric],
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      deletionVectorInfos: Array[DeletionVector.DeletionVectorInfo]
  ): GpuDataProducer[Table] = {

    require(deletionVectorInfos.nonEmpty,
      "MakeParquetTableWithDVProducer should be used only when deletion vectors are present")

    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
        logWarning(s"Wrote data for ${splits.mkString(", ")} to $p")
      }
    }
    if (useChunkedReader) {
      DeltaParquetTableReader(conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
        opts, buffers, metrics, dateRebaseMode, timestampRebaseMode,
        isSchemaCaseSensitive, useFieldId, readDataSchema, clippedParquetSchema,
        splits, debugDumpPrefix, debugDumpAlways, deletionVectorInfos)
    } else {
      val table = withResource(buffers) { _ =>
        withResource(deletionVectorInfos.map(_.serializedBitmap)) { _ =>
          try {
            RmmRapidsRetryIterator.withRetryNoSplit[Table] {
              NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
                DeletionVector.readParquet(opts, buffers, deletionVectorInfos)
              }
            }
          } catch {
            case e: Exception =>
              val dumpMsg = debugDumpPrefix.map { prefix =>
                if (!debugDumpAlways) {
                  val p = DumpUtils.dumpBuffer(conf, buffers, prefix, ".parquet")
                  s", data dumped to $p"
                } else {
                  ""
                }
              }.getOrElse("")
              throw new IOException(s"Error when processing ${splits.mkString("; ")}$dumpMsg", e)
          }
        }
      }
      // The cuDF reader prepends an extra index column in the output table.
      // We need to drop it before returning as we don't use it.
      val tableWithoutIndex = RapidsDeletionVectors.dropFirstColumn(table)
      closeOnExcept(tableWithoutIndex) { _ =>
        GpuParquetScan.throwIfRebaseNeededInExceptionMode(tableWithoutIndex, dateRebaseMode,
          timestampRebaseMode)
        if (readDataSchema.length < tableWithoutIndex.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${tableWithoutIndex.getNumberOfColumns} from ${splits.mkString("; ")}")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(tableWithoutIndex,
        clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
      val outputTable = GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode,
        timestampRebaseMode)
      new SingleGpuDataProducer(outputTable)
    }
  }
}
