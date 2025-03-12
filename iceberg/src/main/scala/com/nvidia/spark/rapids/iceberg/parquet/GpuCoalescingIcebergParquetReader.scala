package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}

import com.nvidia.spark.rapids.{CpuCompressionConfig, DateTimeRebaseMode, ExtraInfo, MultiFileParquetPartitionReader, ParquetDataBlock, ParquetExtraInfo, ParquetSchemaWrapper, ParquetSingleDataBlockMeta, SingleDataBlockInfo}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuCoalescingIcebergParquetReader(val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {

  private var inited = false
  private lazy val reader = createParquetReader()

  override def close(): Unit = {
    if (inited) {
      reader.close()
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = reader.get()

  private def createParquetReader() = {
    val clippedBlocks = files.map(f => (f, super.filterParquetBlocks(f, conf.expectedSchema)))
      .flatMap {
        case (file, info) =>
          val postProcessor = new GpuParquetReaderPostProcessor(
            info,
            constantsProvider(file),
            conf.expectedSchema)

          info.blocks.map { block =>
            ParquetSingleDataBlockMeta(
              info.filePath,
              ParquetDataBlock(block, CpuCompressionConfig.disabled()),
              InternalRow.empty,
              ParquetSchemaWrapper(info.schema),
              info.readSchema,
              IcebergParquetExtraInfo(
                info.dateRebaseMode,
                info.timestampRebaseMode,
                info.hasInt96Timestamps,
                postProcessor))
          }
      }

    inited = true

    new MultiFileParquetPartitionReader(
      conf.parquetConf.conf,
      files.map(_.sparkPartitionedFile).toArray,
      clippedBlocks,
      conf.parquetConf.caseSensitive,
      conf.parquetConf.parquetDebugDumpPrefix,
      conf.parquetConf.parquetDebugDumpAlways,
      conf.parquetConf.maxBatchSizeRows,
      conf.parquetConf.maxBatchSizeBytes,
      conf.parquetConf.targetBatchSizeBytes,
      conf.parquetConf.maxGpuColumnSizeBytes,
      conf.parquetConf.useChunkedReader,
      conf.parquetConf.maxChunkedReaderMemoryUsageSizeBytes,
      CpuCompressionConfig.disabled(),
      conf.parquetConf.metrics,
      new StructType(), // partitionSchema
      conf.parquetConf.threadConf.asInstanceOf[MultiFile].numThreads,
      false, // ignoreMissingFiles
      false, // ignoreCorruptFiles
      false) // useFieldId
      {
      override def checkIfNeedToSplitDataBlock(currentBlockInfo: SingleDataBlockInfo,
          nextBlockInfo: SingleDataBlockInfo): Boolean = {
        // Iceberg should always use field id for matching columns, so we should always disable
        // coalescing.
        true
      }

      override def finalizeOutputBatch(batch: ColumnarBatch,
          extraInfo: ExtraInfo): ColumnarBatch = {
        extraInfo
          .asInstanceOf[IcebergParquetExtraInfo]
          .postProcessor
          .process(batch)
      }
    }
  }
}

private case class IcebergParquetExtraInfo(override val dateRebaseMode: DateTimeRebaseMode,
    override val timestampRebaseMode: DateTimeRebaseMode,
    override val hasInt96Timestamps: Boolean,
    postProcessor: GpuParquetReaderPostProcessor)
  extends ParquetExtraInfo(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps)
