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

import com.nvidia.spark.rapids.{DateTimeRebaseMode, ExtraInfo, GpuColumnVector, SingleDataBlockInfo}
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.parquet._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuCoalescingIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {

  private var inited = false
  private lazy val reader = createParquetReader()
  private var curPostProcessor: GpuParquetReaderPostProcessor = _

  override def close(): Unit = {
    if (inited) {
      reader.close()
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = {
    val batch = reader.get()
    require(curPostProcessor != null,
      "The post processor should not be null when calling next()")
    curPostProcessor.process(batch)
  }

  private def createParquetReader() = {
    val clippedBlocks = files.map(f => (f, super.filterParquetBlocks(f, conf.expectedSchema)))
      .flatMap {
        case (file, (info, shadedFileReadSchema)) =>
          val postProcessor = new GpuParquetReaderPostProcessor(
            info,
            constantsProvider(file),
            conf.expectedSchema,
            shadedFileReadSchema)

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
      rapidsFileIO,
      conf.conf,
      files.map(_.sparkPartitionedFile).toArray,
      clippedBlocks,
      conf.caseSensitive,
      conf.parquetDebugDumpPrefix,
      conf.parquetDebugDumpAlways,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      conf.targetBatchSizeBytes,
      conf.maxGpuColumnSizeBytes,
      conf.useChunkedReader,
      conf.maxChunkedReaderMemoryUsageSizeBytes,
      CpuCompressionConfig.disabled(),
      conf.metrics,
      new StructType(), // partitionSchema
      conf.threadConf.asInstanceOf[MultiFile].poolConfBuilder.build(),
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
        GpuCoalescingIcebergParquetReader.this.curPostProcessor = extraInfo
          .asInstanceOf[IcebergParquetExtraInfo]
          .postProcessor

        GpuColumnVector.incRefCounts(batch)
      }
    }
  }
}

private case class IcebergParquetExtraInfo(override val dateRebaseMode: DateTimeRebaseMode,
    override val timestampRebaseMode: DateTimeRebaseMode,
    override val hasInt96Timestamps: Boolean,
    postProcessor: GpuParquetReaderPostProcessor)
  extends ParquetExtraInfo(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps)