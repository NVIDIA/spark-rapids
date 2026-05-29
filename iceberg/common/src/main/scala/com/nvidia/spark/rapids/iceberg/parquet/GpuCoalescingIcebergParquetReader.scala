/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
import org.apache.hadoop.fs.Path

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

  override def close(): Unit = {
    if (inited) {
      reader.close()
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = reader.get()

  private def createParquetReader() = {
    val multiFileConf = conf.threadConf.asInstanceOf[MultiFile]
    val hasFilePathMetadata = multiFileConf.hasFilePathMetadata
    val hasRowPositionMetadata = multiFileConf.hasRowPositionMetadata
    val clippedBlocks = files.map(f => (f, super.filterParquetBlocks(f, conf.expectedSchema)))
      .flatMap {
        case (file, (info, shadedFileReadSchema)) =>
          val postProcessor = new GpuParquetReaderPostProcessor(
            info,
            constantsProvider(file),
            conf.expectedSchema,
            shadedFileReadSchema,
            conf.metrics)

          // MultiFileCoalescingPartitionReaderBase.populateCurrentBlockChunk only invokes
          // checkIfNeedToSplitDataBlock when adjacent blocks have different filePaths. Two
          // Iceberg splits of the same physical Parquet file share their real path, so
          // without a tag here the parent would merge their blocks under the first split's
          // extraInfo (= post-processor) and corrupt _pos. Tagging the path with the split
          // range (a URI fragment) makes distinct splits present as different "files" to the
          // parent; the override then runs and the postProcessor identity check forces a
          // chunk split.
          //
          // The tagged path is NOT confined to equality checks: it is stored as
          // ParquetSingleDataBlockMeta.filePath, used as a key in the parent's per-file
          // block map, logged, and passed to fileIO.newInputFile when blocks are read. The
          // physical file still opens correctly because Hadoop FileSystem implementations
          // strip the URI fragment when resolving the underlying file — that is an implicit
          // invariant this code relies on. Side effect: FileCache entries are keyed per
          // split, so two splits of the same file no longer share cache contents. The real
          // path stays on the post-processor and is the value materialized for the _file
          // metadata column, so user-visible output is unaffected.
          val coalescerPath = file.split match {
            case Some((start, length)) =>
              new Path(s"${info.filePath}#iceberg-split=$start-$length")
            case None => info.filePath
          }

          info.blocks.map { block =>
            ParquetSingleDataBlockMeta(
              coalescerPath,
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
      multiFileConf.poolConfBuilder.build(),
      false, // ignoreMissingFiles
      false, // ignoreCorruptFiles
      false) // useFieldId
      {
      override def checkIfNeedToSplitDataBlock(currentBlockInfo: SingleDataBlockInfo,
          nextBlockInfo: SingleDataBlockInfo): Boolean = {
        val curExtra = currentBlockInfo.extraInfo.asInstanceOf[IcebergParquetExtraInfo]
        val nextExtra = nextBlockInfo.extraInfo.asInstanceOf[IcebergParquetExtraInfo]
        // Each Iceberg split owns its own GpuParquetReaderPostProcessor with private block
        // metadata and _pos counters. Two splits of the same physical Parquet file share a
        // file path but get distinct post-processors, so coalescing their blocks into one
        // chunk would let the first split's post-processor finalize rows that belong to the
        // second split — producing wrong _pos values and indexing past its block array.
        if (curExtra.postProcessor ne nextExtra.postProcessor) {
          return true
        }
        if (currentBlockInfo.filePath == nextBlockInfo.filePath) {
          return false
        }
        if (hasFilePathMetadata || hasRowPositionMetadata) {
          return true
        }
        if (checkIfNeedToSplitBlocks(
          currentBlockInfo.extraInfo.dateRebaseMode,
          nextBlockInfo.extraInfo.dateRebaseMode,
          currentBlockInfo.extraInfo.timestampRebaseMode,
          nextBlockInfo.extraInfo.timestampRebaseMode,
          currentBlockInfo.schema,
          nextBlockInfo.schema,
          currentBlockInfo.filePath.toString,
          nextBlockInfo.filePath.toString)) {
          return true
        }
        !curExtra.postProcessor.compatibleForCombining(nextExtra.postProcessor)
      }

      override def finalizeOutputBatch(batch: ColumnarBatch,
          extraInfo: ExtraInfo): ColumnarBatch = {
        extraInfo
          .asInstanceOf[IcebergParquetExtraInfo]
          .postProcessor
          .process(GpuColumnVector.incRefCounts(batch))
      }
    }
  }
}

private case class IcebergParquetExtraInfo(override val dateRebaseMode: DateTimeRebaseMode,
    override val timestampRebaseMode: DateTimeRebaseMode,
    override val hasInt96Timestamps: Boolean,
    postProcessor: GpuParquetReaderPostProcessor)
  extends ParquetExtraInfo(dateRebaseMode, timestampRebaseMode, hasInt96Timestamps)