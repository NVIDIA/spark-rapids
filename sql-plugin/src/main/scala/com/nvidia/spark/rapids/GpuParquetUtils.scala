/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.Locale

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

object GpuParquetUtils extends Logging {
  /**
   * Trim block metadata to contain only the column chunks that occur in the specified schema.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param readSchema the schema to preserve
   * @param blocks the block metadata from the original Parquet file
   * @param isCaseSensitive indicate if it is case sensitive
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def clipBlocksToSchema(
      readSchema: MessageType,
      blocks: java.util.List[BlockMetaData],
      isCaseSensitive: Boolean): Seq[BlockMetaData] = {
    val columnPaths = readSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
    val pathSet = if (isCaseSensitive) {
      columnPaths.map(cp => cp.toDotString).toSet
    } else {
      columnPaths.map(cp => cp.toDotString.toLowerCase(Locale.ROOT)).toSet
    }
    blocks.asScala.map { oldBlock =>
      //noinspection ScalaDeprecation
      val newColumns = if (isCaseSensitive) {
        oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath.toDotString))
      } else {
        oldBlock.getColumns.asScala.filter(c =>
          pathSet.contains(c.getPath.toDotString.toLowerCase(Locale.ROOT)))
      }
      newBlockMeta(oldBlock.getRowCount, newColumns)
    }
  }

  /**
   * Build a new BlockMetaData
   *
   * @param rowCount the number of rows in this block
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  def newBlockMeta(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }

  def getBlocksInBatch(
      blockIter: BufferedIterator[BlockMetaData],
      readSchema: StructType,
      maxBatchSizeRows: Int,
      maxBatchSizeBytes: Long): Seq[BlockMetaData] = {
    val currentChunk = new ArrayBuffer[BlockMetaData]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.getRowCount
            numParquetBytes += currentChunk.last.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
        s"Estimated GPU bytes: $numBytes")
    currentChunk
  }
}
