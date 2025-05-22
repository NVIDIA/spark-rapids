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

import com.nvidia.spark.rapids.{DateTimeRebaseCorrected, PartitionReaderWithBytesRead}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, ParquetPartitionReader}
import java.util.{Map => JMap}
import org.apache.hadoop.fs.Path
import scala.annotation.tailrec

import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSingleThreadIcebergParquetReader(
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader  {

  private val taskIterator = files.iterator
  private var parquetIterator: SingleFileReader = _
  private var dataIterator: Iterator[ColumnarBatch] = _

  override def hasNext: Boolean = {
    ensureParquetReader()
    if (parquetIterator == null) {
      return false
    }
    dataIterator.hasNext
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more elements")
    }
    dataIterator.next()
  }

  @tailrec
  private def ensureParquetReader(): Unit = {
    if (parquetIterator == null) {
      if (taskIterator.hasNext) {
        val file = taskIterator.next()

        parquetIterator = new SingleFileReader(file, constantsProvider(file), conf)
        dataIterator = parquetIterator
        // update the current file for Spark's filename() function
        InputFileUtils.setInputFileBlock(file.path.toString, file.start, file.length)
      }
    } else {
      if (!parquetIterator.hasNext) {
        withResource(parquetIterator) { _ =>
          parquetIterator = null
        }
        ensureParquetReader()
      }
    }
  }

  override def close(): Unit = {
    if (parquetIterator != null) {
      withResource(parquetIterator) { _ =>
        parquetIterator = null
      }
    }
  }
}

private class SingleFileReader(
    val file: IcebergPartitionedFile,
    val idToConstant: JMap[Integer, _],
    override val conf: GpuIcebergParquetReaderConf)
  extends GpuIcebergParquetReader {

  private var inited = false
  private lazy val (reader, postProcessor) = open()

  override def close(): Unit = {
    if (inited) {
      withResource(reader) { _ => }
    }
  }

  override def hasNext: Boolean = reader.next()

  override def next(): ColumnarBatch = {
    postProcessor.process(reader.get())
  }

  private def open() = {
    val filteredParquet = super.filterParquetBlocks(file, conf.expectedSchema)

    val parquetPartReader = new ParquetPartitionReader(conf.conf,
      file.sparkPartitionedFile,
      new Path(file.file.location()),
      filteredParquet.blocks,
      filteredParquet.schema,
      conf.caseSensitive,
      filteredParquet.readSchema,
      conf.parquetDebugDumpPrefix,
      conf.parquetDebugDumpAlways,
      conf.maxBatchSizeRows,
      conf.maxBatchSizeBytes,
      conf.targetBatchSizeBytes,
      conf.useChunkedReader,
      conf.maxChunkedReaderMemoryUsageSizeBytes,
      CpuCompressionConfig.disabled(),
      conf.metrics,
      DateTimeRebaseCorrected, // dateRebaseMode
      DateTimeRebaseCorrected, // timestampRebaseMode
      true, // hasInt96Timestamps
      false) // useFieldId

    val parquetReader = new PartitionReaderWithBytesRead(parquetPartReader)
    val postProcessor = new GpuParquetReaderPostProcessor(filteredParquet.schema,
      idToConstant,
      conf.expectedSchema)

    inited = true
    (parquetReader, postProcessor)
  }
}
