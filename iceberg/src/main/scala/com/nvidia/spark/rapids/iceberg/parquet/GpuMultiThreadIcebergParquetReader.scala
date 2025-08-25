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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CombineConf
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter
import com.nvidia.spark.rapids.parquet.{CpuCompressionConfig, MultiFileCloudParquetPartitionReader}

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiThreadIcebergParquetReader(
    val rapidsFileIO: IcebergFileIO,
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val deleteFilterProvider: IcebergPartitionedFile => Option[GpuDeleteFilter],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {
  private val pathToFile = files.groupBy(_.urlEncodedPath).mapValues(_.toSeq)
  private val postProcessors: ConcurrentMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor]
  = new ConcurrentHashMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor](files.size)


  private var inited = false
  private lazy val reader = createParquetReader()
  private val fileIterator = files.iterator
  private val lastBatchHolder: Array[Option[ColumnarBatch]] = Array.fill(1)(None)
  private var curDataIterator: Iterator[ColumnarBatch] = _

  override def close(): Unit = {
    if (inited) {
      withResource(reader) { _ =>
        withResource(lastBatchHolder(0)) { _ =>
        }
      }
    }
  }

  override def hasNext: Boolean = {
    ensureDataIterator()
    if (curDataIterator == null) {
      false
    } else {
      curDataIterator.hasNext
    }
  }

  override def next(): ColumnarBatch = {
    curDataIterator.next()
  }

  private def ensureDataIterator(): Unit = {
    if (curDataIterator == null || !curDataIterator.hasNext) {
      curDataIterator = null
      if (fileIterator.hasNext) {
        val file = fileIterator.next()
        val gpuDeleteFilter = deleteFilterProvider(file)
        val fileDataIterator = new SingleFileColumnarBatchIterator(file,
          lastBatchHolder, reader, postProcessors)
        curDataIterator = gpuDeleteFilter
          .map(_.filterAndDelete(fileDataIterator))
          .getOrElse(fileDataIterator)
      }
    }
  }

  private def createParquetReader() = {
    val sparkPartitionedFiles = files.map(_.sparkPartitionedFile).toArray

    inited = true
    new MultiFileCloudParquetPartitionReader(
      rapidsFileIO,
      conf.conf,
      sparkPartitionedFiles,
      this.filterBlock,
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
      new StructType(), // iceberg handles partition value by itself
      conf.threadConf.asInstanceOf[MultiThread].numThreads,
      conf.threadConf.asInstanceOf[MultiThread].maxNumFilesProcessed,
      false, // ignoreMissingFiles
      false, // ignoreCorruptFiles
      false, // useFieldId
      // We always set this to true to disable combining small files into a larger one
      // as iceberg's parquet file may have different schema due to schema evolution.
      true, // queryUsesInputFile
      true, // keepReadsInOrder, this is required for iceberg
      CombineConf(-1, -1)) // Disable combine
  }

  private def filterBlock(f: PartitionedFile) = {
    val path = f.filePath.toString()
    val icebergFiles = pathToFile(path).filter(p => p.isSame(f))
    require(icebergFiles.length == 1, s"Expected 1 iceberg partition file, but found " +
      s"${icebergFiles.length} for $f")
    val icebergFile = icebergFiles.head
    val deleteFilter = deleteFilterProvider(icebergFile)

    val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

    val filteredParquet = super.filterParquetBlocks(icebergFile, requiredSchema)

    val postProcessor = new GpuParquetReaderPostProcessor(
      filteredParquet,
      constantsProvider(icebergFile),
      requiredSchema)

    val old = postProcessors.put(icebergFile, postProcessor)
    require(old == null, "Iceberg parquet partition file post processor already exists!")
    filteredParquet
  }
}

private class SingleFileColumnarBatchIterator(val file: IcebergPartitionedFile,
    lastBatchHolder: Array[Option[ColumnarBatch]],
    inner: PartitionReader[ColumnarBatch],
    postProcessors: ConcurrentMap[IcebergPartitionedFile, GpuParquetReaderPostProcessor])
    extends Iterator[ColumnarBatch]  {

  private def lastBatch: Option[ColumnarBatch] = lastBatchHolder(0)

  override def hasNext: Boolean = if (lastBatch.isEmpty) {
    if (inner.next()) {
      lastBatchHolder(0) = Some(inner.get())

      // Current file partition
      InputFileUtils.getCurInputFilePath() == file.urlEncodedPath &&
        InputFileUtils.getCurInputFileStartOffset == file.start &&
        InputFileUtils.getCurInputFileLength == file.length
    } else {
      false
    }
  } else {
    true
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more elements")
    }
    try {
      postProcessors.get(file).process(lastBatch.get)
    } finally {
      lastBatchHolder(0) = None
    }
  }
}