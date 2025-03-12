package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.nvidia.spark.rapids.{CombineConf, CpuCompressionConfig, MultiFileCloudParquetPartitionReader}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter2

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiThreadIcebergParquetReader(
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val deleteFilterProvider: IcebergPartitionedFile => Option[GpuDeleteFilter2],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader {
  private val pathToFile = files.map(f => f.urlEncodedPath -> f).toMap
  private val postProcessors: ConcurrentMap[String, GpuParquetReaderPostProcessor] =
    new ConcurrentHashMap[String, GpuParquetReaderPostProcessor](files.size)

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
        val filePath = file.urlEncodedPath
        val gpuDeleteFilter = deleteFilterProvider(file)
        val fileDataIterator = new SingleFileColumnarBatchIterator(filePath,
          lastBatchHolder, reader, postProcessors)
        curDataIterator = gpuDeleteFilter
          .map(_.filterAndDelete(fileDataIterator))
          .getOrElse(fileDataIterator)
      }
    }
  }

  private def createParquetReader() = {
    val sparkPartitionedFile = files.map(_.sparkPartitionedFile).toArray

    inited = true
    new MultiFileCloudParquetPartitionReader(conf.parquetConf.conf,
      sparkPartitionedFile,
      this.filterBlock,
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
      new StructType(), // iceberg handles partition value by itself
      conf.parquetConf.threadConf.asInstanceOf[MultiThread].numThreads,
      conf.parquetConf.threadConf.asInstanceOf[MultiThread].maxNumFilesProcessed,
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
    val icebergFile = pathToFile(path)
    val deleteFilter = deleteFilterProvider(icebergFile)

    val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

    val filteredParquet = super.filterParquetBlocks(icebergFile, requiredSchema)

    val postProcessor = new GpuParquetReaderPostProcessor(
      filteredParquet,
      constantsProvider(icebergFile),
      requiredSchema)

    postProcessors.put(path, postProcessor)
    filteredParquet
  }
}

private class SingleFileColumnarBatchIterator(val targetPath: String,
    lastBatchHolder: Array[Option[ColumnarBatch]],
    inner: PartitionReader[ColumnarBatch],
    postProcessors: ConcurrentMap[String, GpuParquetReaderPostProcessor])
    extends Iterator[ColumnarBatch]  {

  private def lastBatch: Option[ColumnarBatch] = lastBatchHolder(0)

  override def hasNext: Boolean = if (lastBatch.isEmpty) {
    if (inner.next()) {
      lastBatchHolder(0) = Some(inner.get())
      if (InputFileUtils.getCurInputFilePath() != targetPath) {
        false
      } else {
        true
      }
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
    withResource(lastBatch.get) { batch =>
      lastBatchHolder(0) = None
      postProcessors.get(targetPath).process(batch)
    }
  }
}
