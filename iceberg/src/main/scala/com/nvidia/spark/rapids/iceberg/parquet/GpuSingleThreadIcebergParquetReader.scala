package com.nvidia.spark.rapids.iceberg.parquet

import com.nvidia.spark.rapids.{CpuCompressionConfig, DateTimeRebaseCorrected, ParquetPartitionReader, PartitionReaderWithBytesRead}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter2
import java.util.{Map => JMap}
import org.apache.hadoop.fs.Path
import scala.annotation.tailrec

import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSingleThreadIcebergParquetReader(
    val files: Seq[IcebergPartitionedFile],
    val constantsProvider: IcebergPartitionedFile => JMap[Integer, _],
    val gpuDeleteProvider: IcebergPartitionedFile => Option[GpuDeleteFilter2],
    override val conf: GpuIcebergParquetReaderConf) extends GpuIcebergParquetReader  {

  private val taskIterator = files.iterator
  private var gpuDeleteFilter: Option[GpuDeleteFilter2] = _
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

        gpuDeleteFilter = gpuDeleteProvider(file)
        parquetIterator = new SingleFileReader(file, constantsProvider(file), gpuDeleteFilter, conf)
        dataIterator = gpuDeleteFilter
          .map(_.filterAndDelete(parquetIterator))
          .getOrElse(parquetIterator)
        // update the current file for Spark's filename() function
        InputFileUtils.setInputFileBlock(file.path.toString, file.start, file.length)
      }
    } else {
      if (!parquetIterator.hasNext) {
        withResource(parquetIterator) { _ =>
          parquetIterator = null
          withResource(gpuDeleteFilter) { _ =>
            gpuDeleteFilter = None
          }
        }
        ensureParquetReader()
      }
    }
  }

  override def close(): Unit = {
    if (parquetIterator != null) {
      withResource(parquetIterator) { _ =>
        parquetIterator = null
        withResource(gpuDeleteFilter) { _ =>
          gpuDeleteFilter = None
        }
      }
    }
  }
}

private class SingleFileReader(
    val file: IcebergPartitionedFile,
    val idToConstant: JMap[Integer, _],
    val deleteFilter: Option[GpuDeleteFilter2],
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
    withResource(reader.get()) { batch =>
      postProcessor.process(batch)
    }
  }

  private def open() = {
    withResource(file.newReader) { reader =>
      val requiredSchema = deleteFilter.map(_.requiredSchema).getOrElse(conf.expectedSchema)

      val filteredParquet = super.filterParquetBlocks(file, requiredSchema)

//      val partReaderSparkSchema = TypeWithSchemaVisitor.visit(requiredSchema,
//        filteredParquet.schema , new GpuParquetReader.SparkSchemaConverter)
//        .asInstanceOf[StructType]

//      val partReaderSparkSchema = TypeUtil.visit(requiredSchema, new TypeToSparkType)
//        .asInstanceOf[StructType]

      val parquetPartReader = new ParquetPartitionReader(conf.parquetConf.conf,
        file.sparkPartitionedFile,
        new Path(file.file.location()),
        filteredParquet.blocks,
        filteredParquet.schema,
        conf.parquetConf.caseSensitive,
        filteredParquet.readSchema,
        conf.parquetConf.parquetDebugDumpPrefix,
        conf.parquetConf.parquetDebugDumpAlways,
        conf.parquetConf.maxBatchSizeRows,
        conf.parquetConf.maxBatchSizeBytes,
        conf.parquetConf.targetBatchSizeBytes,
        conf.parquetConf.useChunkedReader,
        conf.parquetConf.maxChunkedReaderMemoryUsageSizeBytes,
        CpuCompressionConfig.disabled(),
        conf.parquetConf.metrics,
        DateTimeRebaseCorrected, // dateRebaseMode
        DateTimeRebaseCorrected, // timestampRebaseMode
        true, // hasInt96Timestamps
        false) // useFieldId

      val parquetReader = new PartitionReaderWithBytesRead(parquetPartReader)
      val postProcessor = new GpuParquetReaderPostProcessor(filteredParquet,
        idToConstant,
        requiredSchema)

      inited = true
      (parquetReader, postProcessor)
    }
  }
}

