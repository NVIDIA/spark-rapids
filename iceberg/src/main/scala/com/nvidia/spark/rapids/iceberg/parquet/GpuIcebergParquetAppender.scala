package com.nvidia.spark.rapids.iceberg.parquet

import scala.util.Using

import com.nvidia.spark.rapids.{GpuParquetWriter, SpillableColumnarBatch}
import java.{lang, util}
import java.util.stream.{Stream => JStream}
import org.apache.iceberg.{FieldMetrics, Metrics, MetricsConfig}
import org.apache.iceberg.io.{FileAppender, FileIO}
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.ParquetMetadata

class GpuIcebergParquetAppender(
    val inner: GpuParquetWriter,
    val metricsConfig: MetricsConfig,
    val fileIO: FileIO) extends FileAppender[SpillableColumnarBatch] {
  private var closed = false
  private var footer: ParquetMetadata = _

  override def add(d: SpillableColumnarBatch): Unit = {
    // TODO: Split to meet row group size
    inner.writeSpillableAndClose(d)
  }

  override def metrics(): Metrics = {
    require(footer != null, "Writer must be closed before getting metrics")

    ParquetUtil.footerMetrics(footer, JStream.empty[FieldMetrics[_]](), metricsConfig)
  }

  override def length(): Long = inner.getFileLength

  override def close(): Unit = {
    if (!closed) {
      inner.close()
      footer = Using(IcebergPartitionedFile(fileIO.newInputFile(inner.path)).newReader) { reader =>
        // TODO: Get footer from table writer
          reader.getFooter
      }.get
      closed = true
    }
  }

  override def splitOffsets(): util.List[lang.Long] = {
    require(footer != null, "Writer must be closed before getting split offsets")

    ParquetUtil.getSplitOffsets(footer)
  }
}
