package com.nvidia.spark.rapids.iceberg.parquet

import com.nvidia.spark.rapids.{GpuParquetWriter, SpillableColumnarBatch}
import java.{lang, util}
import org.apache.iceberg.{Metrics, MetricsConfig}
import org.apache.iceberg.io.FileAppender

import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuIcebergParquetWriter(
    val inner: GpuParquetWriter,
    val metricsConfig: MetricsConfig) extends
  FileAppender[SpillableColumnarBatch] {
  private var closed = false

  override def add(d: SpillableColumnarBatch): Unit = {
    // TODO: Split to meet row group size
    inner.writeSpillableAndClose(d)
  }

  override def metrics(): Metrics = ???

  override def length(): Long = inner.getFileLength

  override def close(): Unit = {
    if (!closed) {
      closed = true
      inner.close()
    }
  }

  override def splitOffsets(): util.List[lang.Long] = {
    throw new UnsupportedOperationException("Not supported yet.")
  }
}
