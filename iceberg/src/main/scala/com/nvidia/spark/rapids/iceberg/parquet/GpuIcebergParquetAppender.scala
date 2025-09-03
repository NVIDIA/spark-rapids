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

import java.{lang, util}
import java.util.stream.{Stream => JStream}

import com.nvidia.spark.rapids.{GpuParquetWriter, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import org.apache.iceberg.{FieldMetrics, Metrics, MetricsConfig}
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.ParquetMetadata



/**
 * An Iceberg FileAppender that uses the GPU Parquet writer.
 * <br/>
 *
 * In iceberg, a [[FileAppender]] is used to write data to a file in some format. It's in the
 * lowest part of writer stack, and is used by the higher level writers such as rolling file
 * writer, partitioned file writer to write data.
 */
class GpuIcebergParquetAppender(
  val inner: GpuParquetWriter,
  val metricsConfig: MetricsConfig,
  val fileIO: IcebergFileIO) extends FileAppender[SpillableColumnarBatch] {
  private var closed = false
  private var footer: ParquetMetadata = _

  override def add(d: SpillableColumnarBatch): Unit = {
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
      footer = withResource(IcebergPartitionedFile(fileIO.newInputFile(inner.path)).newReader) {
        reader =>
          // TODO: Get footer from table writer
          reader.getFooter
      }
      closed = true
    }
  }

  override def splitOffsets(): util.List[lang.Long] = {
    require(footer != null, "Writer must be closed before getting split offsets")

    ParquetUtil.getSplitOffsets(footer)
  }
}