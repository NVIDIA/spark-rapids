/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.parquet

import scala.annotation.nowarn

import com.nvidia.spark.rapids.{GpuMetric, NoopMetric}
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader

/**
 * Iceberg 1.6.x shim: footer caching is not supported because the shaded `ParquetFileReader` in
 * 1.6.x has no public API to inject pre-parsed footer metadata. This opens the reader via the
 * plain `open(InputFile, ParquetReadOptions)` path and always reads the footer from the file.
 * The footer-miss counter is bumped on every call so dashboards see non-zero activity instead
 * of silently interpreting "all zeros" as "everything was cached".
 */
object GpuParquetIOShim {
  def openReader(
      inputFile: IcebergInputFile,
      _filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
    ParquetFileReader.open(GpuParquetIO.file(inputFile.getDelegate), options)
  }
}
