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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.json.rapids.GpuReadJsonFileFormat
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

object GpuFileSourceScanExec {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    meta.wrapped.relation.fileFormat match {
      case _: CSVFileFormat => GpuReadCSVFileFormat.tagSupport(meta)
      case f if GpuOrcFileFormat.isSparkOrcFormat(f) => GpuReadOrcFileFormat.tagSupport(meta)
      case _: ParquetFileFormat => GpuReadParquetFileFormat.tagSupport(meta)
      case _: JsonFileFormat => GpuReadJsonFileFormat.tagSupport(meta)
      case f =>
        meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
    }
  }
}
