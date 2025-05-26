/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.parquet.GpuParquetScan

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.metric.SQLMetrics

trait GpuBatchScanExecMetrics extends GpuExec {
  import GpuMetric._

  def scan: Scan

  override def supportsColumnar = true

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    BUFFER_TIME_WITH_SEM -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_WITH_SEM),
    FILTER_TIME_WITH_SEM -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME_WITH_SEM),
    OP_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
  ) ++ fileCacheMetrics ++ scanCustomMetrics

  lazy val fileCacheMetrics: Map[String, GpuMetric] = {
    // File cache only supported on Parquet files for now.
    scan match {
      case _: GpuParquetScan | _: GpuOrcScan => createFileCacheMetrics()
      case _ => Map.empty
    }
  }

  private lazy val scanCustomMetrics: Map[String, GpuMetric] = {
    scan.supportedCustomMetrics().map { metric =>
      metric.name() -> WrappedGpuMetric(SQLMetrics.createV2CustomMetric(sparkContext, metric))
    }.toMap
  }
}
