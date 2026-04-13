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

import java.util.stream.{Stream => JStream}

import org.apache.iceberg.{FieldMetrics, Metrics, MetricsConfig, Schema}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.ParquetMetadata

/**
 * Helper in Iceberg's parquet package so GPU code can reuse ParquetMetrics with the original
 * Iceberg schema instead of reconstructing schema from the written Parquet footer.
 */
object GpuParquetMetricsHelper {
  def footerMetrics(
      schema: Schema,
      footer: ParquetMetadata,
      fieldMetrics: JStream[FieldMetrics[_]],
      metricsConfig: MetricsConfig): Metrics = {
    ParquetMetrics.metrics(schema, footer.getFileMetaData.getSchema, metricsConfig, footer,
      fieldMetrics)
  }
}
